package bittorrent

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/mymath"
	"honnef.co/go/bittorrent/protocol"

	"github.com/zeebo/bencode"
)

// How often we choke/unchoke peers. Spec calls for 30 seconds, libtorrent defaults to 15.
const unchokeInterval = 15 * time.Second

var DefaultSettings = Settings{
	NumConcurrentAnnounces: 10,
	ListenAddress:          "0.0.0.0",
	ListenPort:             "58261",
	MaxSessionPeers:        -1,
}

type Settings struct {
	NumConcurrentAnnounces int
	ListenAddress          string
	ListenPort             string // TODO support port ranges
	// Session-wide maximum number of connected peers. -1 means unlimited
	MaxSessionPeers int
}

type Session struct {
	Settings     Settings
	PeerIDPrefix []byte

	// XXX rename this field. these functions are only used for
	// decision making, and are separate from events, which are
	// delivered aysnchronously. calling it "callbacks" sort of
	// suggests that we'd use it for all kinds of events.
	//
	// XXX there'll be some overlap between these callbacks and
	// events. someone who wants to control incoming connections will
	// set PeerIncoming, but we'll also deliver an event when a new
	// peer connects. figure out if we can get rid of the overlap or
	// unify callbacks and events. if not, be clear when documenting
	// what to use callbacks for.
	Callbacks struct {
		// If set, PeerIncoming gets called when a new incoming connection is being made.
		// If it returns false, the connection will be rejected.
		//
		// Rejected connections will not trigger PeerDisconnected.
		PeerIncoming func(pconn *protocol.Connection) (allow bool)

		// If set, PeerHandshakeInfoHash gets called after receiving a peer's info hash in the handshake.
		// If it returns false, the connection will be rejected.
		//
		// The function will get called even if the info hash doesn't match any of our torrents or if the torrent is stopped.
		//
		// Rejected connections will trigger PeerDisconnected.
		PeerHandshakeInfoHash func(peer *Peer, hash protocol.InfoHash) (allow bool)

		// If set, PeerHandshakePeerID gets called after receiving a peer's peer ID in the handshake.
		// If it returns false, the connection will be rejected.
		//
		// Note that by the time we read the peer ID, we'll already have sent our own handshake.
		//
		// Rejected connections will trigger PeerDisconnected.
		PeerHandshakePeerID func(peer *Peer, peerID [20]byte) (allow bool)

		// XXX this should be an event instead, because we don't depend on a return value
		//
		// If set, PeerDisconnected gets called after a peer connection has been closed.
		PeerDisconnected func(peer *Peer, err error)
	}

	done chan struct{}

	closing  chan struct{}
	mu       sync.RWMutex
	listener net.Listener
	torrents map[protocol.InfoHash]*Torrent
	peers    container.Set[*Peer]

	eventsMu sync.Mutex
	events   []Event

	rngMu sync.Mutex
	rng   *rand.Rand

	torrentsWithPeers container.ConcurrentSet[*Torrent]

	// XXX check alignment on 32-bit
	// accessed atomically
	statistics struct {
		numConnectedPeers uint64
		uploadedRaw       uint64
		downloadedRaw     uint64
		numRejectedPeers  struct {
			sessionLimit                  uint64
			peerIncomingCallback          uint64
			peerHandshakeInfoHashCallback uint64
			shutdown                      uint64
			unknownTorrent                uint64
			stoppedTorrent                uint64
		}
		numTorrents struct {
			stopped  uint64
			leeching uint64
			seeding  uint64
			action   uint64
		}
	}
}

func NewSession() *Session {
	return &Session{
		Settings:          DefaultSettings,
		torrents:          map[protocol.InfoHash]*Torrent{},
		peers:             container.NewSet[*Peer](),
		closing:           make(chan struct{}),
		done:              make(chan struct{}),
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		torrentsWithPeers: container.NewConcurrentSet[*Torrent](),
	}
}

// OPT maybe Events should copy into a user-provided buffer. this
// would avoid allocations, but would make the user code more
// complicated: it would need to figure out a good size for the
// buffer, and iteratively call Events to fully drain the slice, but
// without getting stuck draining forever.
//
// alternatively, we could let the user pop a single event at a time,
// but that would have a lot of function call and locking overhead.
func (sess *Session) Events() []Event {
	sess.eventsMu.Lock()
	defer sess.eventsMu.Unlock()
	s := sess.events
	sess.events = make([]Event, 0, len(s))
	return s
}

func (sess *Session) validateMetainfo(info *Metainfo) error {
	var a int64
	if len(info.Info.Files) == 0 {
		// single-file mode
		a = info.Info.Length
	} else {
		// multi-file mode
		for _, f := range info.Info.Files {
			a += f.Length
		}
	}

	b := info.Info.PieceLength

	// XXX theoretically, this can overflow. practically it never will
	numPieces := int((a + b - 1) / b)

	u, err := url.Parse(info.Announce)
	if err != nil {
		return fmt.Errorf("invalid announce URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid scheme %q in announce URL %q", u.Scheme, info.Announce)
	}
	if info.Info.PieceLength < 1<<14 {
		return fmt.Errorf("piece size %d is too small, has to be at least 16 KiB", info.Info.PieceLength)
	}
	if len(info.Info.Pieces) != 20*numPieces {
		return fmt.Errorf("got %d bytes of hashes, expected %d", len(info.Info.Pieces), 20*numPieces)
	}
	if info.Info.Name == "" {
		return fmt.Errorf("invalid filename %q", info.Info.Name)
	}
	return nil
	// XXX prevent directory traversal in info.Info.Name
}

func (sess *Session) AddTorrent(info *Metainfo, hash protocol.InfoHash) (*Torrent, error) {
	// XXX check the torrent hasn't already been added

	if err := sess.validateMetainfo(info); err != nil {
		return nil, fmt.Errorf("torrent failed validation: %w", err)
	}

	torr := &Torrent{
		Metainfo: info,
		Hash:     hash,
		have:     NewBitset(),
		session:  sess,
		peers:    container.NewConcurrentSet[*Peer](),
	}

	// XXX ensure the on-disk files are of the right lengths

	// XXX don't require all files in a torrent to always be open. open them lazily.

	var files dataStorage
	if len(info.Info.Files) == 0 {
		// single-file mode
		files.add(info.Info.Name, info.Info.Length)
	} else {
		// multi-file mode
		for _, fe := range info.Info.Files {
			// XXX prevent directory traversal
			files.add(filepath.Join(info.Info.Name, filepath.Join(fe.Path...)), fe.Length)
		}
	}
	torr.data = &files

	n := uint32(torr.NumPieces())
	torr.availability = Pieces{
		pieceIndices:   make([]uint32, n),
		sortedPieces:   make([]uint32, n),
		availabilities: make([]uint16, n),
		buckets:        []uint32{n},
	}
	for i := uint32(0); i < n; i++ {
		torr.availability.pieceIndices[i] = i
		torr.availability.sortedPieces[i] = i
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.isClosing() {
		// Don't add new torrent to an already stopped client
		return nil, ErrClosing
	}
	atomic.AddUint64(&sess.statistics.numTorrents.stopped, 1)
	sess.torrents[torr.Hash] = torr

	return torr, nil
}

func (sess *Session) listen() error {
	defer log.Println("listener goroutine has quit")

	l, err := net.Listen("tcp", net.JoinHostPort(sess.Settings.ListenAddress, sess.Settings.ListenPort))
	if err != nil {
		return err
	}
	defer l.Close()

	sess.mu.Lock()
	if sess.isClosing() {
		l.Close()
		return ErrClosing
	}
	sess.listener = l
	sess.mu.Unlock()

	for {
		err := func() error {
			conn, err := l.Accept()
			if err != nil {
				return err
			}

			numPeers := atomic.AddUint64(&sess.statistics.numConnectedPeers, 1)
			if sess.Settings.MaxSessionPeers >= 0 {
				if numPeers > uint64(sess.Settings.MaxSessionPeers) {
					atomic.AddUint64(&sess.statistics.numRejectedPeers.sessionLimit, 1)
					atomic.AddUint64(&sess.statistics.numConnectedPeers, ^uint64(0))
					conn.Close()
					return nil
				}
			}

			pconn := protocol.NewConnection(conn)
			if sess.Callbacks.PeerIncoming != nil {
				if !sess.Callbacks.PeerIncoming(pconn) {
					atomic.AddUint64(&sess.statistics.numRejectedPeers.peerIncomingCallback, 1)
					atomic.AddUint64(&sess.statistics.numConnectedPeers, ^uint64(0))
					pconn.Close()
					return nil
				}
			}

			sess.mu.Lock()
			defer sess.mu.Unlock()
			if sess.isClosing() {
				// We're shutting down, discard the connection and quit
				atomic.AddUint64(&sess.statistics.numRejectedPeers.shutdown, 1)
				atomic.AddUint64(&sess.statistics.numConnectedPeers, ^uint64(0))
				pconn.Close()
				return ErrClosing
			}

			peer := &Peer{
				conn:    pconn,
				session: sess,

				// OPT tweak buffers
				msgs:             make(chan protocol.Message, 256),
				incomingRequests: make(chan request, 256),
				writes:           make(chan protocol.Message, 256),
				controlWrites:    make(chan protocol.Message, 256),
				done:             make(chan struct{}),

				have:           NewBitset(),
				amInterested:   false,
				amChoking:      true,
				peerInterested: false,
				peerChoking:    true,
			}
			sess.peers.Add(peer)

			go func() {
				defer close(peer.done)
				err := peer.run()

				sess.mu.Lock()
				defer sess.mu.Unlock()
				sess.peers.Delete(peer)
				atomic.AddUint64(&sess.statistics.numConnectedPeers, ^uint64(0))
				if sess.Callbacks.PeerDisconnected != nil {
					sess.Callbacks.PeerDisconnected(peer, err)
				}
			}()

			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func (sess *Session) unchokePeers() {
	// TODO implement dynamic slot allocation, see https://github.com/dominikh/gamayun/issues/13
	// TODO make this value configurable
	const uploadSlotsPerTorrent = 5

	torrs := sess.torrentsWithPeers.Copy()
	for torr := range torrs {
		func() {
			torr.stateMu.Lock()
			peers := torr.peers.Copy()
			state := torr.state
			torr.stateMu.Unlock()

			// Peers we have to choke at the end.
			// This starts as the set of all currently unchoked peers.
			// Peers we later decide to keep unchoked will be removed from it.
			choke := container.Set[*Peer]{}
			// All currently interested peers. A subset of them will be unchoked.
			var interested []struct {
				peer       *Peer
				downloaded uint64
			}
			for peer := range peers {
				peer.mu.RLock()
				// All unchoked peers are candidates for choking
				if !peer.amChoking {
					choke.Add(peer)
				}

				// We always have to reset the download counter, even if the peer isn't interested at this point in time.
				stat := atomic.SwapUint64(&peer.chokingStatistics.downloaded, 0)

				// All interested peers are candidates for unchoking
				if peer.peerInterested {
					interested = append(interested, struct {
						peer       *Peer
						downloaded uint64
					}{peer, stat})
				}
				peer.mu.RUnlock()
			}

			log.Printf("%s: %d currently unchoked peers, %d interested peers", torr, len(choke), len(interested))

			// The peers we eventually decided to unchoke
			unchoke := make([]*Peer, 0, uploadSlotsPerTorrent)
			switch state {
			case TorrentStateLeeching:
				// In leeching mode we implement a tit-for-tat-inspired algorithm.
				// The peers that upload to us the fastest will be unchoked.
				// A small number of peers will be unchoked randomly,
				// so that we can find potentially better peers,
				// and to allow new clients to join the swarm.

				// 20% of all slots - but at least 1 - will be used for opportunistic unchoking
				opportunistic := mymath.Max(1, uploadSlotsPerTorrent/5)
				log.Println(opportunistic, uploadSlotsPerTorrent, uploadSlotsPerTorrent-opportunistic, len(interested[:uploadSlotsPerTorrent-opportunistic]))

				if len(interested) <= uploadSlotsPerTorrent {
					// Unchoke all interested peers
					for _, peer := range interested {
						log.Printf("%s: unchoking %s because we have few peers", torr, peer.peer)
						unchoke = append(unchoke, peer.peer)
					}
				} else {
					// Unchoke some peers based on their upload rate to us
					sort.Slice(interested, func(i, j int) bool {
						// favour the peers that upload the fastest to us
						return interested[i].downloaded < interested[j].downloaded
					})
					for _, peer := range interested[:uploadSlotsPerTorrent-opportunistic] {
						log.Printf("%s: unchoking %s because of reciprocation", torr, peer.peer)
						unchoke = append(unchoke, peer.peer)
					}

					// XXX optimistic unchoking should happen every 30s, while normal unchoking happens every 10s.

					remainder := interested[uploadSlotsPerTorrent-opportunistic:]
					sess.rngMu.Lock()
					// OPT Shuffling all the peers isn't too expensive and this is fine.
					// However, if we have way more peers than optimistic unchoke slots,
					// then generating random, unique indices is much faster,
					// and it can even use a simple O(n²) uniqueness check, avoiding maps.
					//
					// Another option is using something like modernc.org/mathutil.FC32,
					// but that suffers from a high up-front cost.
					//
					// Really, this is fine.
					// Even if we had millions of peers, this would take less than a second.
					// For realistic numbers of peers (say, 200), this takes microseconds.
					sess.rng.Shuffle(len(remainder), func(i, j int) {
						remainder[i], remainder[j] = remainder[j], remainder[i]
					})
					sess.rngMu.Unlock()
					for _, peer := range remainder[:opportunistic] {
						log.Printf("%s: unchoking %s optimistically", torr, peer.peer)
						unchoke = append(unchoke, peer.peer)
					}
				}

			case TorrentStateSeeding:
				// In seeding mode, we cycle through peers in a round-robin fashion.
				// This gives all peers an equal share of our time.

				if len(interested) <= uploadSlotsPerTorrent {
					// Unchoke all interested peers
					for _, peer := range interested {
						log.Printf("%s: unchoking %s because we have few peers", torr, peer.peer)
						unchoke = append(unchoke, peer.peer)
					}
				} else {
					// Unchoke peers in a round-robin manner
					sort.Slice(interested, func(i, j int) bool {
						return interested[i].peer.lastUnchoke.Before(interested[j].peer.lastUnchoke)
					})
					for _, peer := range interested[:uploadSlotsPerTorrent] {
						log.Printf("%s: unchoking %s because of round-robin", torr, peer.peer)
						unchoke = append(unchoke, peer.peer)
					}
				}

			default:
				// This can happen because of the race between grabbing the list of torrents and torrents stopping
				return
			}

			log.Printf("%s: choking %d peers, unchoking %d peers", torr, len(choke), len(unchoke))
			for _, peer := range unchoke {
				// Don't choke peers we want to keep unchoked
				choke.Delete(peer)
			}
			// XXX don't block trying to send the control messages to a slow peer
			for peer := range choke {
				if err := peer.choke(); err != nil {
					// XXX kill peer
				}
			}
			for _, peer := range unchoke {
				if err := peer.unchoke(); err != nil {
					// XXX kill peer
				}
			}
		}()
	}
}

func (sess *Session) Run() error {
	defer close(sess.done)

	errs := make(chan error, 1)
	go func() {
		errs <- sess.listen()
	}()

	t := time.NewTicker(1 * time.Second)
	tUnchoke := time.NewTicker(unchokeInterval)
	for {
		select {
		case <-tUnchoke.C:
			sess.unchokePeers()
		case now := <-t.C:
			// XXX don't depend on a timer for processing announces
			// OPT don't iterate over all torrents, instead keep a list of torrents with outstanding announces
			sess.mu.Lock()
			for _, torr := range sess.torrents {
				for {
					ann, ok := torr.announces.Peek()
					if !ok {
						break
					}
					if ann.NextTry.After(now) {
						break
					}
					// XXX concurrency
					_, err := sess.announce(context.Background(), &ann)
					if err != nil {
						torr.announces.ReplaceFront(ann)
						break
					}
					torr.announces.Pop()
				}
			}
			sess.mu.Unlock()
		case err := <-errs:
			// XXX make sure Shutdown works through all the remaining announces
			return err
		}
	}

	// XXX periodically announce torrents, choke/unchoke peers, ...
}

func (sess *Session) Shutdown(ctx context.Context) error {
	// XXX close all the files in all the torrents. although, once we
	// have lazy file opening, this should happen in Torrent.Stop
	// instead.

	log.Println("Shutting down")
	defer log.Println("Shut down")

	sess.mu.Lock()
	close(sess.closing)
	if sess.listener != nil {
		sess.listener.Close()
	}
	sess.mu.Unlock()

	for _, torr := range sess.torrents {
		torr.Stop()
	}
	// Torrent.Stop closes all peers associated with it; this loop
	// closes the remaining peers, the ones which haven't finished the
	// handshake yet.
	for peer := range sess.peers {
		peer.Close()
	}
	log.Println("All peers done")

	// XXX ideally, our context would be able to cancel the announces that Session.Run is currently working on
	//
	// Wait for Session.Run to terminate
	<-sess.done

	// Process the remaining announces
	//
	// XXX respect backoff of failed announces
	for _, torr := range sess.torrents {
		for {
			ann, ok := torr.announces.Pop()
			if !ok {
				break
			}
			// XXX handle failure
			// XXX concurrency
			sess.announce(ctx, &ann)
		}
	}

	return nil
}

func (sess *Session) isClosing() bool {
	_, ok := channel.TryRecv(sess.closing)
	return ok
}

func (sess *Session) addEvent(ev Event) {
	sess.eventsMu.Lock()
	sess.events = append(sess.events, ev)
	sess.eventsMu.Unlock()
}

func (sess *Session) announce(ctx context.Context, ann *Announce) (_ *TrackerResponse, err error) {
	log.Printf("announcing %q for %s", ann.Event, ann.InfoHash)

	defer func() {
		if err != nil {
			ann.Fails = append(ann.Fails, struct {
				When time.Time
				Err  error
			}{time.Now(), err})
			// XXX exponential backoff
			//
			// XXX cancel announce that has failed too often; remember
			// to cancel all following announces that expect this
			// announce to have gone through
			ann.NextTry = time.Now().Add(10 * time.Second)
			sess.addEvent(EventAnnounceFailed{*ann})
		}
	}()

	type AnnounceResponse struct {
		Torrent  *Torrent
		Response TrackerResponse
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ann.Tracker, nil)
	if err != nil {
		return nil, err
	}

	// XXX set trackerid

	q := req.URL.Query()
	// XXX remove once we support BEP 23
	q["info_hash"] = []string{string(ann.InfoHash[:])}
	q["peer_id"] = []string{string(ann.PeerID[:])}
	q["port"] = []string{sess.Settings.ListenPort}
	q["uploaded"] = []string{strconv.FormatUint(ann.Up, 10)}
	q["downloaded"] = []string{strconv.FormatUint(ann.Down, 10)}
	// XXX support multi-file mode
	// q["left"] = []string{strconv.FormatUint(areq.Torrent.Metainfo.Info.Length, 10)}
	q["left"] = []string{"0"} // XXX use correct value

	if ann.Event == "stopped" {
		q["numwant"] = []string{"0"}
	} else {
		q["numwant"] = []string{"200"}
	}
	q["compact"] = []string{"1"}
	q["no_peer_id"] = []string{"1"}
	if ann.Event != "" {
		q["event"] = []string{ann.Event}
	}

	// Replace "+" with "%20" in encoded info hash. net/url thinks
	// that "+" is an acceptable encoding of spaces in the query
	// portion. It isn't.
	req.URL.RawQuery = strings.Replace(q.Encode(), "+", "%20", -1)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if cat := resp.StatusCode / 100; cat == 4 || cat == 5 {
		// XXX return a typed error
		// XXX include the response body in the error
		return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	// XXX limit size of response body

	var tresp TrackerResponse
	if err := bencode.NewDecoder(resp.Body).Decode(&tresp); err != nil {
		return nil, err
	}

	return &tresp, nil
}

func (sess *Session) GeneratePeerID() [20]byte {
	const minAscii = 33
	const maxAscii = 127

	var peerID [20]byte
	copy(peerID[:], sess.PeerIDPrefix)
	if len(sess.PeerIDPrefix) < 20 {
		suffix := peerID[len(sess.PeerIDPrefix):]
		sess.rngMu.Lock()
		for i := range suffix {
			suffix[i] = byte(sess.rng.Intn(maxAscii-minAscii) + minAscii)
		}
		sess.rngMu.Unlock()
	}

	return peerID
}
