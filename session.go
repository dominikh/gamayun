package bittorrent

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/mymath"
	"honnef.co/go/bittorrent/oursync"
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
	ClientName   string

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
	}

	done chan struct{}

	closing  chan struct{}
	mu       sync.RWMutex
	listener net.Listener
	torrents map[protocol.InfoHash]*Torrent
	peers    container.Set[*Peer]

	eventsMu sync.Mutex
	events   []Event

	// XXX check alignment on 32-bit
	statistics struct {
		numConnectedPeers oursync.Uint64
		uploadedRaw       oursync.Uint64
		downloadedRaw     oursync.Uint64
		numRejectedPeers  struct {
			sessionLimit                  oursync.Uint64
			peerIncomingCallback          oursync.Uint64
			peerHandshakeInfoHashCallback oursync.Uint64
			shutdown                      oursync.Uint64
			unknownTorrent                oursync.Uint64
			stoppedTorrent                oursync.Uint64
		}
		numTorrents struct {
			stopped  oursync.Uint64
			leeching oursync.Uint64
			seeding  oursync.Uint64
			action   oursync.Uint64
		}
	}
}

func NewSession() *Session {
	return &Session{
		Settings: DefaultSettings,
		torrents: map[protocol.InfoHash]*Torrent{},
		peers:    container.NewSet[*Peer](),
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
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
	var n int64
	if len(info.Info.Files) == 0 {
		// single-file mode
		n = info.Info.Length
	} else {
		// multi-file mode
		for _, f := range info.Info.Files {
			n += f.Length
		}
	}

	numPieces := mymath.DivUp(n, info.Info.PieceLength)

	u, err := url.Parse(info.Announce)
	if err != nil {
		return fmt.Errorf("invalid announce URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid scheme %q in announce URL %q", u.Scheme, info.Announce)
	}
	if info.Info.PieceLength%protocol.MaxBlockSize != 0 {
		return fmt.Errorf("piece size %d is not a multiple of 16 KiB", info.Info.PieceLength)
	}
	if int64(len(info.Info.Pieces)) != 20*numPieces {
		return fmt.Errorf("got %d bytes of hashes, expected %d", len(info.Info.Pieces), 20*numPieces)
	}
	if info.Info.Name == "" {
		return fmt.Errorf("invalid filename %q", info.Info.Name)
	}
	if info.NumBytes() == 0 {
		return fmt.Errorf("torrent has no pieces")
	}
	return nil
	// XXX prevent directory traversal in info.Info.Name
}

func (sess *Session) AddTorrent(info *Metainfo, hash protocol.InfoHash) (*Torrent, error) {
	// XXX check the torrent hasn't already been added

	if err := sess.validateMetainfo(info); err != nil {
		return nil, fmt.Errorf("torrent failed validation: %w", err)
	}

	torr := NewTorrent(hash, info, sess)

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

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.isClosing() {
		// Don't add new torrent to an already stopped client
		return nil, ErrClosing
	}
	sess.statistics.numTorrents.stopped.Add(1)
	sess.torrents[torr.InfoHash] = torr

	go torr.run()

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

			numPeers := sess.statistics.numConnectedPeers.Add(1)
			if sess.Settings.MaxSessionPeers >= 0 {
				if numPeers > uint64(sess.Settings.MaxSessionPeers) {
					sess.statistics.numRejectedPeers.sessionLimit.Add(1)
					sess.statistics.numConnectedPeers.Add(^uint64(0))
					conn.Close()
					return nil
				}
			}

			pconn := protocol.NewConnection(conn)
			if sess.Callbacks.PeerIncoming != nil {
				if !sess.Callbacks.PeerIncoming(pconn) {
					sess.statistics.numRejectedPeers.peerIncomingCallback.Add(1)
					sess.statistics.numConnectedPeers.Add(^uint64(0))
					pconn.Close()
					return nil
				}
			}

			sess.mu.Lock()
			defer sess.mu.Unlock()
			if sess.isClosing() {
				// We're shutting down, discard the connection and quit
				sess.statistics.numRejectedPeers.shutdown.Add(1)
				sess.statistics.numConnectedPeers.Add(^uint64(0))
				pconn.Close()
				return ErrClosing
			}

			peer := NewPeer(pconn, sess)
			sess.peers.Add(peer)

			go func() {
				defer close(peer.done)
				err := peer.run()

				sess.mu.Lock()
				defer sess.mu.Unlock()
				sess.peers.Delete(peer)
				sess.statistics.numConnectedPeers.Add(^uint64(0))
				sess.emitEvent(EventPeerDisconnected{
					When: time.Now(),
					Peer: peer,
					Err:  err,
				})
			}()

			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func (sess *Session) Run() error {
	defer close(sess.done)

	errs := make(chan error, 1)
	go func() {
		errs <- sess.listen()
	}()

	t := time.NewTicker(1 * time.Second)
	for {
		select {
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
	//
	// We need to hold sess.mu because this can race with peers
	// disconnecting and removing themselves from the map.
	sess.mu.Lock()
	for peer := range sess.peers {
		peer.Close()
	}
	sess.mu.Unlock()
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

func (sess *Session) emitEvent(ev Event) {
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
			sess.emitEvent(EventAnnounceFailed{*ann})
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

	// XXX but what if one of the values contained an actual plus sign?
	//
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
