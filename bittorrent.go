package bittorrent

// XXX support removing torrents

// XXX handle clients that don't support the Fast extension; don't send them messages they won't understand

// TODO learn about socket options, send/receive buffers, corking, …

// TODO corking and/or nagle might make sense. blocks are obviously
// too large to benefit from it, but HAVE messages are small enough to
// be batched together. should we relegate this caching to the kernel,
// or should we cache in user space instead?

// TODO can we write to sockets without having to copy data from user space to kernel space?

// XXX have per-torrent peer ID

// XXX there is a possible deadlock in the protocol: the peer keeps
// sending us requests, we keep sending rejects. If our control write
// queue fills up, we'll block and stop reading messages, causing the
// peer's write queue to also fill up. Now neither client can make
// progress.
//
// This will probably only ever occur with misbehaving clients; sane
// clients won't flood us with requests.

// XXX handle read/write timeouts, both on disk i/o and network i/o

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/protocol"

	"github.com/shurcooL/go-goon"
	"github.com/zeebo/bencode"
)

type Statistics struct {
	UploadedData  uint64 // XXX not implemented yet
	UploadedTotal uint64

	DownloadedData  uint64 // XXX not implemented yet
	DownloadedTotal uint64
}

type Session struct {
	Settings Settings

	PeerID [20]byte
	Peers  map[*Peer]struct{}

	incomingConnections chan *protocol.Connection
	closedConnections   chan *Peer

	// XXX check alignment on ARM
	statistics Statistics

	mu       sync.RWMutex
	Torrents map[protocol.InfoHash]*Torrent
}

func (sess *Session) Statistics() Statistics {
	return Statistics{
		UploadedData:    atomic.LoadUint64(&sess.statistics.UploadedData),
		UploadedTotal:   atomic.LoadUint64(&sess.statistics.UploadedTotal),
		DownloadedData:  atomic.LoadUint64(&sess.statistics.DownloadedData),
		DownloadedTotal: atomic.LoadUint64(&sess.statistics.DownloadedTotal),
	}
}

type Request struct {
	Index  uint32
	Begin  uint32
	Length uint32
}

var DefaultSettings = Settings{
	NumConcurrentAnnounces: 10,
	ListenAddress:          "0.0.0.0",
	ListenPort:             "58261",
}

type Settings struct {
	NumConcurrentAnnounces int
	ListenAddress          string
	ListenPort             string // TODO support port ranges
}

func (sess *Session) lock()    { sess.mu.Lock() }
func (sess *Session) rlock()   { sess.mu.RLock() }
func (sess *Session) unlock()  { sess.mu.Unlock() }
func (sess *Session) runlock() { sess.mu.RLock() }

func NewSession() *Session {
	return &Session{
		// XXX generate peer ID
		PeerID:   [20]byte{123, 123, 126, 36, 123, 42, 122},
		Settings: DefaultSettings,
		Torrents: map[protocol.InfoHash]*Torrent{},
		Peers:    map[*Peer]struct{}{},
		// XXX optimize all these buffer sizes
		incomingConnections: make(chan *protocol.Connection),
		closedConnections:   make(chan *Peer),
	}
}

func Verify(data []byte, checksum []byte) bool {
	h := sha1.Sum(data)
	return bytes.Equal(h[:], checksum)
}

func (sess *Session) validateMetainfo(info *Metainfo) error {
	a := info.Info.Length
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

func (sess *Session) AddTorrent(info *Metainfo, hash protocol.InfoHash) error {
	// XXX check the torrent hasn't already been added

	if err := sess.validateMetainfo(info); err != nil {
		return fmt.Errorf("torrent failed validation: %w", err)
	}

	torr := &Torrent{
		Metainfo: info,
		Hash:     hash,
		Have:     NewBitset(),
		newPeers: make(chan *Peer),
	}

	n := uint32(torr.NumPieces())
	torr.Availability = Pieces{
		pieceIndices: make([]uint32, n),
		SortedPieces: make([]uint32, n),
		Priorities:   make([]uint16, n),
		buckets:      []uint32{n},
	}
	for i := uint32(0); i < n; i++ {
		torr.Availability.pieceIndices[i] = i
		torr.Availability.SortedPieces[i] = i
	}

	sess.lock()
	sess.Torrents[torr.Hash] = torr
	sess.unlock()

	go sess.RunTorrent(context.TODO(), torr)
	return nil
}

func (sess *Session) announce(ctx context.Context, torr *Torrent, event string) (*TrackerResponse, error) {
	type AnnounceResponse struct {
		Torrent  *Torrent
		Response TrackerResponse
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, torr.Metainfo.Announce, nil)
	if err != nil {
		return nil, err
	}

	// XXX set trackerid

	q := req.URL.Query()
	// XXX remove once we support BEP 23
	q["info_hash"] = []string{string(torr.Hash[:])}
	q["peer_id"] = []string{string(sess.PeerID[:])}
	// XXX proper values
	q["port"] = []string{sess.Settings.ListenPort}
	q["uploaded"] = []string{"0"}
	q["downloaded"] = []string{"0"}
	// XXX support multi file mode
	// q["left"] = []string{strconv.FormatUint(areq.Torrent.Metainfo.Info.Length, 10)}
	q["left"] = []string{"0"}
	q["numwant"] = []string{"200"}
	q["compact"] = []string{"1"}
	q["no_peer_id"] = []string{"1"}
	if event != "" {
		q["event"] = []string{event}
	}

	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// XXX check status code of response

	var tresp TrackerResponse

	if err := bencode.NewDecoder(resp.Body).Decode(&tresp); err != nil {
		return nil, err
	}

	return &tresp, nil
}

func (sess *Session) listen(ctx context.Context) error {
	l, err := net.Listen("tcp", net.JoinHostPort(sess.Settings.ListenAddress, sess.Settings.ListenPort))
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		pconn := protocol.NewConnection(conn)
		select {
		case sess.incomingConnections <- pconn:
		case <-ctx.Done():
			pconn.Close()
			return ctx.Err()
		}
	}
}

type HandshakeMessage struct {
	Peer *Peer
	Hash protocol.InfoHash
}

type PeerIDMessage struct {
	Peer   *Peer
	PeerID [20]byte
}

func verifyTorrent(torr *Torrent) (Bitset, error) {
	// OPT use more than one goroutine to verify in parallel; we get about 1 GB/s on one core
	pieceSize := torr.Metainfo.Info.PieceLength
	numPieces := torr.NumPieces()
	// XXX support multi-file mode
	name := torr.Metainfo.Info.Name

	f, err := os.Open(name)
	if err != nil {
		return Bitset{}, err
	}
	defer f.Close()

	bits := NewBitset()
	buf := make([]byte, pieceSize)
	piece := uint32(0)
	for {
		n, err := io.ReadFull(f, buf)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				if piece+1 != uint32(numPieces) {
					break
				} else {
					buf = buf[:n]
				}
			} else if err == io.EOF {
				break
			} else {
				return Bitset{}, err
			}
		}

		off := piece * 20
		if Verify(buf, torr.Metainfo.Info.Pieces[off:off+20]) {
			bits.Set(piece)
		}

		piece++
	}
	return bits, err
}

type VerifiedTorrent struct {
	Torrent *Torrent
	Have    Bitset
}

func (peer *Peer) blockReader(ctx context.Context) error {
	for {
		select {
		case req := <-peer.incomingRequests:
			// OPT use channel.Collect and try to coalesce reads
			err := func() error {
				f, err := os.Open(peer.Torrent.Metainfo.Info.Name)
				if err != nil {
					return err
				}
				defer f.Close()

				// OPT: cache pieces(?)
				_, err = f.Seek(int64(req.Index)*int64(peer.Torrent.Metainfo.Info.PieceLength)+int64(req.Begin), io.SeekStart)
				if err != nil {
					return err
				}

				// XXX make sure req.Length isn't too long
				// OPT reuse buffers
				buf := make([]byte, req.Length)
				_, err = io.ReadFull(f, buf)
				if err != nil {
					return err
				}

				// XXX track upload stat

				select {
				case peer.writes <- Message{
					Message: protocol.Message{
						Type:   protocol.MessageTypePiece,
						Index:  req.Index,
						Begin:  req.Begin,
						Length: req.Length,
						Data:   buf,
					},
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (peer *Peer) readPeer(ctx context.Context) error {
	// XXX add handshake and peer id to DownloadedTotal stat

	hash, err := peer.Conn.ReadHandshake()
	if err != nil {
		return err
	}
	select {
	case peer.handshake <- hash:
	case <-ctx.Done():
		return ctx.Err()
	}

	id, err := peer.Conn.ReadPeerID()
	if err != nil {
		return err
	}
	select {
	case peer.peerID <- id:
	case <-ctx.Done():
		return ctx.Err()
	}

	for {
		msg, err := peer.Conn.ReadMessage(ctx)
		if err != nil {
			return err
		}
		atomic.AddUint64(&peer.Session.statistics.DownloadedTotal, uint64(msg.Size()))
		select {
		case peer.msgs <- Message{
			Message: msg,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (peer *Peer) writePeer(ctx context.Context) error {
	writeMsg := func(msg Message) error {
		atomic.AddUint64(&peer.Session.statistics.UploadedTotal, uint64(msg.Size()))
		return peer.Conn.WriteMessage(msg.Message)
	}

	for {
		if msg, ok := channel.TryRecv(peer.controlWrites); ok {
			// OPT use channel.Collect
			if err := writeMsg(msg); err != nil {
				return err
			}
		}
		select {
		case msg := <-peer.controlWrites:
			// OPT use channel.Collect
			if err := writeMsg(msg); err != nil {
				return err
			}
		case msg := <-peer.writes:
			// OPT use channel.Collect
			if err := writeMsg(msg); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type UnknownTorrentError struct {
	hash protocol.InfoHash
}

func (err UnknownTorrentError) Error() string {
	return fmt.Sprintf("unknown torrent %s", err.hash)
}

type WriteError struct {
	error
}

func (err WriteError) Unwrap() error {
	return err.error
}

func (sess *Session) RunPeer(ctx context.Context, peer *Peer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 1)
	// OPT multiple reader goroutines?
	go func() { channel.TrySend(errs, peer.blockReader(ctx)) }()
	go func() { channel.TrySend(errs, peer.readPeer(ctx)) }()
	go func() { channel.TrySend(errs, peer.writePeer(ctx)) }()

	t := time.NewTicker(time.Second)
	for {
		select {
		case err := <-errs:
			return err
		case <-t.C:
			if !peer.peerInterested && !peer.amChoking {
				peer.amChoking = true
				peer.controlWrites <- Message{
					Message: protocol.Message{
						Type: protocol.MessageTypeChoke,
					},
				}
			} else if peer.peerInterested && peer.amChoking {
				// XXX limit number of unchoked peers
				peer.amChoking = false
				peer.controlWrites <- Message{
					Message: protocol.Message{
						Type: protocol.MessageTypeUnchoke,
					},
				}
			}

			torr := peer.Torrent
			torr.lock()
			if torr.Have.count != peer.lastHaveSent.count {
				have := torr.Have.Copy()
				torr.unlock()

				// XXX find diff between lastHaveSent and have, send Have messages

				peer.lastHaveSent = have
			} else {
				torr.unlock()
			}

		case hash := <-peer.handshake:
			log.Println("got handshake from", peer)
			// XXX don't allow connecting to stopped torrents
			sess.lock()
			torr, ok := sess.Torrents[hash]
			sess.unlock()
			if !ok {
				return UnknownTorrentError{hash}
			}
			peer.Torrent = torr

			if err := peer.Conn.SendHandshake(hash, sess.PeerID); err != nil {
				return WriteError{err}
			}
		case id := <-peer.peerID:
			log.Println("got peer ID from", peer)
			peer.PeerID = id

			// Note that we send to torr.newPeers after we got the
			// peer ID, not after the first half of the handshake, to
			// avoid a race on peer.PeerID.
			peer.Torrent.newPeers <- peer

			// We've received their handshake and peer ID and have
			// sent ours, now tell the peer which pieces we have
			const haveMessageSize = 4 + 1 + 4 // length prefix, message type, index
			torr := peer.Torrent
			torr.lock()
			have := torr.Have.Copy()
			torr.unlock()
			peer.lastHaveSent = have
			if have.count == 0 {
				peer.writes <- Message{
					Message: protocol.Message{
						Type: protocol.MessageTypeHaveNone,
					},
				}
			} else if have.count == torr.NumPieces() {
				peer.writes <- Message{
					Message: protocol.Message{
						Type: protocol.MessageTypeHaveAll,
					},
				}
			} else if have.count*haveMessageSize < torr.NumPieces()/8 || true {
				// it's more compact to send a few Have messages than a bitfield that is mostly zeroes
				for i := 0; i < torr.NumPieces(); i++ {
					if have.bits.Bit(i) != 0 {
						peer.writes <- Message{
							Message: protocol.Message{
								Type:  protocol.MessageTypeHave,
								Index: uint32(i),
							},
						}
					}
				}
			} else {
				// XXX implement sending bitfield. don't forget to remove '|| true' from the previous condition
			}

		case msg := <-peer.msgs:
			if !peer.setup {
				switch msg.Type {
				case protocol.MessageTypeBitfield:
					t := time.Now()
					// XXX verify length and shape of bitfield
					peer.Have.SetBitfield(msg.Data)
					bit := 0
					for _, b := range msg.Data {
						for n := 7; n >= 0; n-- {
							bit++
							if b&1<<n != 0 {
								peer.Torrent.Availability.Inc(uint32(bit))
							}
						}
					}
					fmt.Println(time.Since(t))
					// XXX decrement when peer disconnects
				case protocol.MessageTypeHaveNone:
					// nothing to do
				case protocol.MessageTypeHaveAll:
					// OPT more efficient representation for HaveAll
					for i := 0; i < peer.Torrent.NumPieces(); i++ {
						peer.Have.Set(uint32(i))
					}
					// XXX decrement when peer disconnects
					peer.Torrent.Availability.HaveAll++
				default:
					// XXX instead of panicing we should kill the peer for a protocol violation
					panic(fmt.Sprintf("unexpected message %s", msg))
				}
				peer.setup = true
			} else {
				switch msg.Type {
				case protocol.MessageTypeRequest:
					// XXX respect cancel messages
					// XXX verify that index, begin and length are in bounds
					// XXX reject if we don't have the piece
					if peer.amChoking {
						peer.controlWrites <- Message{
							Message: protocol.Message{
								Type:   protocol.MessageTypeRejectRequest,
								Index:  msg.Index,
								Begin:  msg.Begin,
								Length: msg.Length,
							},
						}
					} else {
						req := Request{
							Index:  msg.Index,
							Begin:  msg.Begin,
							Length: msg.Length,
						}
						if !channel.TrySend(peer.incomingRequests, req) {
							peer.controlWrites <- Message{
								Message: protocol.Message{
									Type:   protocol.MessageTypeRejectRequest,
									Index:  msg.Index,
									Begin:  msg.Begin,
									Length: msg.Length,
								},
							}
						}
					}
				case protocol.MessageTypeInterested:
					peer.peerInterested = true
				case protocol.MessageTypeNotInterested:
					peer.peerInterested = false
				case protocol.MessageTypeHave:
					// XXX check bounds
					// XXX ignore Have if we've gotten HaveAll before
					peer.Have.Set(msg.Index)
					// XXX decrement when peer disconnects
					peer.Torrent.Availability.Inc(msg.Index)
				case protocol.MessageTypeChoke:
					// XXX handle
					peer.peerChoking = true
				case protocol.MessageTypeUnchoke:
					// XXX handle
					peer.peerChoking = false
				case protocol.MessageTypeAllowedFast:
					// XXX make use of this information
				case protocol.MessageTypeKeepAlive:
					// XXX should we respond?
				case protocol.MessageTypeBitfield, protocol.MessageTypeHaveNone, protocol.MessageTypeHaveAll:
					// XXX instead of panicing we should kill the peer for a protocol violation
					panic(fmt.Sprintf("unexpected message %s", msg))
				default:
					// XXX instead of panicing we should kill the peer for a protocol violation
					panic(fmt.Sprintf("unhandled message %s", msg))
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (sess *Session) RunTorrent(ctx context.Context, torr *Torrent) error {
	torr.State = TorrentStateVerifying
	torr.NextState = TorrentStateStopped

	// XXX disabled for debugging
	if false {
		t := time.Now()
		bits, err := verifyTorrent(torr)
		if err != nil {
			// XXX handle
			panic(err)
		}
		d := time.Since(t)

		// XXX support multi file mode
		rate := float64(torr.Metainfo.Info.Length) / float64(d)
		log.Printf("verified %s at %.2f MB/s: %.2f%%", torr.Hash, rate*1000, float64(bits.count)/float64(torr.NumPieces())*100)

		torr.Have = bits
	} else {
		bits := NewBitset()
		for i := 0; i < torr.NumPieces(); i++ {
			bits.Set(uint32(i))
		}
		torr.Have = bits
	}

	log.Printf("transitioning %s from state %s to %s", torr.Hash, torr.State, torr.NextState)
	torr.State = torr.NextState
	torr.NextState = TorrentStateStopped

	// XXX send "started" if we're not a seed. or even if we're a seed?
	// TODO(dh): we probably shouldn't block trying to send the announce
	tresp, err := sess.announce(ctx, torr, "")
	if err != nil {
		// XXX
		panic(err)
	}
	goon.Dump(tresp)

	// OPT(dh): disable ticker if we have no peers. there's no point in waking up thousands of torrents every second just to do nothing.
	ticker := time.NewTicker(time.Second)
	announcer := time.NewTicker(30 * time.Minute) // XXX use actual announce interval
	for {
		select {
		case <-announcer.C:
			// TODO(dh): we probably shouldn't block trying to send the announce
			tresp, err := sess.announce(ctx, torr, "")
			if err != nil {
				// TODO(dh): try to announce again soon, don't wait for the next planned announc
				log.Printf("failed announcing %s to %s: %s", torr.Hash, torr.Metainfo.Announce, err)
				continue
			}
			// XXX do something with the response
			goon.Dump(tresp)
		case <-ticker.C:
			// XXX
		case peer := <-torr.newPeers:
			log.Printf("new peer %s for torrent %s", peer, torr)
			// XXX
			_ = peer
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (sess *Session) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 1)
	go func() { errs <- sess.listen(ctx) }()

	for {
		select {
		case err := <-errs:
			return err
		case pconn := <-sess.incomingConnections:
			log.Println("incoming connection:", pconn)
			peer := &Peer{
				Conn:    pconn,
				Session: sess,

				// OPT tweak buffers
				msgs:             make(chan Message, 256),
				handshake:        make(chan protocol.InfoHash),
				peerID:           make(chan [20]byte),
				incomingRequests: make(chan Request, 256),
				writes:           make(chan Message, 256),
				controlWrites:    make(chan Message, 256),

				Have:           NewBitset(),
				amInterested:   false,
				amChoking:      true,
				peerInterested: false,
				peerChoking:    true,
			}
			sess.Peers[peer] = struct{}{}

			go func() {
				err := sess.RunPeer(ctx, peer)
				log.Printf("peer failed: %s", err)
				sess.closedConnections <- peer
			}()
		case peer := <-sess.closedConnections:
			// XXX fully close the peer connection
			// XXX update per-torrent lists of peers
			delete(sess.Peers, peer)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type Message struct {
	protocol.Message
}

//go:generate go run golang.org/x/tools/cmd/stringer@master -type TorrentState
type TorrentState uint8

const (
	TorrentStateStopped TorrentState = iota
	TorrentStateVerifying
	TorrentStateStarted
)

type Torrent struct {
	Metainfo     *Metainfo
	Hash         protocol.InfoHash
	Peers        []*Peer
	Availability Pieces

	State     TorrentState
	NextState TorrentState

	newPeers chan *Peer

	mu sync.RWMutex
	// Pieces we have
	Have Bitset
}

func (torr *Torrent) String() string {
	return torr.Hash.String()
}

func (torr *Torrent) lock()    { torr.mu.Lock() }
func (torr *Torrent) rlock()   { torr.mu.RLock() }
func (torr *Torrent) unlock()  { torr.mu.Unlock() }
func (torr *Torrent) runlock() { torr.mu.RUnlock() }

func (torr *Torrent) NumPieces() int {
	a := torr.Metainfo.Info.Length
	b := torr.Metainfo.Info.PieceLength
	// XXX theoretically, this can overflow. practically it never will
	return int((a + b - 1) / b)
}

type Peer struct {
	Conn    *protocol.Connection
	Session *Session

	// incoming
	msgs             chan Message
	handshake        chan protocol.InfoHash
	peerID           chan [20]byte
	incomingRequests chan Request

	// outgoing
	controlWrites chan Message
	writes        chan Message

	// OPT: bitflags

	// immutable

	// mutable, but doesn't need locking
	// Pieces the peer has
	PeerID [20]byte
	Have   Bitset
	// The peer has sent one of the allowed initial messages
	setup          bool
	amChoking      bool
	amInterested   bool
	peerChoking    bool
	peerInterested bool
	lastHaveSent   Bitset
	Torrent        *Torrent

	// mutable, needs lock held

	// accessed atomically
	droppedRequests uint32
}

func (peer *Peer) String() string {
	return peer.Conn.String()
}

type Error struct {
	Error error
	Peer  *Peer
}

func NewBitset() Bitset {
	return Bitset{bits: big.NewInt(0)}
}

type Bitset struct {
	count int
	// OPT: don't use big.Int. use our own representation that matches
	// the wire protocol, so that we don't have to convert between the
	// two all the time
	bits *big.Int
}

func (set Bitset) Copy() Bitset {
	nset := Bitset{
		count: set.count,
		bits:  big.NewInt(0),
	}
	nset.bits.Set(set.bits)
	return nset
}

func (set *Bitset) Set(piece uint32) {
	if set.bits.Bit(int(piece)) == 1 {
		// Some clients send Have messages for pieces they already announced in the bitfield, and we don't want to count them twice
		return
	}
	set.count++
	set.bits.SetBit(set.bits, int(piece), 1)
}

// SetBitfield populates set from a bittorrent bitfield.
// The first byte of the bitfield corresponds to indices 0 - 7 from high bit to low bit, respectively. The next one 8-15, etc.
func (set *Bitset) SetBitfield(buf []byte) {
	// OPT optimize this, probably by writing our own bitset type instead of using big.Int.
	// Note that we cannot use big.Int.SetBytes, because the byte layout doesn't match that of the bitfield.
	bit := 0
	for _, b := range buf {
		for n := 7; n >= 0; n-- {
			bit++
			if b&1<<n != 0 {
				set.count++
				set.bits.SetBit(set.bits, bit, 1)
			} else {
				set.bits.SetBit(set.bits, bit, 0)
			}
		}
	}
}

func (set *Bitset) Intersect(other *Bitset) *Bitset {
	out := &Bitset{
		bits: big.NewInt(0),
	}

	if set.count == 0 || other.count == 0 {
		return out
	}

	out.bits.And(set.bits, other.bits)

	// OPT: we'll call Intersect a lot, and Bytes allocates, so this is no good.
	// We could use FillBytes, but the loop is still O(n).
	for _, b := range out.bits.Bytes() {
		out.count += bits.OnesCount8(b)
	}
	return out
}

func (set *Bitset) String() string {
	return fmt.Sprintf("%d pieces", set.count)
}

// OPT don't spread requests for the same piece among many peers, it
// wastes their disk caches. "The logic in libtorrent switches into
// the request-whole-pieces mode when a peer sends blocks fast enough
// to send an entire piece in 30 seconds."
//
// At an 8 MB piece piece, 30 seconds means 267 kB/s. We might be able
// to always default to requesting entire pieces, based on modern
// internet speeds. However, 8 MB pieces have 500 blocks, so we can't
// truly request them all at once without risking overflowing the
// peer's request queue.

// OPT: this data structure is fine for keeping a priority-sorted list
// of pieces, but finding a peer-piece combination requires looping
// over each piece and checking with the peer until we find one. IOW,
// it's O(n) worst case.
//
// TODO: prioritize pieces we've already downloading
type Pieces struct {
	// mapping from piece to entry in sorted_pieces
	pieceIndices []uint32

	// sorted list of pieces, indexed into by piece_indices
	SortedPieces []uint32

	// mapping from piece to priority
	Priorities []uint16 // supports at most 65536 peers

	// mapping from priority to one past the last item in sorted_pieces that is part of that priority
	buckets []uint32

	// Mapping from piece to status
	Downloading big.Int // OPT don't use big.Int, it wastes a byte (+padding) on the sign

	// Mapping from piece to block bitmap
	BlockBitmapPtrs []uint16 // supports at most 65536 actively downloading pieces
	// Mapping from block bitmap pointer to block bitmap
	BlockBitmaps []*big.Int

	// Number of peers that have all pieces. These aren't tracked in Priorities.
	HaveAll int
}

func (t *Pieces) Inc(piece uint32) {
	avail := t.Priorities[piece]
	t.buckets[avail]--
	t.Priorities[piece]++

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.SortedPieces[other_index]

	t.SortedPieces[other_index], t.SortedPieces[index] = t.SortedPieces[index], t.SortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]

	if uint32(avail+1) >= uint32(len(t.buckets)) {
		t.buckets = append(t.buckets, other_index+1)
	}
}

func (t *Pieces) Dec(piece uint32) {
	t.Priorities[piece]--
	avail := t.Priorities[piece]

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.SortedPieces[other_index]
	t.buckets[avail]++

	log.Println(other_index)

	t.SortedPieces[other_index], t.SortedPieces[index] = t.SortedPieces[index], t.SortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]
}
