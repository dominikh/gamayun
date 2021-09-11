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
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/protocol"

	"github.com/zeebo/bencode"
)

type Statistics struct {
	UploadedData  uint64 // XXX not implemented yet
	UploadedTotal uint64

	DownloadedData  uint64 // XXX not implemented yet
	DownloadedTotal uint64
}

type Session struct {
	Settings  Settings
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

		// If set, PeerDisconnected gets called after a peer connection has been closed.
		PeerDisconnected func(peer *Peer, err error)
	}

	PeerID [20]byte

	// XXX check alignment on ARM
	statistics Statistics

	peersWg sync.WaitGroup

	closing  chan struct{}
	mu       sync.RWMutex
	listener net.Listener
	torrents map[protocol.InfoHash]*Torrent
	peers    container.Set[*Peer]
}

func (sess *Session) Statistics() Statistics {
	return Statistics{
		UploadedData:    atomic.LoadUint64(&sess.statistics.UploadedData),
		UploadedTotal:   atomic.LoadUint64(&sess.statistics.UploadedTotal),
		DownloadedData:  atomic.LoadUint64(&sess.statistics.DownloadedData),
		DownloadedTotal: atomic.LoadUint64(&sess.statistics.DownloadedTotal),
	}
}

type request struct {
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

func NewSession() *Session {
	return &Session{
		// XXX generate peer ID
		PeerID:   [20]byte{123, 123, 126, 36, 123, 42, 122},
		Settings: DefaultSettings,
		torrents: map[protocol.InfoHash]*Torrent{},
		peers:    container.NewSet[*Peer](),
		closing:  make(chan struct{}),
	}
}

func verifyBlock(data []byte, checksum []byte) bool {
	h := sha1.Sum(data)
	return bytes.Equal(h[:], checksum)
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
		peers:    container.NewSet[*Peer](),
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
		pieceIndices: make([]uint32, n),
		sortedPieces: make([]uint32, n),
		priorities:   make([]uint16, n),
		buckets:      []uint32{n},
	}
	for i := uint32(0); i < n; i++ {
		torr.availability.pieceIndices[i] = i
		torr.availability.sortedPieces[i] = i
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if _, ok := channel.TryRecv(sess.closing); ok {
		// Don't add new torrent to an already stopped client
		return nil, ErrClosing
	}
	sess.torrents[torr.Hash] = torr

	return torr, nil
}

func (sess *Session) announce(ctx context.Context, torr *Torrent, event string) (*TrackerResponse, error) {
	log.Printf("announcing %q for %s", event, torr.Hash)

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
	// XXX support multi-file mode
	// q["left"] = []string{strconv.FormatUint(areq.Torrent.Metainfo.Info.Length, 10)}
	q["left"] = []string{"0"}
	q["numwant"] = []string{"200"}
	q["compact"] = []string{"1"}
	q["no_peer_id"] = []string{"1"}
	if event != "" {
		q["event"] = []string{event}
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

	// XXX check status code of response

	var tresp TrackerResponse

	if err := bencode.NewDecoder(resp.Body).Decode(&tresp); err != nil {
		return nil, err
	}

	return &tresp, nil
}

var ErrClosing = errors.New("client is shutting down")

func (sess *Session) listen() error {
	defer log.Println("listener goroutine has quit")

	l, err := net.Listen("tcp", net.JoinHostPort(sess.Settings.ListenAddress, sess.Settings.ListenPort))
	if err != nil {
		return err
	}
	defer l.Close()

	sess.mu.Lock()
	if _, ok := channel.TryRecv(sess.closing); ok {
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
			pconn := protocol.NewConnection(conn)
			if sess.Callbacks.PeerIncoming != nil {
				if !sess.Callbacks.PeerIncoming(pconn) {
					pconn.Close()
					return nil
				}
			}

			sess.mu.Lock()
			defer sess.mu.Unlock()
			if _, ok := channel.TryRecv(sess.closing); ok {
				// We're shutting down, discard the connection and quit
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

			sess.peersWg.Add(1)
			go func() {
				defer sess.peersWg.Done()
				err := sess.runPeer(peer)

				sess.mu.Lock()
				defer sess.mu.Unlock()
				sess.peers.Delete(peer)
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

func (peer *Peer) write(msg protocol.Message) error {
	select {
	case peer.writes <- msg:
		return nil
	case <-peer.done:
		return nil
	case <-peer.session.closing:
		return ErrClosing
	}
}

func (peer *Peer) controlWrite(msg protocol.Message) error {
	select {
	case peer.controlWrites <- msg:
		return nil
	case <-peer.done:
		return nil
	case <-peer.session.closing:
		return ErrClosing
	}
}

func (peer *Peer) blockReader() error {
	for {
		select {
		case req := <-peer.incomingRequests:
			// OPT use channel.Collect and try to coalesce reads
			err := func() error {
				// OPT: cache pieces(?)
				// XXX make sure req.Length isn't too long
				// OPT reuse buffers
				buf := make([]byte, req.Length)
				_, err := peer.torrent.data.ReadAt(buf, int64(req.Index)*int64(peer.torrent.Metainfo.Info.PieceLength)+int64(req.Begin))
				if err != nil {
					return err
				}

				// XXX track upload stat

				return peer.write(protocol.Message{
					Type:   protocol.MessageTypePiece,
					Index:  req.Index,
					Begin:  req.Begin,
					Length: req.Length,
					Data:   buf,
				})
			}()
			if err != nil {
				return err
			}
		case <-peer.done:
			return nil
		}
	}
}

func (peer *Peer) readPeer() error {
	for {
		msg, err := peer.conn.ReadMessage()
		if err != nil {
			return err
		}
		atomic.AddUint64(&peer.session.statistics.DownloadedTotal, uint64(msg.Size()))
		select {
		case peer.msgs <- msg:
		case <-peer.done:
			return nil
		}
	}
}

func (peer *Peer) writePeer() error {
	writeMsg := func(msg protocol.Message) error {
		atomic.AddUint64(&peer.session.statistics.UploadedTotal, uint64(msg.Size()))
		return peer.conn.WriteMessage(msg)
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
		case <-peer.done:
			return nil
		}
	}
}

type UnknownTorrentError struct {
	hash protocol.InfoHash
}

func (err UnknownTorrentError) Error() string {
	return fmt.Sprintf("unknown torrent %s", err.hash)
}

type StoppedTorrentError struct {
	hash protocol.InfoHash
}

func (err StoppedTorrentError) Error() string {
	return fmt.Sprintf("stopped torrent %s", err.hash)
}

type WriteError struct {
	error
}

func (err WriteError) Unwrap() error {
	return err.error
}

type CallbackRejectedInfoHashError struct {
	Hash protocol.InfoHash
}

func (err CallbackRejectedInfoHashError) Error() string {
	return fmt.Sprintf("peer wasn't allowed to connect to torrent %s", err.Hash)
}

type CallbackRejectedPeerIDError struct {
	PeerID [20]byte
}

func (err CallbackRejectedPeerIDError) Error() string {
	return fmt.Sprintf("peer wasn't allowed to connect with peer ID %q", err.PeerID)
}

func (sess *Session) runPeer(peer *Peer) error {
	defer func() {
		close(peer.done)
		peer.conn.Close()
	}()

	// XXX add handshake and peer id to DownloadedTotal stat
	hash, err := peer.conn.ReadHandshake()
	if err != nil {
		return err
	}

	if sess.Callbacks.PeerHandshakeInfoHash != nil {
		if !sess.Callbacks.PeerHandshakeInfoHash(peer, hash) {
			return CallbackRejectedInfoHashError{hash}
		}
	}

	sess.mu.RLock()
	torr, ok := sess.torrents[hash]
	sess.mu.RUnlock()
	if !ok {
		return UnknownTorrentError{hash}
	}

	// XXX once we support removing torrents, this will race

	torr.mu.Lock()
	if torr.state == TorrentStateStopped {
		// Don't let peers connect to stopped torrents
		peer.conn.Close()
		torr.mu.Unlock()
		return StoppedTorrentError{hash}
	}
	torr.peers.Add(peer)
	torr.mu.Unlock()

	defer func() {
		torr.mu.Lock()
		defer torr.mu.Unlock()
		torr.peers.Delete(peer)
	}()

	peer.torrent = torr

	if err := peer.conn.SendHandshake(hash, sess.PeerID); err != nil {
		return WriteError{err}
	}

	id, err := peer.conn.ReadPeerID()
	if err != nil {
		return err
	}
	peer.peerID = id
	if sess.Callbacks.PeerHandshakePeerID != nil {
		if !sess.Callbacks.PeerHandshakePeerID(peer, id) {
			return CallbackRejectedPeerIDError{id}
		}
	}

	errs := make(chan error, 1)
	// OPT multiple reader goroutines?
	//
	// These goroutines will exit either when they encounter read/write errors on the connection or when Peer.done gets closed.
	// This combination should ensure that the goroutines always terminate when RunPeer returns.
	go func() { channel.TrySend(errs, peer.blockReader()) }()
	go func() { channel.TrySend(errs, peer.readPeer()) }()
	go func() { channel.TrySend(errs, peer.writePeer()) }()

	// We've received their handshake and peer ID and have
	// sent ours, now tell the peer which pieces we have
	const haveMessageSize = 4 + 1 + 4 // length prefix, message type, index
	torr.mu.RLock()
	have := torr.have.Copy()
	torr.mu.RUnlock()
	peer.lastHaveSent = have
	if have.count == 0 {
		err := peer.write(protocol.Message{
			Type: protocol.MessageTypeHaveNone,
		})
		if err != nil {
			return err
		}
	} else if have.count == torr.NumPieces() {
		err := peer.write(protocol.Message{
			Type: protocol.MessageTypeHaveAll,
		})
		if err != nil {
			return err
		}
	} else if have.count*haveMessageSize < torr.NumPieces()/8 || true {
		// it's more compact to send a few Have messages than a bitfield that is mostly zeroes
		for i := 0; i < torr.NumPieces(); i++ {
			if have.bits.Bit(i) != 0 {
				err := peer.write(protocol.Message{
					Type:  protocol.MessageTypeHave,
					Index: uint32(i),
				})
				if err != nil {
					return err
				}
			}
		}
	} else {
		// XXX implement sending bitfield. don't forget to remove '|| true' from the previous condition
	}

	t := time.NewTicker(time.Second)
	for {
		select {
		case err := <-errs:
			// This will also fire when we're shutting down, because blockReader, readPeer and writePeer will fail,
			// because the peer connections will get closed by Session.Run
			return err
		case <-t.C:
			if !peer.peerInterested && !peer.amChoking {
				peer.amChoking = true
				err := peer.controlWrite(protocol.Message{
					Type: protocol.MessageTypeChoke,
				})
				if err != nil {
					return err
				}
			} else if peer.peerInterested && peer.amChoking {
				// XXX limit number of unchoked peers
				peer.amChoking = false
				err := peer.controlWrite(protocol.Message{
					Type: protocol.MessageTypeUnchoke,
				})
				if err != nil {
					return err
				}
			}

			torr := peer.torrent
			torr.mu.RLock()
			if torr.have.count != peer.lastHaveSent.count {
				have := torr.have.Copy()
				torr.mu.RUnlock()

				// XXX find diff between lastHaveSent and have, send Have messages

				peer.lastHaveSent = have
			} else {
				torr.mu.RUnlock()
			}
		case msg := <-peer.msgs:
			if !peer.setup {
				switch msg.Type {
				case protocol.MessageTypeBitfield:
					t := time.Now()
					// XXX verify length and shape of bitfield
					peer.have.SetBitfield(msg.Data)
					bit := 0
					for _, b := range msg.Data {
						for n := 7; n >= 0; n-- {
							bit++
							if b&1<<n != 0 {
								peer.torrent.availability.inc(uint32(bit))
							}
						}
					}
					fmt.Println(time.Since(t))
					// XXX decrement when peer disconnects
				case protocol.MessageTypeHaveNone:
					// nothing to do
				case protocol.MessageTypeHaveAll:
					// OPT more efficient representation for HaveAll
					for i := 0; i < peer.torrent.NumPieces(); i++ {
						peer.have.Set(uint32(i))
					}
					// XXX decrement when peer disconnects
					peer.torrent.availability.haveAll++
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
						err := peer.controlWrite(protocol.Message{
							Type:   protocol.MessageTypeRejectRequest,
							Index:  msg.Index,
							Begin:  msg.Begin,
							Length: msg.Length,
						})
						if err != nil {
							return err
						}
					} else {
						req := request{
							Index:  msg.Index,
							Begin:  msg.Begin,
							Length: msg.Length,
						}
						if !channel.TrySend(peer.incomingRequests, req) {
							err := peer.controlWrite(protocol.Message{
								Type:   protocol.MessageTypeRejectRequest,
								Index:  msg.Index,
								Begin:  msg.Begin,
								Length: msg.Length,
							})
							if err != nil {
								return err
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
					peer.have.Set(msg.Index)
					// XXX decrement when peer disconnects
					peer.torrent.availability.inc(msg.Index)
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
		}
	}
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

	for _, torr := range sess.torrents {
		torr.Stop(ctx)
	}
	for peer := range sess.peers {
		peer.conn.Close()
	}
	sess.mu.Unlock()

	// XXX allow ctx to cancel waiting
	// sess.torrentsWg.Wait()
	// log.Println("All torrents done")

	// XXX wait for torr.Stop to actually have done its job

	sess.peersWg.Wait()
	log.Println("All peers done")
	return nil
}

func (sess *Session) Run() error {
	return sess.listen()

	// XXX periodically announce torrents, choke/unchoke peers, ...
}

//go:generate go run golang.org/x/tools/cmd/stringer@master -type TorrentState
type TorrentState uint8

const (
	TorrentStateStopped TorrentState = iota
	TorrentStateLeeching
	TorrentStateSeeding
)

type Torrent struct {
	Metainfo     *Metainfo
	Hash         protocol.InfoHash
	availability Pieces

	data *dataStorage

	action Action

	session *Session

	mu sync.RWMutex
	// Pieces we have
	have  Bitset
	state TorrentState
	peers container.Set[*Peer]
}

func (torr *Torrent) Start() {
	if torr.state != TorrentStateStopped {
		return
	}

	if torr.action != nil {
		return
	}

	torr.mu.Lock()
	if torr.IsComplete() {
		torr.state = TorrentStateSeeding
	} else {
		torr.state = TorrentStateLeeching
		panic("XXX: we cannot download torrents yet")
	}
	torr.mu.Unlock()

	// XXX initialize new stats for this "session" (delimieted by a start and stop announce)
	// XXX allow interrupting the announce
	// XXX is "started" the right event to send when we're a seeder?
	// XXX get announce interval from response
	// XXX handle announce failure
	// XXX process the list of peers
	torr.session.announce(context.Background(), torr, "started")
}

func (torr *Torrent) Stop(ctx context.Context) {
	if torr.action != nil {
		torr.action.Stop()
	} else {
		// XXX how should this handle an in-progress announce?

		torr.mu.Lock()
		defer torr.mu.Unlock()
		if torr.state != TorrentStateStopped {
			torr.state = TorrentStateStopped

			for peer := range torr.peers {
				// Right now this won't deadlock. runPeer runs in its
				// own goroutine, so even though it needs to hold the
				// same mutex as Torrent.Stop, it'll happily wait for
				// Stop to return first. However, if we were to wait
				// for runPeer to return before returning from Stop,
				// we'd have a deadlock.
				peer.conn.Close()

			}

			// XXX send final announce
		}
	}
}

func (torr *Torrent) String() string {
	return torr.Hash.String()
}

func (torr *Torrent) IsComplete() bool {
	// XXX locking
	return torr.have.count == torr.NumPieces()
}

func (torr *Torrent) SetHave(have Bitset) {
	torr.have = have
}

func (torr *Torrent) NumPieces() int {
	var a int64
	if len(torr.Metainfo.Info.Files) == 0 {
		// single-file mode
		a = torr.Metainfo.Info.Length
	} else {
		// multi-file mode
		for _, f := range torr.Metainfo.Info.Files {
			a += f.Length
		}
	}

	b := torr.Metainfo.Info.PieceLength
	// XXX theoretically, this can overflow. practically it never will
	return int((a + b - 1) / b)
}

type Peer struct {
	conn    *protocol.Connection
	session *Session

	// incoming
	msgs             chan protocol.Message
	incomingRequests chan request

	// outgoing
	controlWrites chan protocol.Message
	writes        chan protocol.Message

	done chan struct{}

	// OPT: bitflags

	// immutable

	// mutable, but doesn't need locking
	// Pieces the peer has
	peerID [20]byte
	have   Bitset
	// The peer has sent one of the allowed initial messages
	setup          bool
	amChoking      bool
	amInterested   bool
	peerChoking    bool
	peerInterested bool
	lastHaveSent   Bitset
	torrent        *Torrent

	// mutable, needs lock held

	// accessed atomically
	droppedRequests uint32
}

func (peer *Peer) String() string {
	return peer.conn.String()
}

type Error struct {
	Error error
	Peer  *Peer
}

type Bitset struct {
	count int
	// OPT: don't use big.Int. use our own representation that matches
	// the wire protocol, so that we don't have to convert between the
	// two all the time
	bits *big.Int
}

func NewBitset() Bitset {
	return Bitset{bits: big.NewInt(0)}
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
	mu sync.Mutex

	// mapping from piece to entry in sorted_pieces
	pieceIndices []uint32

	// sorted list of pieces, indexed into by piece_indices
	sortedPieces []uint32

	// mapping from piece to priority
	priorities []uint16 // supports at most 65536 peers

	// mapping from priority to one past the last item in sorted_pieces that is part of that priority
	buckets []uint32

	// Mapping from piece to status
	downloading big.Int // OPT don't use big.Int, it wastes a byte (+padding) on the sign

	// Mapping from piece to block bitmap
	blockBitmapPtrs []uint16 // supports at most 65536 actively downloading pieces
	// Mapping from block bitmap pointer to block bitmap
	blockBitmaps []*big.Int

	// Number of peers that have all pieces. These aren't tracked in Priorities.
	haveAll int
}

func (t *Pieces) inc(piece uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	avail := t.priorities[piece]
	t.buckets[avail]--
	t.priorities[piece]++

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.sortedPieces[other_index]

	t.sortedPieces[other_index], t.sortedPieces[index] = t.sortedPieces[index], t.sortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]

	if uint32(avail+1) >= uint32(len(t.buckets)) {
		t.buckets = append(t.buckets, other_index+1)
	}
}

func (t *Pieces) dec(piece uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.priorities[piece]--
	avail := t.priorities[piece]

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.sortedPieces[other_index]
	t.buckets[avail]++

	log.Println(other_index)

	t.sortedPieces[other_index], t.sortedPieces[index] = t.sortedPieces[index], t.sortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]
}

type Action interface {
	Run() (result interface{}, stopped bool, err error)
	Pause()
	Stop()
}

type BaseAction struct {
	Torrent   *Torrent
	PauseCh   chan struct{}
	StopCh    chan struct{}
	UnpauseCh chan struct{}
}

func (l *BaseAction) Pause()   { channel.TrySend(l.PauseCh, struct{}{}) }
func (l *BaseAction) Unpause() { channel.TrySend(l.UnpauseCh, struct{}{}) }
func (l *BaseAction) Stop()    { channel.TrySend(l.StopCh, struct{}{}) }

type Verify struct {
	BaseAction
}

func (l *Verify) Run() (result interface{}, stopped bool, err error) {
	log.Println("started verifying", l.Torrent)
	defer func() {
		if stopped {
			log.Println("stopped verifying", l.Torrent)
		} else {
			log.Println("finished verifying", l.Torrent)
		}
	}()

	// OPT use more than one goroutine to verify in parallel; we get about 1 GB/s on one core
	pieceSize := l.Torrent.Metainfo.Info.PieceLength
	numPieces := l.Torrent.NumPieces()

	res := NewBitset()

	// OPT use a normal MultiReader to avoid the unnecessary cost of
	// mapping from piece to file. We just want to read everything
	// sequentially.

	buf := make([]byte, pieceSize)
	piece := uint32(0)

	for {
		select {
		case <-l.PauseCh:
			<-l.UnpauseCh
		case <-l.StopCh:
			return res, true, nil
		default:
		}

		n, err := l.Torrent.data.ReadAt(buf, int64(piece)*int64(pieceSize))
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
				// XXX report the error
				return res, false, err
			}
		}

		off := piece * 20
		if verifyBlock(buf, l.Torrent.Metainfo.Info.Pieces[off:off+20]) {
			res.Set(piece)
		}

		piece++
	}

	return res, false, nil
}

func NewVerify(torr *Torrent) Action {
	return &Verify{
		BaseAction: BaseAction{
			Torrent:   torr,
			PauseCh:   make(chan struct{}, 1),
			StopCh:    make(chan struct{}, 1),
			UnpauseCh: make(chan struct{}, 1),
		},
	}
}

func (torr *Torrent) RunAction(l Action) (interface{}, bool, error) {
	// XXX Stop is asynchronous, make it synchronous and wait
	// XXX allow timing this out?
	torr.Stop(context.Background())
	torr.action = l
	res, stopped, err := torr.action.Run()
	torr.action = nil

	return res, stopped, err
}

// OPT cache open files
type dataStorage struct {
	files  []dataStorageFile
	length int64
}

type dataStorageFile struct {
	Path   string
	Offset int64
	Size   int64
}

func (ds *dataStorage) add(path string, length int64) {
	ds.files = append(ds.files, dataStorageFile{path, ds.length, length})
	ds.length += length
}

func (ds *dataStorage) relevantFiles(n int, off int64) []dataStorageFile {
	// Skip past the requested offset.
	skipFiles := sort.Search(len(ds.files), func(i int) bool {
		// This function returns whether Files[i] will
		// contribute any bytes to our output.
		file := ds.files[i]
		return file.Offset+file.Size > off
	})
	return ds.files[skipFiles:]
}

func (ds *dataStorage) ReadAt(p []byte, off int64) (int, error) {
	wantN := len(p)

	files := ds.relevantFiles(len(p), off)
	// How far to skip in the first file.
	needSkip := off
	if len(files) > 0 {
		needSkip -= files[0].Offset
	}

	n := 0
	for len(files) > 0 && len(p) > 0 {
		readP := p
		fileSize := files[0].Size
		if int64(len(readP)) > fileSize-needSkip {
			readP = readP[:fileSize-needSkip]
		}

		f, err := os.Open(files[0].Path)
		if err == nil {
			rn, err := f.ReadAt(readP, needSkip)
			f.Close()
			if err != nil {
				if err == io.EOF {
					// file is shorter than it should be. pad with zeroes
					for i := range readP[rn:] {
						readP[i+rn] = 0
					}
				} else {
					return n, err
				}
			}
		} else {
			// if the file is missing we pad with zeros
			for i := range readP {
				readP[i] = 0
			}
		}
		// The return value of ReadAt doesn't matter. Either it read exactly len(readP) bytes, or it returned a non-nil error and we padded with zeroes.
		pn := len(readP)
		n += pn
		p = p[pn:]
		if int64(pn)+needSkip == fileSize {
			files = files[1:]
		}
		needSkip = 0
	}

	if n != wantN {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}
