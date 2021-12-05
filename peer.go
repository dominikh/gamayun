package bittorrent

import (
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/oursync"
	"honnef.co/go/bittorrent/protocol"
)

type Request struct {
	Piece  uint32
	Begin  uint32
	Length uint32
}

type peerState struct {
	amInterested bool
	amChoking    bool

	// these values are transient and get cleared when writing them to the peer
	weHave   Bitset
	request  []Request
	reject   []Request
	haveNone bool
	haveAll  bool
	bitfield Bitset
}

type Peer struct {
	conn    *protocol.Connection
	session *Session

	peerID [20]byte
	// The peer's client, as reported by the extended handshake
	ClientName string

	// How many outstanding incoming requests the peer accepts
	maxOutgoingRequests int

	// outstanding requests we've sent the peer
	curOutgoingRequests container.Set[Request]

	// A mapping of extensions to the IDs used by the peer (BEP 10).
	//
	// TODO only list the extensions we actually support; for now we list all of them just for testing purposes
	extensions struct {
		ltDontHave  int
		shareMode   int
		uploadOnly  int
		utHolepunch int
		utMetadata  int
	}

	desiredMu      sync.Mutex
	desiredState   peerState
	desiredUpdated chan struct{}
	currentState   peerState

	// mutable, but doesn't need locking
	// Pieces the peer has
	have Bitset
	// The peer has sent one of the allowed initial messages
	setup   bool
	Torrent *Torrent // XXX make sure this doesn't need locking, now that it is exported

	peerChoking    bool
	peerInterested bool

	// Last time the peer was unchoked. This is set both when initially unchoking the peer, and when choking it.
	//
	// Accessed exclusively from Session.unchokePeers.
	lastUnchoke time.Time

	// Amount of data transfered since the last time we've reported statistics as an event.
	statistics struct {
		// XXX check alignment on 32-bit systems
		// XXX differentiate raw traffic and data traffic

		// How much data we have uploaded to the peer
		uploaded oursync.Uint64
		// How much data we have downloaded from the peer
		downloaded  oursync.Uint64
		last        time.Time
		lastWasZero bool

		downloadHistory    [4]uint64
		downloadHistoryPtr int
	}

	// Statistics used by the choking algorithm.
	chokingStatistics struct {
		downloaded oursync.Uint64
	}

	// Incoming requests from this peer.
	// Torrent.run sends to it, Peer.blockReader reads from it.
	incomingRequests chan Request

	// Messages to write to the peer.
	// Several places send to it, Peer.writePeer reads from it.
	writes chan protocol.Message

	// A buffered channel that can hold one error.
	// It is used to communicate a fatal error to Peer.run.
	// Peer.Kill sends to it.
	errs chan error

	// This channel gets closed once Peer.run has returned.
	done chan struct{}
}

func NewPeer(conn *protocol.Connection, sess *Session) *Peer {
	return &Peer{
		conn:    conn,
		session: sess,
		// OPT tweak buffers
		incomingRequests:    make(chan Request, 256),
		writes:              make(chan protocol.Message, 256),
		done:                make(chan struct{}),
		curOutgoingRequests: container.NewSet[Request](),

		desiredUpdated: make(chan struct{}, 1),
		desiredState: peerState{
			amChoking: true,
		},
		currentState: peerState{
			amChoking: true,
		},
		peerInterested: false,
		peerChoking:    true,

		// Default value for peers that don't support an extended handshake
		maxOutgoingRequests: 250,
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
				_, err := peer.Torrent.Data.ReadAt(buf, int64(req.Piece)*int64(peer.Torrent.Metainfo.Info.PieceLength)+int64(req.Begin))
				if err != nil {
					return err
				}

				// XXX track upload stat

				return peer.write(protocol.Message{
					Type:   protocol.MessageTypePiece,
					Index:  req.Piece,
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

func (peer *Peer) readPeer(sendTo chan<- protocol.Message) error {
	msgs := make(chan protocol.Message)
	errs := make(chan error, 1)
	go func() {
		for {
			msg, err := peer.conn.ReadMessage()
			if err != nil {
				errs <- err
				return
			}
			select {
			case msgs <- msg:
			case <-peer.done:
				return
			}
		}
	}()

	for {
		select {
		case msg := <-msgs:
			size := uint64(msg.Size())
			peer.session.statistics.downloadedRaw.Add(size)
			peer.statistics.downloaded.Add(size)
			peer.chokingStatistics.downloaded.Add(size)
			select {
			case sendTo <- msg:
			case <-peer.done:
				return ErrClosing
			}
		case <-peer.done:
			return ErrClosing
		case err := <-errs:
			return err
		}
	}
}

func (peer *Peer) BecomeInterested() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	if !peer.desiredState.amInterested {
		peer.desiredState.amInterested = true
		channel.TrySend(peer.desiredUpdated, struct{}{})
	}
}

func (peer *Peer) BecomeUninterested() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	if peer.desiredState.amInterested {
		peer.desiredState.amInterested = false
		channel.TrySend(peer.desiredUpdated, struct{}{})
	}
}

func (peer *Peer) RejectRequest(index, begin, length uint32) {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	peer.desiredState.reject = append(peer.desiredState.reject, Request{index, begin, length})
	channel.TrySend(peer.desiredUpdated, struct{}{})
}

func (peer *Peer) Request(req Request) {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	peer.desiredState.request = append(peer.desiredState.request, req)
	channel.TrySend(peer.desiredUpdated, struct{}{})
}

func (peer *Peer) Have(index uint32) {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	peer.desiredState.weHave.Set(index)
	channel.TrySend(peer.desiredUpdated, struct{}{})
}

func (peer *Peer) HaveAll() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	peer.desiredState.haveAll = false
	channel.TrySend(peer.desiredUpdated, struct{}{})
}

func (peer *Peer) HaveNone() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	peer.desiredState.haveNone = false
	channel.TrySend(peer.desiredUpdated, struct{}{})
}

func (peer *Peer) Choke() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	if !peer.desiredState.amChoking {
		peer.desiredState.amChoking = true
		channel.TrySend(peer.desiredUpdated, struct{}{})
	}
}

func (peer *Peer) Unchoke() {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	if peer.desiredState.amChoking {
		peer.desiredState.amChoking = false
		channel.TrySend(peer.desiredUpdated, struct{}{})
	}
}

func (peer *Peer) AmChoking() bool {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	return peer.desiredState.amChoking
}

func (peer *Peer) AmInterested() bool {
	peer.desiredMu.Lock()
	defer peer.desiredMu.Unlock()
	return peer.desiredState.amInterested
}

func (peer *Peer) writePeer() error {
	writeMsg := func(msg protocol.Message) error {
		peer.session.statistics.uploadedRaw.Add(uint64(msg.Size()))
		peer.statistics.uploaded.Add(uint64(msg.Size()))
		return peer.conn.WriteMessage(msg)
	}
	writeMsgs := func(msgs []protocol.Message) error {
		for _, msg := range msgs {
			peer.session.statistics.uploadedRaw.Add(uint64(msg.Size()))
			peer.statistics.uploaded.Add(uint64(msg.Size()))
		}
		return peer.conn.WriteMessages(msgs)
	}

	for {
		select {
		case <-peer.desiredUpdated:
			peer.desiredMu.Lock()
			desired := peer.desiredState
			// OPT(dh): double buffer the slices
			peer.desiredState.request = nil
			peer.desiredState.reject = nil
			peer.desiredState.haveAll = false
			peer.desiredState.haveNone = false
			peer.desiredState.bitfield = Bitset{}
			peer.desiredState.weHave = Bitset{}
			peer.desiredMu.Unlock()

			// OPT(dh): reuse slice
			var msgs []protocol.Message

			if desired.haveNone {
				msgs = append(msgs, protocol.Message{
					Type: protocol.MessageTypeHaveNone,
				})
			}
			if desired.haveAll {
				msgs = append(msgs, protocol.Message{
					Type: protocol.MessageTypeHaveAll,
				})
			}
			if desired.bitfield.count.Load() > 0 {
				// XXX
				panic("not implemented")
			}
			if desired.amInterested != peer.currentState.amInterested {
				if desired.amInterested {
					msgs = append(msgs, protocol.Message{
						Type: protocol.MessageTypeInterested,
					})
				} else {
					msgs = append(msgs, protocol.Message{
						Type: protocol.MessageTypeNotInterested,
					})
				}
				peer.currentState.amInterested = desired.amInterested
			}
			if desired.amChoking != peer.currentState.amChoking {
				if desired.amChoking {
					msgs = append(msgs, protocol.Message{
						Type: protocol.MessageTypeChoke,
					})
				} else {
					msgs = append(msgs, protocol.Message{
						Type: protocol.MessageTypeUnchoke,
					})
				}
			}
			n := uint32(desired.weHave.bits.BitLen())
			for i := uint32(0); i < n; i++ {
				if desired.weHave.Get(i) {
					msgs = append(msgs, protocol.Message{
						Type:  protocol.MessageTypeHave,
						Index: i,
					})
				}
			}
			for _, req := range desired.request {
				msgs = append(msgs, protocol.Message{
					Type:   protocol.MessageTypeRequest,
					Index:  req.Piece,
					Begin:  req.Begin,
					Length: req.Length,
				})
			}
			for _, rej := range desired.reject {
				msgs = append(msgs, protocol.Message{
					Type:   protocol.MessageTypeRejectRequest,
					Index:  rej.Piece,
					Begin:  rej.Begin,
					Length: rej.Length,
				})
			}

			if err := writeMsgs(msgs); err != nil {
				return err
			}
		case msg := <-peer.writes:
			// OPT(dh): we could use channel.Collect here, but not
			// with an unconditional size of 256 elements. That would
			// mean 4 MiB of data, which can be seconds worth of data
			// for some users.
			if err := writeMsg(msg); err != nil {
				return err
			}
		case <-peer.done:
			return nil
		}
	}
}

func (peer *Peer) run() (err error) {
	defer peer.conn.Close()

	// XXX add handshake and peer id to DownloadedTotal stat
	hash, err := peer.conn.ReadHandshake()
	if err != nil {
		return err
	}

	if peer.session.Callbacks.PeerHandshakeInfoHash != nil {
		if !peer.session.Callbacks.PeerHandshakeInfoHash(peer, hash) {
			peer.session.statistics.numRejectedPeers.peerHandshakeInfoHashCallback.Add(1)
			return CallbackRejectedInfoHashError{hash}
		}
	}

	peer.session.mu.RLock()
	torr, ok := peer.session.torrents[hash]
	peer.session.mu.RUnlock()
	if !ok {
		peer.session.statistics.numRejectedPeers.unknownTorrent.Add(1)
		return UnknownTorrentError{hash}
	}

	// XXX once we support removing torrents, this will race

	ourPeerID, err := torr.trackPeer(peer)
	if err != nil {
		return err
	}
	defer func() {
		peer.Torrent.peerMsgs <- peerMessage{peer: peer, err: err}
	}()

	if err := peer.conn.SendHandshake(hash, ourPeerID); err != nil {
		return WriteError{err}
	}

	peerID, err := peer.conn.ReadPeerID()
	if err != nil {
		return err
	}
	peer.peerID = peerID
	if peer.session.Callbacks.PeerHandshakePeerID != nil {
		if !peer.session.Callbacks.PeerHandshakePeerID(peer, peerID) {
			return CallbackRejectedPeerIDError{peerID}
		}
	}

	if peer.conn.HasExtensionProtocol {
		// Send our extended handshake
		if err := peer.conn.SendExtendedHandshake(protocol.ExtendedHandshake{
			ClientName:  peer.session.ClientName,
			NumRequests: 250,
		}); err != nil {
			return err
		}
	}

	peer.errs = make(chan error, 1)
	msgs := make(chan protocol.Message)

	// OPT multiple reader goroutines?
	//
	// These goroutines will exit either when they encounter read/write errors on the connection or when Peer.done gets closed.
	// This combination should ensure that the goroutines always terminate when Peer.run returns.
	go func() { peer.Kill(peer.blockReader()) }()
	go func() { peer.Kill(peer.readPeer(msgs)) }()
	go func() { peer.Kill(peer.writePeer()) }()

	torr.startPeers <- peer

	defer peer.updateStats()
	trafficTicker := time.NewTicker(peerStatisticsInterval)
	for {
		select {
		case err := <-peer.errs:
			// This will also fire when we're shutting down, because blockReader, readPeer and writePeer will fail,
			// because the peer connections will get closed by Session.Run
			return err
		case msg := <-msgs:
			peer.Torrent.peerMsgs <- peerMessage{peer: peer, msg: msg}
		case <-trafficTicker.C:
			peer.updateStats()
		}
	}
}

// Close closes the peer connection and waits for Peer.run to return.
func (peer *Peer) Close() {
	peer.conn.Close()
	<-peer.done
}

func (peer *Peer) String() string {
	return peer.conn.Conn.RemoteAddr().String()
}

func (peer *Peer) Kill(err error) {
	channel.TrySend(peer.errs, err)
}

func (peer *Peer) updateStats() {
	now := time.Now()
	defer func() { peer.statistics.last = now }()
	up := peer.statistics.uploaded.Swap(0)
	down := peer.statistics.downloaded.Swap(0)

	// XXX the tracker cares about data traffic, not raw traffic
	//
	// We do not hold Torrent.stateMu here. updateStats cannot be
	// called concurrently with Torrent.Start, because Torrent.Start's
	// precondition implies that no peers exist. It can only be called
	// concurrently with Torrent.Stop, which will wait for all
	// still-running peers to stop before reading the torrent's stats.
	//
	// We do, however, have to use atomic operations, because multiple
	// peers may be updating the same torrent's stats.
	peer.Torrent.trackerSession.up.Add(up)
	peer.Torrent.trackerSession.down.Add(down)

	atomic.StoreUint64(&peer.statistics.downloadHistory[peer.statistics.downloadHistoryPtr], down)
	peer.statistics.downloadHistoryPtr = peer.statistics.downloadHistoryPtr % len(peer.statistics.downloadHistory)

	if up == 0 && down == 0 {
		if peer.statistics.lastWasZero {
			// Only skip emitting an event if the previous event was for zero bytes.
			// We want to emit a zero bytes event at least once so that clients can update the displayed rate
			return
		}
		peer.statistics.lastWasZero = true
	} else {
		peer.statistics.lastWasZero = false
	}

	ev := EventPeerTraffic{
		Start: peer.statistics.last,
		Stop:  now,
		Peer:  peer,
		Up:    up,
		Down:  down,
	}

	peer.session.emitEvent(ev)
}

// DownloadSpeed returns the average speed at which we're downloading from the peer, in bytes per second.
func (peer *Peer) DownloadSpeed() uint64 {
	var avg float64
	for i := range peer.statistics.downloadHistory {
		v := float64(atomic.LoadUint64(&peer.statistics.downloadHistory[i]))
		avg += (v - avg) / (float64(i) + 1)
	}
	return uint64(avg)
}
