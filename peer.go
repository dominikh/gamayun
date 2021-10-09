package bittorrent

import (
	"log"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/oursync"
	"honnef.co/go/bittorrent/protocol"
)

type Peer struct {
	conn    *protocol.Connection
	session *Session

	peerID [20]byte
	// The peer's client, as reported by the extended handshake
	ClientName string

	// How many outstanding incoming requests the peer accepts
	maxOutgoingRequests int

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

	// mutable, but doesn't need locking
	// Pieces the peer has
	have Bitset
	// The peer has sent one of the allowed initial messages
	setup   bool
	Torrent *Torrent // XXX make sure this doesn't need locking, now that it is exported

	amChoking      bool
	amInterested   bool
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
	}

	// Statistics used by the choking algorithm.
	chokingStatistics struct {
		downloaded oursync.Uint64
	}

	// Incoming requests from this peer.
	// Torrent.run sends to it, Peer.blockReader reads from it.
	incomingRequests chan request

	// Messages to write to the peer.
	// Several places send to it, Peer.writePeer reads from it.
	controlWrites chan protocol.Message
	writes        chan protocol.Message

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
				_, err := peer.Torrent.data.ReadAt(buf, int64(req.Index)*int64(peer.Torrent.Metainfo.Info.PieceLength)+int64(req.Begin))
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

func (peer *Peer) writePeer() error {
	writeMsg := func(msg protocol.Message) error {
		peer.session.statistics.uploadedRaw.Add(uint64(msg.Size()))
		peer.statistics.uploaded.Add(uint64(msg.Size()))
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

	if err := torr.startPeer(peer); err != nil {
		return err
	}

	msgs := make(chan protocol.Message)

	// OPT multiple reader goroutines?
	//
	// These goroutines will exit either when they encounter read/write errors on the connection or when Peer.done gets closed.
	// This combination should ensure that the goroutines always terminate when Peer.run returns.
	go func() { peer.Kill(peer.blockReader()) }()
	go func() { peer.Kill(peer.readPeer(msgs)) }()
	go func() { peer.Kill(peer.writePeer()) }()

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

func (peer *Peer) choke() error {
	if peer.amChoking {
		return nil
	}

	log.Println("choking", peer)
	peer.lastUnchoke = time.Now()
	peer.amChoking = true

	// XXX don't block trying to send the control message to a slow peer
	return peer.controlWrite(protocol.Message{
		Type: protocol.MessageTypeChoke,
	})
}

func (peer *Peer) unchoke() error {
	if !peer.amChoking {
		return nil
	}

	log.Println("unchoking", peer)
	peer.amChoking = false
	peer.lastUnchoke = time.Now()

	// XXX don't block trying to send the control message to a slow peer
	return peer.controlWrite(protocol.Message{
		Type: protocol.MessageTypeUnchoke,
	})
}

// Close closes the peer connection and waits for Peer.run to return.
func (peer *Peer) Close() {
	peer.conn.Close()
	<-peer.done
}

func (peer *Peer) String() string {
	return peer.conn.String()
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

	peer.session.addEvent(ev)
}
