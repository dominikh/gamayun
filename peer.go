package bittorrent

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/protocol"
)

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
	setup        bool
	amInterested bool
	peerChoking  bool
	lastHaveSent Bitset
	Torrent      *Torrent // XXX make sure this doesn't need locking, now that it is exported

	// mutable, needs lock held
	mu             sync.RWMutex
	amChoking      bool
	peerInterested bool

	// Last time the peer was unchoked. This is set both when initially unchoking the peer, and when choking it.
	//
	// Accessed exclusively from Session.unchokePeers.
	lastUnchoke time.Time

	// accessed atomically
	droppedRequests uint32
	// amount of data transfered since the last time we've reported statistics
	statistics struct {
		// XXX check alignment on 32-bit systems
		// XXX differentiate raw traffic and data traffic

		// How much data we have uploaded to the peer
		uploaded uint64
		// How much data we have downloaded from the peer
		downloaded  uint64
		last        time.Time
		lastWasZero bool
	}

	// accessed atomically, used by the choking algorithm
	chokingStatistics struct {
		downloaded uint64
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

func (peer *Peer) readPeer() error {
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
			atomic.AddUint64(&peer.session.statistics.downloadedRaw, size)
			atomic.AddUint64(&peer.statistics.downloaded, size)
			atomic.AddUint64(&peer.chokingStatistics.downloaded, size)
			select {
			case peer.msgs <- msg:
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
		atomic.AddUint64(&peer.session.statistics.uploadedRaw, uint64(msg.Size()))
		atomic.AddUint64(&peer.statistics.uploaded, uint64(msg.Size()))
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

func (peer *Peer) run() error {
	defer peer.conn.Close()

	// XXX add handshake and peer id to DownloadedTotal stat
	hash, err := peer.conn.ReadHandshake()
	if err != nil {
		return err
	}

	if peer.session.Callbacks.PeerHandshakeInfoHash != nil {
		if !peer.session.Callbacks.PeerHandshakeInfoHash(peer, hash) {
			atomic.AddUint64(&peer.session.statistics.numRejectedPeers.peerHandshakeInfoHashCallback, 1)
			return CallbackRejectedInfoHashError{hash}
		}
	}

	peer.session.mu.RLock()
	torr, ok := peer.session.torrents[hash]
	peer.session.mu.RUnlock()
	if !ok {
		atomic.AddUint64(&peer.session.statistics.numRejectedPeers.unknownTorrent, 1)
		return UnknownTorrentError{hash}
	}

	// XXX once we support removing torrents, this will race

	torr.stateMu.Lock()
	if torr.state == TorrentStateStopped {
		// Don't let peers connect to stopped torrents
		torr.stateMu.Unlock()
		atomic.AddUint64(&peer.session.statistics.numRejectedPeers.stoppedTorrent, 1)
		return StoppedTorrentError{hash}
	}
	peerID := torr.trackerSession.PeerID
	// Add to torr.peers while under the torr.statsMu lock so that Torrent.Stop doesn't miss any peers
	torr.addPeer(peer)
	torr.stateMu.Unlock()
	defer func() {
		// No need to hold torr.statsMu here, we're synchronized under Torrent.Stop waiting for Peer.run to return
		torr.removePeer(peer)
	}()

	peer.Torrent = torr

	if err := peer.conn.SendHandshake(hash, peerID); err != nil {
		return WriteError{err}
	}

	id, err := peer.conn.ReadPeerID()
	if err != nil {
		return err
	}
	peer.peerID = id
	if peer.session.Callbacks.PeerHandshakePeerID != nil {
		if !peer.session.Callbacks.PeerHandshakePeerID(peer, id) {
			return CallbackRejectedPeerIDError{id}
		}
	}

	errs := make(chan error, 1)
	// OPT multiple reader goroutines?
	//
	// These goroutines will exit either when they encounter read/write errors on the connection or when Peer.done gets closed.
	// This combination should ensure that the goroutines always terminate when Peer.run returns.
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

	// Decrement availability of all pieces this peer had. Gets used
	// if the first message was HaveNone or Bitfield. HaveAll instead
	// increments the "haveAll" offset.
	decrementAvailability := func() {
		bits := peer.have.bits
		n := bits.BitLen()
		for i := 0; i < n; i++ {
			if bits.Bit(n) != 0 {
				peer.Torrent.availability.dec(uint32(n))
			}
		}
	}

	defer peer.updateStats()
	trafficTicker := time.NewTicker(peerStatisticsInterval)
	t := time.NewTicker(time.Second)
	for {
		select {
		case err := <-errs:
			// This will also fire when we're shutting down, because blockReader, readPeer and writePeer will fail,
			// because the peer connections will get closed by Session.Run
			return err
		case <-trafficTicker.C:
			peer.updateStats()
		case <-t.C:
			torr := peer.Torrent
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
					// XXX verify length and shape of bitfield
					peer.have.SetBitfield(msg.Data)
					bit := 0
					for _, b := range msg.Data {
						for n := 7; n >= 0; n-- {
							bit++
							if b&1<<n != 0 {
								peer.Torrent.availability.inc(uint32(bit))
							}
						}
					}

					// Decrement availability of all pieces this peer had
					defer decrementAvailability()
				case protocol.MessageTypeHaveNone:
					defer decrementAvailability()
				case protocol.MessageTypeHaveAll:
					// OPT more efficient representation for HaveAll
					for i := 0; i < peer.Torrent.NumPieces(); i++ {
						peer.have.Set(uint32(i))
					}
					peer.Torrent.availability.incAll()
					defer peer.Torrent.availability.decAll()
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
					peer.mu.RLock()
					amChoking := peer.amChoking
					peer.mu.RUnlock()
					if amChoking {
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
					peer.mu.Lock()
					peer.peerInterested = true
					peer.mu.Unlock()
				case protocol.MessageTypeNotInterested:
					peer.mu.Lock()
					peer.peerInterested = false
					peer.mu.Unlock()
				case protocol.MessageTypeHave:
					// XXX check bounds
					// XXX ignore Have if we've gotten HaveAll before (or is it a protocol violation?)
					peer.have.Set(msg.Index)
					peer.Torrent.availability.inc(msg.Index)
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

func (peer *Peer) choke() error {
	peer.mu.Lock()
	defer peer.mu.Unlock()
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
	peer.mu.Lock()
	defer peer.mu.Unlock()
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

func (peer *Peer) updateStats() {
	now := time.Now()
	defer func() { peer.statistics.last = now }()
	up := atomic.SwapUint64(&peer.statistics.uploaded, 0)
	down := atomic.SwapUint64(&peer.statistics.downloaded, 0)

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
	atomic.AddUint64(&peer.Torrent.trackerSession.up, up)
	atomic.AddUint64(&peer.Torrent.trackerSession.down, down)

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
