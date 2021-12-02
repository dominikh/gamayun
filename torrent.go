package bittorrent

import (
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/mymath"
	"honnef.co/go/bittorrent/protocol"
)

// TODO implement dynamic slot allocation, see https://github.com/dominikh/gamayun/issues/13
// TODO make this value configurable
const uploadSlotsPerTorrent = 5

// TODO: when a blockReader fails, should we stop the torrent?

// OPT don't run a torrent goroutine if the torrent has no peers

type announceQueue[T any] struct {
	mu   sync.Mutex
	data []T
}

func (q *announceQueue[T]) Push(el T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.data = append(q.data, el)
}

func (q *announceQueue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if len(q.data) == 0 {
		return zero, false
	}
	el := q.data[0]
	copy(q.data, q.data[1:])
	q.data = q.data[:len(q.data)-1]
	return el, true
}

func (q *announceQueue[T]) Peek() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if len(q.data) == 0 {
		return zero, false
	}
	return q.data[0], true
}

func (q *announceQueue[T]) ReplaceFront(el T) {
	q.data[0] = el
}

//go:generate go run golang.org/x/tools/cmd/stringer@master -type TorrentState
type TorrentState uint32

const (
	TorrentStateStopped TorrentState = iota
	TorrentStateLeeching
	TorrentStateSeeding
)

type Torrent struct {
	Metainfo *Metainfo
	Hash     protocol.InfoHash

	pieces    Pieces
	data      *dataStorage
	session   *Session
	announces announceQueue[Announce]
	peerIDRng lockedRand
	mainRng   *rand.Rand

	// Mutex used to prevent concurrent Start and Stop calls
	stateMu        sync.Mutex
	state          TorrentState
	action         Action
	trackerSession trackerSession
	peers          container.ConcurrentSet[*Peer]

	numUnchoked int

	startPeers chan *Peer
	peerMsgs   chan peerMessage
}

func NewTorrent(hash protocol.InfoHash, info *Metainfo, sess *Session) *Torrent {
	torr := &Torrent{
		Metainfo: info,
		Hash:     hash,
		session:  sess,
		peers:    container.NewConcurrentSet[*Peer](),
		peerIDRng: lockedRand{
			rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		},
		mainRng: rand.New(rand.NewSource(time.Now().UnixNano())),
		// OPT tweak buffers
		startPeers: make(chan *Peer),
		peerMsgs:   make(chan peerMessage, 256),
	}
	n := uint32(torr.NumPieces())
	torr.pieces = Pieces{
		pieceIndices:     make([]uint32, n),
		sortedPieces:     make([]uint32, n),
		availabilities:   make([]uint16, n),
		buckets:          []uint32{n},
		requestedBlocks:  map[uint32]*Bitset{},
		downloadedBlocks: map[uint32]*Bitset{},
		pieceSize:        int(info.Info.PieceLength-1)/protocol.BlockSize + 1,
	}
	for i := uint32(0); i < n; i++ {
		torr.pieces.pieceIndices[i] = i
		torr.pieces.sortedPieces[i] = i
	}

	return torr
}

func (torr *Torrent) GeneratePeerID() [20]byte {
	const minAscii = 33
	const maxAscii = 127

	var peerID [20]byte
	copy(peerID[:], torr.session.PeerIDPrefix)
	if len(torr.session.PeerIDPrefix) < 20 {
		suffix := peerID[len(torr.session.PeerIDPrefix):]
		for i := range suffix {
			suffix[i] = byte(torr.peerIDRng.Intn(maxAscii-minAscii) + minAscii)
		}
	}

	return peerID
}

func (torr *Torrent) addAnnounce(ann Announce) {
	ann.Created = time.Now()
	ann.NextTry = ann.Created
	torr.announces.Push(ann)
}

func (torr *Torrent) Start() {
	torr.stateMu.Lock()
	defer torr.stateMu.Unlock()

	if torr.session.isClosing() {
		// Don't allow starting a torrent in an already stopped session
		return
	}

	if torr.state != TorrentStateStopped {
		return
	}

	if torr.action != nil {
		return
	}

	torr.trackerSession.nextAnnounce = time.Time{}
	torr.trackerSession.up.Store(0)
	torr.trackerSession.down.Store(0)
	torr.trackerSession.PeerID = torr.GeneratePeerID()
	torr.addAnnounce(Announce{
		InfoHash: torr.Hash,
		Tracker:  torr.Metainfo.Announce,
		PeerID:   torr.trackerSession.PeerID,
		Event:    "started",
	})

	if torr.IsComplete() {
		torr.setState(TorrentStateSeeding)
	} else {
		torr.setState(TorrentStateLeeching)
	}
}

func (torr *Torrent) updatePrometheusState(state TorrentState, delta uint64) {
	switch state {
	case TorrentStateLeeching:
		torr.session.statistics.numTorrents.leeching.Add(delta)
	case TorrentStateSeeding:
		torr.session.statistics.numTorrents.seeding.Add(delta)
	case TorrentStateStopped:
		torr.session.statistics.numTorrents.stopped.Add(delta)
	default:
		panic("unhandled state")
	}
}

func (torr *Torrent) setState(state TorrentState) {
	torr.updatePrometheusState(torr.state, ^uint64(0))
	torr.updatePrometheusState(state, 1)
	atomic.StoreUint32((*uint32)(&torr.state), uint32(state))
}

func (torr *Torrent) Stop() {
	torr.stateMu.Lock()
	defer torr.stateMu.Unlock()

	if torr.action != nil {
		torr.action.Stop()
	} else {
		if torr.state != TorrentStateStopped {
			torr.setState(TorrentStateStopped)

			for peer := range torr.peers.Copy() {
				peer.Close()
			}

			torr.addAnnounce(Announce{
				InfoHash: torr.Hash,
				Tracker:  torr.Metainfo.Announce,
				PeerID:   torr.trackerSession.PeerID,
				Event:    "stopped",
				Up:       torr.trackerSession.up.Load(),
				Down:     torr.trackerSession.down.Load(),
			})
		}
	}
}

func (torr *Torrent) String() string {
	return torr.Hash.String()
}

func (torr *Torrent) IsComplete() bool {
	// XXX locking
	return torr.pieces.have.count == torr.NumPieces()
}

func (torr *Torrent) SetHave(have Bitset) {
	torr.pieces.have = have
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

func (torr *Torrent) RunAction(l Action) (interface{}, bool, error) {
	torr.Stop()

	torr.session.statistics.numTorrents.stopped.Add(^uint64(0))
	torr.session.statistics.numTorrents.action.Add(1)

	// XXX guard against a concurrent call to Start
	torr.action = l
	res, stopped, err := torr.action.Run()
	torr.action = nil

	torr.session.statistics.numTorrents.stopped.Add(1)
	torr.session.statistics.numTorrents.action.Add(^uint64(0))

	return res, stopped, err
}

func (torr *Torrent) unchokePeers() {
	// Peers we have to choke at the end.
	// This starts as the set of all currently unchoked peers.
	// Peers we later decide to keep unchoked will be removed from it.
	choke := container.Set[*Peer]{}
	// All currently interested peers. A subset of them will be unchoked.
	var interested []struct {
		peer       *Peer
		downloaded uint64
	}

	for peer := range torr.peers.Copy() {
		// All unchoked peers are candidates for choking
		if !peer.amChoking {
			choke.Add(peer)
		}

		// We always have to reset the download counter, even if the peer isn't interested at this point in time.
		stat := peer.chokingStatistics.downloaded.Swap(0)

		// All interested peers are candidates for unchoking
		if peer.peerInterested {
			interested = append(interested, struct {
				peer       *Peer
				downloaded uint64
			}{peer, stat})
		}
	}

	type unchoke struct {
		peer   *Peer
		reason string
	}
	// The peers we eventually decided to unchokes
	unchokes := make([]unchoke, 0, uploadSlotsPerTorrent)

	// We use an atomic load instead of locking stateMu because of the
	// following possible deadlock: Torrent.Stop holds stateMu and
	// calls peer.Close. Closing a peer needs to send a message to be
	// received by Torrent.run, but Torrent.run would be stuck in
	// unchokePeers trying to obtain stateMu.
	//
	// Since we're not concerned about the state changing while in
	// unchokePeers, and we only want to read the current state, we
	// don't need to hold the lock.
	state := TorrentState(atomic.LoadUint32((*uint32)(&torr.state)))

	switch state {
	case TorrentStateLeeching:
		// In leeching mode we implement a tit-for-tat-inspired algorithm.
		// The peers that upload to us the fastest will be unchoked.
		// A small number of peers will be unchoked randomly,
		// so that we can find potentially better peers,
		// and to allow new clients to join the swarm.

		// 20% of all slots - but at least 1 - will be used for opportunistic unchoking
		opportunistic := mymath.Max(1, uploadSlotsPerTorrent/5)

		if len(interested) <= uploadSlotsPerTorrent {
			// Unchoke all interested peers
			for _, peer := range interested {
				unchokes = append(unchokes, unchoke{peer.peer, "few peers"})
			}
		} else {
			// Unchoke some peers based on their upload rate to us
			sort.Slice(interested, func(i, j int) bool {
				// favour the peers that upload the fastest to us
				return interested[i].downloaded < interested[j].downloaded
			})
			for _, peer := range interested[:uploadSlotsPerTorrent-opportunistic] {
				unchokes = append(unchokes, unchoke{peer.peer, "reciprocation"})
			}

			// XXX optimistic unchoking should happen every 30s, while normal unchoking happens every 10s.

			remainder := interested[uploadSlotsPerTorrent-opportunistic:]
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
			torr.mainRng.Shuffle(len(remainder), func(i, j int) {
				remainder[i], remainder[j] = remainder[j], remainder[i]
			})
			for _, peer := range remainder[:opportunistic] {
				unchokes = append(unchokes, unchoke{peer.peer, "optimistically"})
			}
		}

	case TorrentStateSeeding:
		// In seeding mode, we cycle through peers in a round-robin fashion.
		// This gives all peers an equal share of our time.

		if len(interested) <= uploadSlotsPerTorrent {
			// Unchoke all interested peers
			for _, peer := range interested {
				unchokes = append(unchokes, unchoke{peer.peer, "few peers"})
			}
		} else {
			// Unchoke peers in a round-robin manner
			sort.Slice(interested, func(i, j int) bool {
				return interested[i].peer.lastUnchoke.Before(interested[j].peer.lastUnchoke)
			})
			for _, peer := range interested[:uploadSlotsPerTorrent] {
				unchokes = append(unchokes, unchoke{peer.peer, "round robin"})
			}
		}

	default:
		// This can happen because of the race between grabbing the list of torrents and torrents stopping
		return
	}

	for _, u := range unchokes {
		// Don't choke peers we want to keep unchoked
		choke.Delete(u.peer)
	}

	actual := 0
	for _, u := range unchokes {
		if u.peer.amChoking {
			actual++
		}
	}

	// XXX don't block trying to send the control messages to a slow peer
	for peer := range choke {
		if err := peer.choke(); err != nil {
			// XXX kill peer
		} else {
			torr.session.addEvent(EventPeerChoked{peer, torr})
		}
	}
	for _, u := range unchokes {
		if u.peer.amChoking {
			if err := u.peer.unchoke(); err != nil {
				// XXX kill peer
			} else {
				torr.session.addEvent(EventPeerUnchoked{u.peer, torr, u.reason})
			}
		}
	}

	torr.numUnchoked = len(unchokes)
}

type ProtocolViolationError struct {
	reason string
}

func (err ProtocolViolationError) Error() string { return err.reason }

func (peer *Peer) becomeInterested() error {
	peer.amInterested = true
	return peer.controlWrite(protocol.Message{
		Type: protocol.MessageTypeInterested,
	})
}

func (torr *Torrent) handlePeerMessage(peer *Peer, msg protocol.Message) error {
	// TODO: We may still receive messages for a peer even after we've killed it. Should we automatically ignore those messages?

	if !peer.setup {
		if msg.Type == protocol.MessageTypeExtended {
			if msg.Data[0] != 0 {
				return ProtocolViolationError{"received extended message during peer setup phase, but it wasn't the extension handshake"}
			}
			hs, err := protocol.ParseExtendedHandshake(msg.Data[1:])
			if err != nil {
				return err
			}
			peer.extensions.ltDontHave = hs.Map["lt_donthave"]
			peer.extensions.shareMode = hs.Map["share_mode"]
			peer.extensions.uploadOnly = hs.Map["upload_only"]
			peer.extensions.utHolepunch = hs.Map["ut_holepunch"]
			peer.extensions.utMetadata = hs.Map["ut_metadata"]
			peer.ClientName = hs.ClientName
			peer.maxOutgoingRequests = hs.NumRequests
		} else {
			switch msg.Type {
			case protocol.MessageTypeBitfield:
				// XXX verify length and shape of bitfield
				peer.have.SetBitfield(msg.Data)
				bit := 0
				for _, b := range msg.Data {
					for n := 7; n >= 0; n-- {
						bit++
						if b&1<<n != 0 {
							torr.pieces.incAvailability(uint32(bit))
						}
					}
				}

			case protocol.MessageTypeHaveNone:
			case protocol.MessageTypeHaveAll:
				// OPT more efficient representation for HaveAll
				for i := 0; i < torr.NumPieces(); i++ {
					peer.have.Set(uint32(i))
					torr.pieces.incAvailability(uint32(i))
				}
			case protocol.MessageTypeExtended:
				// nothing to do for now
			default:
				return ProtocolViolationError{fmt.Sprintf("unexpected message %q in peer setup phase", msg)}
			}
			peer.setup = true

			var bits big.Int
			bits.AndNot(&peer.have.bits, &torr.pieces.have.bits)
			if bits.BitLen() != 0 {
				return peer.becomeInterested()
			}
		}
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
					return peer.controlWrite(protocol.Message{
						Type:   protocol.MessageTypeRejectRequest,
						Index:  msg.Index,
						Begin:  msg.Begin,
						Length: msg.Length,
					})
				}
			}
		case protocol.MessageTypeInterested:
			peer.peerInterested = true
			if torr.numUnchoked < uploadSlotsPerTorrent {
				// We have free slots, unchoke the peer immediately.
				torr.session.addEvent(EventPeerUnchoked{peer, torr, "immediately"})
				peer.unchoke()
				torr.numUnchoked++
			}
		case protocol.MessageTypeNotInterested:
			peer.peerInterested = false
		case protocol.MessageTypeHave:
			// XXX check bounds
			// XXX ignore Have if we've gotten HaveAll before (or is it a protocol violation?)
			peer.have.Set(msg.Index)
			torr.pieces.incAvailability(msg.Index)

			// If we're not yet interested in the peer and we do need this piece, become interested
			// OPT don't check if we're a seeder
			if !peer.amInterested {
				if !torr.pieces.have.Get(msg.Index) {
					return peer.becomeInterested()
				}
			}
		case protocol.MessageTypeChoke:
			// XXX consider outgoing requests as failed if the peer doesn't support RejectRequest messages
			peer.peerChoking = true
		case protocol.MessageTypeUnchoke:
			peer.peerChoking = false
			return torr.requestBlocks(peer)
		case protocol.MessageTypeAllowedFast:
			// XXX make use of this information
		case protocol.MessageTypeKeepAlive:
			// XXX should we respond?
		case protocol.MessageTypeExtended:
			// nothing to do for now
		case protocol.MessageTypeBitfield, protocol.MessageTypeHaveNone, protocol.MessageTypeHaveAll:
			return ProtocolViolationError{fmt.Sprintf("message %s after peer setup has finished", msg)}
		case protocol.MessageTypePiece:
			// XXX validate that we've actually requested this block
			// XXX validate length
			// XXX validate that msg.Begin is at a block boundary
			delete(peer.curOutgoingRequests, block{
				piece:  msg.Index,
				offset: int(msg.Begin),
			})
			if len(msg.Data) != protocol.BlockSize {
				// XXX don't panic, kill the peer
				// XXX handle the final block in the torrent
				panic(fmt.Sprintf("got %d bytes of data, not BlockSize", len(msg.Data)))
			}
			torr.requestBlocks(peer)
			off := int64(msg.Index)*int64(torr.Metainfo.Info.PieceLength) + int64(msg.Begin)
			if _, err := torr.data.WriteAt(msg.Data, off); err != nil {
				// XXX It's not enough to just return an error. That will
				// just kill the peer, but it's not a problem with the
				// peer, but with our storage, and we should stop the
				// torrent.
				return err
			}
			if torr.pieces.HaveBlock(msg.Index, msg.Begin/protocol.BlockSize) {
				// OPT(dh): move hashing to separate goroutine; we don't need backpressure from hashing.
				buf := make([]byte, torr.Metainfo.Info.PieceLength)
				// XXX handle final piece in torrent, which might be shorter
				_, err := torr.data.ReadAt(buf, int64(msg.Index)*int64(len(buf)))
				if err != nil {
					// XXX It's not enough to just return an error. That will
					// just kill the peer, but it's not a problem with the
					// peer, but with our storage, and we should stop the
					// torrent.
					return err
				}
				// TODO(dh): write a helper function for getting the
				// expected hash of a piece. this is needed in the
				// Verify action as well as here.
				off := msg.Index * 20
				if verifyBlock(buf, torr.Metainfo.Info.Pieces[off:off+20]) {
					torr.pieces.HavePiece(msg.Index)
					for otherPeer := range torr.peers.Copy() {
						// It's possible that a client connects, gets
						// sent the updated bitfield, then also gets a
						// Have message from this loop. As far as the
						// protocol is concerned, this should be fine.
						err := otherPeer.controlWrite(protocol.Message{
							Type:  protocol.MessageTypeHave,
							Index: msg.Index,
						})
						if err != nil {
							otherPeer.Kill(err)
						}
					}
				} else {
					// TODO(dh): figure out which peers sent us bad data
					torr.pieces.NeedPiece(msg.Index)
				}
			}
		case protocol.MessageTypeRejectRequest:
			// XXX validate that we've actually requested this block
			// XXX handle this, marking blocks as still needed
		case protocol.MessageTypeSuggestPiece:
			// TODO(dh): Can we do something useful with this?
		default:
			// XXX we're not yet handling all types
			return ProtocolViolationError{fmt.Sprintf("unexpected message %s", msg)}
		}
	}

	return nil
}

// requestBlocks requests more blocks from a peer if we're running low on outstanding requests.
func (torr *Torrent) requestBlocks(peer *Peer) error {
	if !peer.amInterested {
		return nil
	}

	// XXX respect peer.maxOutgoingRequests

	// XXX don't hard-code 10, use bandwidth-latency product
	if len(peer.curOutgoingRequests) > 10 {
		return nil
	}

	// XXX don't hard-code 100, base off bandwidth-latency product

	// XXX if ok == false, the peer has nothing to offer
	// us, and we should stop being interested. however,
	// we have to become interested again if requests to
	// another peer failed for a piece that this peer has.
	ranges, _ := torr.pieces.Pick(peer, 100-len(peer.curOutgoingRequests))
	for _, r := range ranges {
		for i := r.start; i < r.end; i++ {
			// XXX when requesting the last block of the last piece, the block may be shorter than protocol.BlockSize
			peer.curOutgoingRequests.Add(block{piece: r.piece, offset: i * protocol.BlockSize})
			err := peer.controlWrite(protocol.Message{
				Type:   protocol.MessageTypeRequest,
				Index:  r.piece,
				Begin:  uint32(i) * protocol.BlockSize,
				Length: protocol.BlockSize,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (torr *Torrent) trackPeer(peer *Peer) (peerID [20]byte, err error) {
	torr.stateMu.Lock()
	defer torr.stateMu.Unlock()
	if torr.state == TorrentStateStopped {
		// Don't let peers connect to stopped torrents
		torr.session.statistics.numRejectedPeers.stoppedTorrent.Add(1)
		return [20]byte{}, StoppedTorrentError{torr.Hash}
	}
	peer.Torrent = torr
	torr.peers.Add(peer)
	return torr.trackerSession.PeerID, nil
}

func (torr *Torrent) startPeer(peer *Peer) error {
	// We've received their handshake and peer ID and have
	// sent ours, now tell the peer which pieces we have
	const haveMessageSize = 4 + 1 + 4 // length prefix, message type, index
	if torr.pieces.have.count == 0 {
		err := peer.controlWrite(protocol.Message{
			Type: protocol.MessageTypeHaveNone,
		})
		if err != nil {
			return err
		}
	} else if torr.pieces.have.count == torr.NumPieces() {
		err := peer.controlWrite(protocol.Message{
			Type: protocol.MessageTypeHaveAll,
		})
		if err != nil {
			return err
		}
	} else if torr.pieces.have.count*haveMessageSize < torr.NumPieces()/8 || true {
		// it's more compact to send a few Have messages than a bitfield that is mostly zeroes
		for i := 0; i < torr.NumPieces(); i++ {
			if torr.pieces.have.bits.Bit(i) != 0 {
				err := peer.controlWrite(protocol.Message{
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

	return nil
}

func (torr *Torrent) removePeer(peer *Peer) {
	torr.peers.Delete(peer)

	if peer.setup {
		// Decrement availability of all pieces this peer had. Gets used
		// if the first message was HaveNone or Bitfield. HaveAll instead
		// increments the "haveAll" offset.
		bits := peer.have.bits
		n := bits.BitLen()
		for i := 0; i < n; i++ {
			if bits.Bit(n) != 0 {
				torr.pieces.decAvailability(uint32(n))
			}
		}

		for b := range peer.curOutgoingRequests {
			torr.pieces.NeedBlock(b.piece, uint32(b.offset/protocol.BlockSize))
		}
	}
}

// XXX we need to return from run when removing the torrent
//
// OPT ideally, there would be no torrent goroutine for idle torrents
func (torr *Torrent) run() {
	// OPT disable ticker when we have no peers, or consider having a single timer for all torrents
	tUnchoke := time.NewTicker(unchokeInterval)
	for {
		select {
		case <-tUnchoke.C:
			torr.unchokePeers()
		case pmsg := <-torr.peerMsgs:
			if pmsg.err != nil {
				torr.removePeer(pmsg.peer)
			} else {
				if err := torr.handlePeerMessage(pmsg.peer, pmsg.msg); err != nil {
					pmsg.peer.Kill(err)
				}
			}
		case peer := <-torr.startPeers:
			if err := torr.startPeer(peer); err != nil {
				peer.Kill(err)
			}
		}
	}
}

type peerMessage struct {
	peer *Peer
	msg  protocol.Message
	err  error
}
