package bittorrent

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/mymath"
	"honnef.co/go/bittorrent/protocol"
)

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
type TorrentState uint8

const (
	TorrentStateStopped TorrentState = iota
	TorrentStateLeeching
	TorrentStateSeeding
)

type Torrent struct {
	Metainfo *Metainfo
	Hash     protocol.InfoHash

	availability Pieces
	data         *dataStorage
	session      *Session
	announces    announceQueue[Announce]

	// Mutex used to prevent concurrent Start and Stop calls
	stateMu        sync.Mutex
	state          TorrentState
	action         Action
	trackerSession trackerSession
	peers          container.ConcurrentSet[*Peer]

	peerMsgs chan peerMessage

	// Pieces we have
	have Bitset
}

func NewTorrent(hash protocol.InfoHash, info *Metainfo, sess *Session) *Torrent {
	return &Torrent{
		Metainfo: info,
		Hash:     hash,
		session:  sess,
		have:     NewBitset(),
		peers:    container.NewConcurrentSet[*Peer](),
		// OPT tweak buffers
		peerMsgs: make(chan peerMessage, 256),
	}
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
	torr.trackerSession.up = 0
	torr.trackerSession.down = 0
	torr.trackerSession.PeerID = torr.session.GeneratePeerID()
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
		panic("XXX: we cannot download torrents yet")
	}
}

func (torr *Torrent) updatePrometheusState(state TorrentState, delta uint64) {
	switch state {
	case TorrentStateLeeching:
		atomic.AddUint64(&torr.session.statistics.numTorrents.leeching, delta)
	case TorrentStateSeeding:
		atomic.AddUint64(&torr.session.statistics.numTorrents.seeding, delta)
	case TorrentStateStopped:
		atomic.AddUint64(&torr.session.statistics.numTorrents.stopped, delta)
	default:
		panic("unhandled state")
	}
}

func (torr *Torrent) setState(state TorrentState) {
	torr.updatePrometheusState(torr.state, ^uint64(0))
	torr.updatePrometheusState(state, 1)
	torr.state = state
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
				Up:       torr.trackerSession.up,
				Down:     torr.trackerSession.down,
			})
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

func (torr *Torrent) RunAction(l Action) (interface{}, bool, error) {
	torr.Stop()

	atomic.AddUint64(&torr.session.statistics.numTorrents.stopped, ^uint64(0))
	atomic.AddUint64(&torr.session.statistics.numTorrents.action, 1)

	// XXX guard against a concurrent call to Start
	torr.action = l
	res, stopped, err := torr.action.Run()
	torr.action = nil

	atomic.AddUint64(&torr.session.statistics.numTorrents.stopped, 1)
	atomic.AddUint64(&torr.session.statistics.numTorrents.action, ^uint64(0))

	return res, stopped, err
}

func (torr *Torrent) unchokePeers() {
	// TODO implement dynamic slot allocation, see https://github.com/dominikh/gamayun/issues/13
	// TODO make this value configurable
	const uploadSlotsPerTorrent = 5

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
		stat := atomic.SwapUint64(&peer.chokingStatistics.downloaded, 0)

		// All interested peers are candidates for unchoking
		if peer.peerInterested {
			interested = append(interested, struct {
				peer       *Peer
				downloaded uint64
			}{peer, stat})
		}
	}

	log.Printf("%s: %d currently unchoked peers, %d interested peers", torr, len(choke), len(interested))

	// The peers we eventually decided to unchoke
	unchoke := make([]*Peer, 0, uploadSlotsPerTorrent)

	torr.stateMu.Lock()
	state := torr.state
	torr.stateMu.Unlock()

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
			torr.session.rngMu.Lock()
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
			torr.session.rng.Shuffle(len(remainder), func(i, j int) {
				remainder[i], remainder[j] = remainder[j], remainder[i]
			})
			torr.session.rngMu.Unlock()
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
}

func (torr *Torrent) handlePeerMessage(peer *Peer, msg protocol.Message) error {
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
						torr.availability.inc(uint32(bit))
					}
				}
			}

		case protocol.MessageTypeHaveNone:
		case protocol.MessageTypeHaveAll:
			// OPT more efficient representation for HaveAll
			for i := 0; i < torr.NumPieces(); i++ {
				peer.have.Set(uint32(i))
			}
			torr.availability.incAll()
			defer torr.availability.decAll()
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
			// XXX ignore Have if we've gotten HaveAll before (or is it a protocol violation?)
			peer.have.Set(msg.Index)
			torr.availability.inc(msg.Index)

			// If we're not yet interested in the peer and we do need this piece, become interested
			// OPT don't check if we're a seeder
			if !peer.amInterested {
				if !torr.have.Get(msg.Index) {
					err := peer.controlWrite(protocol.Message{
						Type: protocol.MessageTypeInterested,
					})
					if err != nil {
						return err
					}
				}
			}
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

	return nil
}

func (torr *Torrent) trackPeer(peer *Peer) (peerID [20]byte, err error) {
	torr.stateMu.Lock()
	defer torr.stateMu.Unlock()
	if torr.state == TorrentStateStopped {
		// Don't let peers connect to stopped torrents
		atomic.AddUint64(&torr.session.statistics.numRejectedPeers.stoppedTorrent, 1)
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
	if torr.have.count == 0 {
		err := peer.controlWrite(protocol.Message{
			Type: protocol.MessageTypeHaveNone,
		})
		if err != nil {
			return err
		}
	} else if torr.have.count == torr.NumPieces() {
		err := peer.controlWrite(protocol.Message{
			Type: protocol.MessageTypeHaveAll,
		})
		if err != nil {
			return err
		}
	} else if torr.have.count*haveMessageSize < torr.NumPieces()/8 || true {
		// it's more compact to send a few Have messages than a bitfield that is mostly zeroes
		for i := 0; i < torr.NumPieces(); i++ {
			if torr.have.bits.Bit(i) != 0 {
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
				// XXX handle haveAll
				torr.availability.dec(uint32(n))
			}
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
					channel.TrySend(pmsg.peer.errs, err)
				}
			}
		}
	}
}

type peerMessage struct {
	peer *Peer
	msg  protocol.Message
	err  error
}
