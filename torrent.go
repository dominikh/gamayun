package bittorrent

import (
	"sync"
	"sync/atomic"
	"time"

	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/protocol"
)

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

	mu sync.RWMutex
	// Pieces we have
	have Bitset
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

			peers := torr.peers.Copy()

			for peer := range peers {
				peer.Close()
			}

			torr.addAnnounce(Announce{
				InfoHash: torr.Hash,
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
