package bittorrent

import (
	"sync"
	"time"

	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/protocol"
)

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

	// Mutex used to prevent concurrent Start and Stop calls
	stateMu        sync.Mutex
	state          TorrentState
	action         Action
	trackerSession trackerSession
	peers          container.ConcurrentSet[*Peer]

	mu sync.RWMutex
	// Pieces we have
	have      Bitset
	announces []Announce
}

// getAnnounces empties the list of outstanding announces and returns it
func (torr *Torrent) getAnnounces() []Announce {
	torr.mu.Lock()
	defer torr.mu.Unlock()
	out := torr.announces
	torr.announces = nil
	return out
}

func (torr *Torrent) addAnnounce(ann Announce) {
	ann.Created = time.Now()
	ann.NextTry = ann.Created
	torr.mu.Lock()
	defer torr.mu.Unlock()
	torr.announces = append(torr.announces, ann)
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
		torr.state = TorrentStateSeeding
	} else {
		torr.state = TorrentStateLeeching
		panic("XXX: we cannot download torrents yet")
	}
}

func (torr *Torrent) Stop() {
	torr.stateMu.Lock()
	defer torr.stateMu.Unlock()

	if torr.action != nil {
		torr.action.Stop()

	} else {
		if torr.state != TorrentStateStopped {
			torr.state = TorrentStateStopped

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
	// XXX guard against a concurrent call to Start
	torr.action = l
	res, stopped, err := torr.action.Run()
	torr.action = nil

	return res, stopped, err
}
