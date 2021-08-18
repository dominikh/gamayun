package bittorrent

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zeebo/bencode"
	"honnef.co/go/bittorrent/protocol"
)

type Bitfield []byte

func NewBitflag(size uint32) Bitfield {
	if size == 0 {
		return Bitfield{}
	}
	return make(Bitfield, (size-1)/8+1)
}

func (bits Bitfield) Set(idx uint32, b bool) {
	return
	panic("not implemented")
}

func (bits Bitfield) Has(idx uint32) bool {
	return false
	panic("not implemented")
}

// TODO implement rate limiting by handing out tokens via channels

type Torrent struct {
	Metainfo   *Metainfo
	InfoHash   [20]byte
	Peers      []*Peer
	KnownPeers []PeerInfo

	// bitflag of which blocks we already have
	// OPT(dh): nil out when we have the whole torrent.
	//
	// OPT(dh): 80 GiB / 16 KiB * 1 bit amounts to 640 KiB, so it
	// doesn't seem worth tracking blocks at piece granularity,
	// especially when each slice has 24 bytes of overhead, or 1.875
	// MiB for 1 MiB pieces in a 80 GiB torrent.
	blocks Bitfield
	files  []io.WriterAt
	// OPT(dh): implement a data structure that maintains priority
	// sort. c.f.
	// https://blog.libtorrent.org/2011/11/writing-a-fast-piece-picker/
	pieces []Piece
}

type Piece struct {
	Availability uint32
}

// a torrent has many pieces.
// each piece has many blocks.
// clients advertise having pieces, but download blocks.
// for incomplete pieces we need to know which blocks we already have/have queued up.
// we need to know which peers have which pieces
// - we need to know the rarity of a piece
// - we need to know which pieces to decrement when peer leaves

type Peer struct {
	Conn *protocol.Connection

	Pieces Bitfield
	// XXX a map is not the best data structure for this; we probably
	// want to sort requests by piece and block so we can serve
	// responses more sequentially
	InboundRequests map[protocol.Request]struct{}

	// XXX also needs to be a map, possibly with a separate counter
	OutboundRequests uint32

	// OPT(dh): bitflags
	AmChoking      bool
	AmInterested   bool
	PeerChoking    bool
	PeerInterested bool
}

func NewPeer(tr *Torrent, conn *protocol.Connection) *Peer {
	return &Peer{
		Conn:            conn,
		Pieces:          NewBitflag(uint32(len(tr.pieces))),
		InboundRequests: map[protocol.Request]struct{}{},
		AmChoking:       true,
		PeerChoking:     true,
	}
}

type Client struct {
	PeerID   [20]byte
	torrents map[[20]byte]*Torrent
	active   []*Torrent
}

func NewClient() *Client {
	// XXX better peer ID
	var peerID [20]byte
	rand.Read(peerID[:])

	return &Client{
		PeerID:   peerID,
		torrents: map[[20]byte]*Torrent{},
	}
}

func (cl *Client) AddTorrent(info *Metainfo, infoHash [20]byte) *Torrent {
	// XXX thread safety

	tr := &Torrent{
		Metainfo: info,
		InfoHash: infoHash,
		// XXX calculate correct number of pieces
		pieces: make([]Piece, 9999),
	}
	cl.torrents[infoHash] = tr
	return tr
}

func (cl *Client) announce(tr *Torrent, event string) (*TrackerResponse, error) {
	req, err := http.NewRequest(http.MethodGet, tr.Metainfo.Announce, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q["info_hash"] = []string{string(tr.InfoHash[:])}
	q["peer_id"] = []string{string(cl.PeerID[:])}
	// XXX proper values
	q["port"] = []string{"9999"}
	q["uploaded"] = []string{"0"}
	q["downloaded"] = []string{"0"}
	// XXX support multi file mode
	q["left"] = []string{strconv.FormatUint(tr.Metainfo.Info.Length, 10)}
	q["numwant"] = []string{"200"}
	if event != "" {
		q["event"] = []string{event}
	}

	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tresp TrackerResponse
	if err := bencode.NewDecoder(resp.Body).Decode(&tresp); err != nil {
		return nil, err
	}

	return &tresp, nil
}

const (
	EventStarted   = "started"
	EventCompleted = "completed"
	EventStopped   = "stopped"
)

func (cl *Client) RunPeer(tr *Torrent, peer *Peer) error {
	// XXX decrement availabilities when peer disconnects
	// XXX close TCP connection when we're done

	// XXX add code handling the case when we're a seeder

	for {
		// OPT receive more than one message at once
		msg, err := peer.Conn.ReadMessage()
		if err != nil {
			return err
		}
		log.Printf("message from %s: %s", peer.Conn.Conn.RemoteAddr(), msg)

		switch msg.Type {
		case protocol.MessageTypeKeepAlive:
			// nothing to do
		case protocol.MessageTypeChoke:
			peer.PeerChoking = true

			if !peer.Conn.HasFastPeers {
				// XXX consider all outgoing requests cancelled
			}
		case protocol.MessageTypeUnchoke:
			peer.PeerChoking = false
		case protocol.MessageTypeInterested:
			peer.PeerInterested = true
		case protocol.MessageTypeNotInterested:
			peer.PeerInterested = false
		case protocol.MessageTypeHave:
			peer.Pieces.Set(msg.Index, true)
			tr.IncrementAvailability(msg.Index)
		case protocol.MessageTypeBitfield:
			// XXX it's an error if we receive this in the middle of a session
			// XXX verify length of bitfield

			copy(peer.Pieces, msg.Data)
			// OPT(dh): for peers that have all pieces keep a single
			// counter instead of incrementing every single piece
			for i, b := range peer.Pieces {
				for j := 0; j < 8; j++ {
					if (b & (1 << (8 - j - 1))) != 0 {
						tr.IncrementAvailability(uint32(i*8 + j))
					}
				}
			}
		case protocol.MessageTypeRequest:
			// XXX check that request is sane
			// XXX limit maximum requests
			// XXX drop or cancel request if we're choking the peer
			peer.InboundRequests[msg] = struct{}{}
		case protocol.MessageTypePiece:
			// XXX check that we actually requested this piece (though consider the race with Cancel)
			// XXX process the piece (hash and store and update our bitmap)

			atomic.AddUint32(&peer.OutboundRequests, ^uint32(0))
		case protocol.MessageTypeCancel:
			// XXX what happens if Length doesn't match the request?
			delete(peer.InboundRequests, protocol.Request(msg))
		case protocol.MessageTypeHaveAll:
			// XXX it's an error if we receive this in the middle of a session
			// XXX don't accept HaveAll from peers without the extension enabled
			for i := range peer.Pieces {
				peer.Pieces[i] = 0xFF
			}
		case protocol.MessageTypeHaveNone:
			// XXX it's an error if we receive this in the middle of a session
			// XXX don't accept HaveNone from peers without the extension enabled
		default:
			panic(fmt.Sprintf("unreachable: %T", msg))
		}
	}
}

func (cl *Client) RunTorrent(tr *Torrent) error {
	tresp, err := cl.announce(tr, EventStarted)
	if err != nil {
		// XXX retry until it succesds, with exponential backoff.
		// though the retry logic should probably exist in the client.
		log.Fatal(err)
	}

	tr.KnownPeers = append(tr.KnownPeers, tresp.Peers...)

	for _, kp := range tr.KnownPeers[:10] {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", kp.IP, kp.Port), 2*time.Second)
		if err != nil {
			log.Println(err)
			continue
		}
		peer := NewPeer(tr, &protocol.Connection{Conn: conn})
		if err := peer.Conn.SendHandshake(tr.InfoHash, cl.PeerID); err != nil {
			log.Println(err)
			continue
		}
		// XXX verify the info hash
		if _, err := peer.Conn.ReadHandshake(); err != nil {
			log.Println(err)
			continue
		}
		// XXX verify their peer ID
		if _, err := peer.Conn.ReadPeerID(); err != nil {
			log.Println(err)
			continue
		}
		go func() {
			if err := cl.RunPeer(tr, peer); err != nil {
				log.Println(err)
			}
		}()

		tr.Peers = append(tr.Peers, peer)
	}

	for {
		time.Sleep(time.Second)
		// XXX thread safety
		for _, peer := range tr.Peers {
			// XXX data race when accessing PeerChoking
			if atomic.LoadUint32(&peer.OutboundRequests) == 0 && !peer.PeerChoking {
				msg := protocol.Request{
					Index:  0,
					Begin:  0,
					Length: 1 << 14,
				}
				atomic.AddUint32(&peer.OutboundRequests, 1)
				if err := peer.Conn.WriteMessage(msg); err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	// var ch chan PeerInfo

	// for {
	// 	// - announce at an interval
	// 	// - scrape at an interval
	// 	// - optimistically unchoke peer at an interval
	// 	// - select peers to unchoke at an interval
	// 	select {}
	// }
}

func (tr *Torrent) IncrementAvailability(piece uint32) {
	atomic.AddUint32(&tr.pieces[piece].Availability, 1)
}

func pickPiece(peer *Peer) {

}
