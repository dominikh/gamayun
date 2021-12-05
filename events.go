package bittorrent

// XXX all events should contain a timestamp

import "time"

type Event interface {
	isEvent()
}

func (EventPeerTraffic) isEvent()      {}
func (EventPeerNoTraffic) isEvent()    {}
func (EventPeerDisconnected) isEvent() {}
func (EventAnnounceFailed) isEvent()   {}
func (EventPeerUnchoked) isEvent()     {}
func (EventPeerChoked) isEvent()       {}
func (EventPeerConnected) isEvent()    {}

// EventPeerTraffic is emitted once a peer has traffic.
// It will not be sent again for the same peer until an EventPeerNoTraffic has been emitted.
type EventPeerTraffic struct {
	Peer *Peer
}

// EventPeerNoTraffic is emitted once a peer no longer has any traffic.
type EventPeerNoTraffic struct {
	Peer *Peer
}

// EventPeerConnected is emitted when a peer has connected and finished its half of the handshake.
type EventPeerConnected struct {
	Peer    *Peer
	Torrent *Torrent
}

type EventPeerDisconnected struct {
	When time.Time
	Peer *Peer
	Err  error
}

type EventAnnounceFailed struct {
	Announce Announce
}

type EventPeerUnchoked struct {
	Peer    *Peer
	Torrent *Torrent
	Reason  string // XXX make this an enum
}

type EventPeerChoked struct {
	Peer    *Peer
	Torrent *Torrent
}
