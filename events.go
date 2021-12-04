package bittorrent

// XXX all events should contain a timestamp

import "time"

type Event interface {
	isEvent()
}

func (EventPeerTraffic) isEvent()      {}
func (EventPeerDisconnected) isEvent() {}
func (EventAnnounceFailed) isEvent()   {}
func (EventPeerUnchoked) isEvent()     {}
func (EventPeerChoked) isEvent()       {}

type EventPeerTraffic struct {
	// XXX report both raw traffic and data traffic
	Start time.Time
	Stop  time.Time
	Peer  *Peer
	Up    uint64
	Down  uint64
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
