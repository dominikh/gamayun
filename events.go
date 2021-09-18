package bittorrent

import "time"

type Event interface {
	isEvent()
}

func (EventPeerTraffic) isEvent()      {}
func (EventPeerDisconnected) isEvent() {}
func (EventAnnounceFailed) isEvent()   {}

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
}

type EventAnnounceFailed struct {
	Announce announce
}
