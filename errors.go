package bittorrent

import (
	"errors"
	"fmt"

	"honnef.co/go/bittorrent/protocol"
)

var ErrClosing = errors.New("client is shutting down")

type Error struct {
	Error error
	Peer  *Peer
}

type UnknownTorrentError struct {
	hash protocol.InfoHash
}

func (err UnknownTorrentError) Error() string {
	return fmt.Sprintf("unknown torrent %s", err.hash)
}

type StoppedTorrentError struct {
	hash protocol.InfoHash
}

func (err StoppedTorrentError) Error() string {
	return fmt.Sprintf("stopped torrent %s", err.hash)
}

type WriteError struct {
	error
}

func (err WriteError) Unwrap() error {
	return err.error
}

type CallbackRejectedInfoHashError struct {
	Hash protocol.InfoHash
}

func (err CallbackRejectedInfoHashError) Error() string {
	return fmt.Sprintf("peer wasn't allowed to connect to torrent %s", err.Hash)
}

type CallbackRejectedPeerIDError struct {
	PeerID [20]byte
}

func (err CallbackRejectedPeerIDError) Error() string {
	return fmt.Sprintf("peer wasn't allowed to connect with peer ID %q", err.PeerID)
}
