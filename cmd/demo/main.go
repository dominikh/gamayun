package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"honnef.co/go/bittorrent"
	"honnef.co/go/bittorrent/protocol"
)

func main() {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	client := bittorrent.NewSession()
	client.Callbacks.PeerIncoming = func(pconn *protocol.Connection) bool {
		log.Println("New peer:", pconn)
		return true
	}
	client.Callbacks.PeerDisconnected = func(peer *bittorrent.Peer, err error) {
		log.Println("Peer disconnected:", peer, err)
	}
	client.Callbacks.PeerHandshakeInfoHash = func(peer *bittorrent.Peer, h protocol.InfoHash) bool {
		log.Printf("Peer %s wants to connect to torrent %s", peer, h)
		return true
	}
	client.Callbacks.PeerHandshakePeerID = func(peer *bittorrent.Peer, id [20]byte) bool {
		log.Printf("Peer %s wants to connect with peer ID %q", peer, id)
		return true
	}

	go func() {
		err := client.Run()
		log.Println("Client terminated:", err)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		log.Println("Shutting down")
		client.Shutdown(context.TODO())
		os.Exit(0)
	}()

	for _, arg := range os.Args[1:] {
		f, err := os.Open(arg)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		minfo, infoHash, err := bittorrent.ParseMetainfo(f)
		if err != nil {
			log.Fatal(err)
		}

		torr, err := client.AddTorrent(minfo, infoHash)
		if err != nil {
			log.Printf("could not add torrent %2x: %s", infoHash, err)
			continue
		}

		go func() {
			l := bittorrent.NewVerify(torr)
			result, stopped, err := torr.RunAction(l)
			log.Println(result, stopped, err)
			if err != nil {
				// XXX
			}
			if stopped {
				// XXX
			} else {
				torr.SetHave(result.(bittorrent.Bitset))
				if torr.IsComplete() {
					torr.Start()
				}
			}
		}()
	}

	t := time.NewTicker(5 * time.Second)

	// XXX leak
	traffic := map[*bittorrent.Torrent]struct{ up, down uint64 }{}
	for range t.C {
		for _, ev := range client.Events() {
			switch ev := ev.(type) {
			case bittorrent.EventPeerTraffic:
				tr := traffic[ev.Peer.Torrent]
				tr.up += ev.Up
				tr.down += ev.Down
				traffic[ev.Peer.Torrent] = tr
			}
		}

		for torr, tr := range traffic {
			fmt.Printf("%s: %d up / %d down\n", torr, tr.up, tr.down)
		}
	}
}
