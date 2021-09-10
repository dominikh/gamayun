package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"honnef.co/go/bittorrent"
)

func main() {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	client := bittorrent.NewSession()

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
	for range t.C {
		stats := client.Statistics()
		fmt.Printf("%d up / %d down\n", stats.UploadedTotal, stats.DownloadedTotal)
	}
}
