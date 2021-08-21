package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"net/http"
	_ "net/http/pprof"

	"honnef.co/go/bittorrent"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	client := bittorrent.NewSession()
	go client.Run(context.TODO())

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

		if err := client.AddTorrent(minfo, infoHash); err != nil {
			log.Printf("could not add torrent %2x: %s", infoHash, err)
		}
	}

	t := time.NewTicker(5 * time.Second)
	for range t.C {
		stats := client.Statistics()
		fmt.Printf("%d up / %d down\n", stats.UploadedTotal, stats.DownloadedTotal)
	}
}
