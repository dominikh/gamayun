package main

import (
	"log"
	"os"

	"honnef.co/go/bittorrent"
)

func main() {
	client := bittorrent.NewClient()

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	minfo, infoHash, err := bittorrent.ParseMetainfo(f)
	if err != nil {
		log.Fatal(err)
	}

	tr := client.AddTorrent(minfo, infoHash)
	log.Println(client.RunTorrent(tr))
}
