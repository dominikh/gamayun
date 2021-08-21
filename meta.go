package bittorrent

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"net"

	"github.com/zeebo/bencode"
)

type Metainfo struct {
	Announce     string
	CreationDate uint64
	Comment      string
	CreatedBy    string
	Encoding     string
	Info         InfoDictionary
}

type metainfo struct {
	Announce     string             `bencode:"announce"`
	CreationDate uint64             `bencode:"creation date"`
	Comment      string             `bencode:"comment"`
	CreatedBy    string             `bencode:"created by"`
	Encoding     string             `bencode:"encoding"`
	Info         bencode.RawMessage `bencode:"info"`
}

type InfoDictionary struct {
	PieceLength uint64 `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
	Private     bool   `bencode:"private"`
	Name        string `bencode:"name"`

	// Single file Mode
	Length uint64 `bencode:"length"`

	// Multi File Mode
	Files []InfoFile `bencode:"files"`
}

type InfoFile struct {
	Length uint64   `bencode:"length"`
	Path   []string `bencode:"path"`
}

type TrackerRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	IP         string
	Port       int
	Uploaded   int
	Downloaded int
	Left       int
	Event      string
}

type TrackerResponse struct {
	FailureReason string    `bencode:"failure reason"`
	Interval      int       `bencode:"interval"`
	TrackerID     string    `bencode:"tracker id"`
	Complete      int       `bencode:"complete"`
	Incomplete    int       `bencode:"incomplete"`
	Peers         PeerInfos `bencode:"peers"`
}

type PeerInfos []PeerInfo

func (pi *PeerInfos) UnmarshalBencode(b []byte) error {
	if b[0] == 'l' {
		// list of dicts
		return bencode.DecodeBytes(b, (*[]PeerInfo)(pi))
	} else if b[0] >= '0' || b[0] <= '9' {
		// compact encoding
		var s string
		if err := bencode.DecodeBytes(b, &s); err != nil {
			return err
		}
		if len(s)%6 != 0 {
			return errors.New("malformed compact peer list encoding")
		}
		for len(s) > 0 {
			ip := net.IPv4(s[0], s[1], s[2], s[3])
			port := int(s[4])<<8 | int(s[5])
			*pi = append(*pi, PeerInfo{IP: ip.String(), Port: port})
			s = s[6:]
		}
	} else {
		return errors.New("unsupported peer list encoding")
	}
	return nil
}

type PeerInfo struct {
	PeerID string `bencode:"peer id"`
	IP     string `bencode:"ip"`
	Port   int    `bencode:"port"`
}

type ScrapeResponse struct {
	Files map[string]ScrapeFile `bencode:"files"`
}

type ScrapeFile struct {
	// Number of seeders
	Complete int `bencode:"complete"`
	// Number of times this torrent has been downloaded fully
	Downloaded int `bencode:"downloaded"`
	// Number of leechers
	Incomplete int `bencode:"incomplete"`
	// The torrent's name
	Name string `bencode:"name"`
}

func ParseMetainfo(r io.Reader) (*Metainfo, [20]byte, error) {
	var minfo metainfo
	if err := bencode.NewDecoder(r).Decode(&minfo); err != nil {
		return nil, [20]byte{}, err
	}

	hash := sha1.Sum([]byte(minfo.Info))
	out := &Metainfo{
		Announce:     minfo.Announce,
		CreationDate: minfo.CreationDate,
		Comment:      minfo.Comment,
		CreatedBy:    minfo.CreatedBy,
		Encoding:     minfo.Encoding,
	}
	err := bencode.NewDecoder(bytes.NewReader([]byte(minfo.Info))).Decode(&out.Info)
	return out, hash, err
}
