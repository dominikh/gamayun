package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	_ "sync" // work around compiler bug
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"honnef.co/go/bittorrent"
	"honnef.co/go/bittorrent/container"
	"honnef.co/go/bittorrent/protocol"
)

type Torrent struct {
	Torrent *bittorrent.Torrent
	Peers   container.Set[*Peer]
}

type Peer struct {
	peer     *bittorrent.Peer
	up       uint64
	down     uint64
	unchoked bool
}

type Client struct {
	mu sync.Mutex

	sess     *bittorrent.Session
	peers    map[*bittorrent.Peer]*Peer
	torrents map[protocol.InfoHash]*Torrent

	symlinks []struct {
		from string
		to   protocol.InfoHash
	}
}

func NewClient() *Client {
	sess := bittorrent.NewSession()
	sess.PeerIDPrefix = []byte("GMY0001-")
	sess.ClientName = "Gamayun 0.0.1"

	return &Client{
		sess:     sess,
		peers:    map[*bittorrent.Peer]*Peer{},
		torrents: map[protocol.InfoHash]*Torrent{},
	}
}

func (cl *Client) lock()   { cl.mu.Lock() }
func (cl *Client) unlock() { cl.mu.Unlock() }

func (cl *Client) Run() error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- cl.sess.Run()
	}()

	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-t.C:
			cl.lock()
			for _, ev := range cl.sess.Events() {
				switch ev := ev.(type) {
				case bittorrent.EventPeerTraffic:
					peer := cl.peers[ev.Peer]
					peer.up += ev.Up
					peer.down += ev.Down
				case bittorrent.EventAnnounceFailed:
					// nothing to do
				case bittorrent.EventPeerUnchoked:
					cl.peers[ev.Peer].unchoked = true
				case bittorrent.EventPeerChoked:
					cl.peers[ev.Peer].unchoked = false
				case bittorrent.EventPeerConnected:
					peer := &Peer{peer: ev.Peer}
					cl.peers[ev.Peer] = peer
					torr := cl.torrents[ev.Torrent.InfoHash]
					torr.Peers.Add(peer)
				case bittorrent.EventPeerDisconnected:
					peer := cl.peers[ev.Peer]
					delete(cl.peers, ev.Peer)
					if ev.Peer.Torrent != nil {
						torr := cl.torrents[ev.Peer.Torrent.InfoHash]
						torr.Peers.Delete(peer)
					}
				default:
					panic(fmt.Sprintf("unhandled event: %v", ev))
				}
			}
			cl.unlock()
		case err := <-errCh:
			t.Stop()
			return err
		}
	}
}

func (cl *Client) AddTorrent(info *bittorrent.Metainfo, hash protocol.InfoHash) (*Torrent, error) {
	cl.lock()
	defer cl.unlock()

	torr, err := cl.sess.AddTorrent(info, hash)
	if err != nil {
		return nil, err
	}
	out := &Torrent{
		Torrent: torr,
		Peers:   container.Set[*Peer]{},
	}
	cl.torrents[torr.InfoHash] = out
	return out, nil
}

func doFuse(client *Client) error {
	c, err := fuse.Mount("./mnt", fuse.FSName("gamayun"))
	if err != nil {
		return err
	}

	defer c.Close()

	srv := fs.New(c, nil)
	fsys := &FS{
		srv:    srv,
		client: client,
	}
	return srv.Serve(fsys)
}

type FS struct {
	srv    *fs.Server
	client *Client
}

func (fsys *FS) Root() (fs.Node, error) {
	return &Dir{
		readDirAll: func(ctx context.Context) ([]fuse.Dirent, error) {
			return rootDirents, nil
		},
		lookup: func(ctx context.Context, name string) (fs.Node, error) {
			if name != "torrents" {
				return nil, syscall.ENOENT
			}

			return &Torrents{fsys.srv, fsys.client}, nil
		},
	}, nil
}

type TextFile struct {
	content func() string
}

func (file *TextFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0o444
	a.Size = uint64(len(file.content()))
	return nil
}

func (f *TextFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fuseutil.HandleRead(req, resp, []byte(f.content()))
	return nil
}

type FSPeer struct{}

func (peer *FSPeer) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0o444
	return nil
}

var rootDirents = []fuse.Dirent{
	{Type: fuse.DT_Dir, Name: "torrents"},
	{Type: fuse.DT_File, Name: "ctl"},
}
var torrentDirents = []fuse.Dirent{
	{Type: fuse.DT_Dir, Name: "files"},
	{Type: fuse.DT_Dir, Name: "peers"},
	{Type: fuse.DT_File, Name: "name"},
	{Type: fuse.DT_File, Name: "ctl"},
	{Type: fuse.DT_File, Name: "completion"},
}
var peerDirents = []fuse.Dirent{
	{Type: fuse.DT_File, Name: "stats"},
}

type Dir struct {
	readDirAll func(ctx context.Context) ([]fuse.Dirent, error)
	lookup     func(ctx context.Context, name string) (fs.Node, error)
}

func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dir.readDirAll(ctx)
}

func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	return dir.lookup(ctx, name)
}

type Torrents struct {
	srv    *fs.Server
	client *Client
}

func (torrs *Torrents) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o755
	return nil
}

func (torrs *Torrents) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	torrs.client.lock()
	defer torrs.client.unlock()

	var out []fuse.Dirent
	for hash := range torrs.client.torrents {
		out = append(out, fuse.Dirent{
			Type: fuse.DT_Dir,
			Name: hex.EncodeToString(hash[:]),
		})
	}

	for _, symlink := range torrs.client.symlinks {
		out = append(out, fuse.Dirent{
			Name: symlink.from,
			Type: fuse.DT_Link,
		})
	}
	return out, nil
}

// XXX we create new fs.Nodes all the time, but we should try to reuse
// them, so that files have stable NodeIDs.
func (torrs *Torrents) Lookup(ctx context.Context, name string) (fs.Node, error) {
	torrs.client.lock()
	defer torrs.client.unlock()

	// OPT(dh): use a map
	for _, symlink := range torrs.client.symlinks {
		if symlink.from == name {
			return &TorrentSymlink{symlink}, nil
		}
	}

	if len(name) != 40 {
		return nil, syscall.ENOENT
	}
	b, err := hex.DecodeString(name)
	if len(b) != 20 || err != nil {
		return nil, syscall.ENOENT
	}
	hash := *(*protocol.InfoHash)(b)
	torr, ok := torrs.client.torrents[hash]
	if !ok {
		return nil, syscall.ENOENT
	}
	return &Dir{
		readDirAll: func(ctx context.Context) ([]fuse.Dirent, error) {
			return torrentDirents, nil
		},
		lookup: func(ctx context.Context, name string) (fs.Node, error) {
			switch name {
			case "peers":
				return &Dir{
					readDirAll: func(ctx context.Context) ([]fuse.Dirent, error) {
						torrs.client.lock()
						defer torrs.client.unlock()

						var out []fuse.Dirent
						for peer := range torr.Peers {
							out = append(out, fuse.Dirent{
								Type: fuse.DT_Dir,
								Name: peer.peer.String(),
							})
						}
						return out, nil
					},
					lookup: func(ctx context.Context, name string) (fs.Node, error) {
						torrs.client.lock()
						defer torrs.client.unlock()

						// OPT(dh): add a map from string to peer
						for peer := range torr.Peers {
							if peer.peer.String() == name {
								return &Dir{
									readDirAll: func(ctx context.Context) ([]fuse.Dirent, error) {
										return peerDirents, nil
									},
									lookup: func(ctx context.Context, name string) (fs.Node, error) {
										switch name {
										case "stats":
											return &TextFile{func() string {
												torrs.client.lock()
												defer torrs.client.unlock()
												return fmt.Sprintf("%d %d %t\n", peer.up, peer.down, peer.unchoked)
											}}, nil
										case "ctl":
											// XXX
											return nil, syscall.EIO
										default:
											return nil, syscall.ENOENT
										}
									},
								}, nil
							}
						}
						return nil, syscall.ENOENT
					},
				}, nil
			case "name":
				return &TextFile{
					content: func() string { return torr.Torrent.Metainfo.Info.Name + "\n" },
				}, nil
			case "completion":
				return &TextFile{
					content: func() string {
						return fmt.Sprintf("%d %d\n", torr.Torrent.CompletedPieces(), torr.Torrent.NumPieces())
					},
				}, nil
			case "ctl":
				return &TorrentCtl{
					torr: torr,
				}, nil
			case "files":
				return &Dir{
					readDirAll: func(ctx context.Context) ([]fuse.Dirent, error) {
						var out []fuse.Dirent
						for _, f := range torr.Torrent.Data.Files {
							// XXX this doesn't work for multi-file torrents that include directories
							out = append(out, fuse.Dirent{
								Type: fuse.DT_File,
								Name: f.Path,
							})
						}
						return out, nil
					},
					lookup: func(ctx context.Context, name string) (fs.Node, error) {
						// OPT(dh): don't be O(n)
						for _, f := range torr.Torrent.Data.Files {
							if f.Path == name {
								return &TorrentFile{
									torr:   torr,
									offset: f.Offset,
									size:   f.Size,
								}, nil
							}
						}
						return nil, syscall.ENOENT
					},
				}, nil
			default:
				return nil, syscall.ENOENT
			}

		},
	}, nil
}

type TorrentCtl struct {
	torr *Torrent
}

func (c *TorrentCtl) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0o222
	return nil
}

func (c *TorrentCtl) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// XXX don't accept arbitrarily large data
	switch strings.TrimSpace(string(req.Data)) {
	case "start":
		c.torr.Torrent.Start()
	}
	resp.Size = len(req.Data)
	return nil
}

type TorrentFile struct {
	torr   *Torrent
	offset int64
	size   int64
}

func (c *TorrentFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0o444
	a.Size = uint64(c.size)
	return nil
}

func (c *TorrentFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if req.Offset >= c.size {
		return io.EOF
	}

	n, err := c.torr.Torrent.Data.ReadAt(resp.Data[:req.Size], c.offset)
	resp.Data = resp.Data[:n]
	return err
}

type TorrentSymlink struct {
	symlink struct {
		from string
		to   protocol.InfoHash
	}
}

func (c *TorrentSymlink) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeSymlink | 0o666
	return nil
}

func (c *TorrentSymlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return hex.EncodeToString(c.symlink.to[:]), nil
}

func (torrs *Torrents) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	tc := &TorrentCreate{
		srv:    torrs.srv,
		client: torrs.client,
		parent: torrs,
		name:   req.Name,
	}
	return tc, tc, nil
}

type TorrentCreate struct {
	srv    *fs.Server
	client *Client
	parent fs.Node
	name   string
	buf    bytes.Buffer
}

func (c *TorrentCreate) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0o644
	a.Size = uint64(c.buf.Len())
	return nil
}

func (c *TorrentCreate) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	c.buf.Write(req.Data)
	resp.Size = len(req.Data)
	return nil
}

func (c *TorrentCreate) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// XXX "delete" the file if parsing fails

	minfo, infoHash, err := bittorrent.ParseMetainfo(&c.buf)
	if err != nil {
		return err
	}

	_, err = c.client.AddTorrent(minfo, infoHash)
	if err != nil {
		return err
	}

	c.client.lock()
	defer c.client.unlock()
	c.client.symlinks = append(c.client.symlinks, struct {
		from string
		to   protocol.InfoHash
	}{
		from: c.name,
		to:   infoHash,
	})

	c.srv.InvalidateEntry(c.parent, c.name)

	return nil
}

var _ fs.HandleReleaser = (*TorrentCreate)(nil)
var _ fs.HandleWriter = (*TorrentCreate)(nil)

func main() {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(":2112", nil)
	}()

	client := NewClient()
	prometheus.DefaultRegisterer.MustRegister(client.sess)
	go doFuse(client)

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

		torr.Torrent.Start()
		// go func() {
		// 	l := bittorrent.NewVerify(torr)
		// 	result, stopped, err := torr.RunAction(l)
		// 	log.Println(result, stopped, err)
		// 	if err != nil {
		// 		// XXX
		// 		log.Fatal(err)
		// 	}
		// 	if stopped {
		// 		// XXX
		// 	} else {
		// 		torr.SetHave(result.(bittorrent.Bitset))
		// 		// if torr.IsComplete() {
		// 		torr.Start()
		// 		// }
		// 	}
		// }()
	}

	err := client.Run()
	log.Println("Client terminated:", err)
}
