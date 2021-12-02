// Scalability
//
// TODO. We avoid polling that is O(torrents) by using events. E.g. instead of inspecting each torrent for its state or statistics, we'll emit events only for those torrents that had activity.
//
// Callbacks, events, and blocking calls
//
// We use three different mechanisms for communicating with users of this package.
//
// Callbacks
//
// Callbacks are used when we need to request data from the user.
// For example, in order to implement peer whitelists or blacklists, we'll ask the user to make a decision whenever a new incoming connection is made.
//
// Callbacks are executed synchronously as we cannot proceed until we get their results.
// In some cases, this means that callbacks should finish as quickly as possible, so that good performance can be maintained.
// In other cases, deliberate delays can be used to implement rate control, for example to throttle the rate of incoming connections.
//
// Events
//
// Events are used when we need to send data to the user but don't require a response.
// For example, periodic per-peer traffic statistics are reported as events.
// Events have to be polled periodically by the user.
// Not doing so will cause events to accumulate and grow memory usage without bounds.
//
// Some events may overlap with callbacks.
// For example, there is both a callback and an event for new peer connections.
// While callbacks can make decisions and block execution, events can do neither.
//
// Blocking calls
//
// Requests and actions originating from the user are handled by blocking calls.
// For example, verifying a torrent's data is done via a function call that will not return until verification has completed.
// If the user wishes to run actions in parallel with their other code, they have to set up concurrency themselves, by using goroutines.
package bittorrent

// XXX support removing torrents

// XXX handle clients that don't support the Fast extension; don't send them messages they won't understand

// TODO learn about socket options, send/receive buffers, corking, …

// TODO corking and/or nagle might make sense. blocks are obviously
// too large to benefit from it, but HAVE messages are small enough to
// be batched together. should we relegate this caching to the kernel,
// or should we cache in user space instead?

// TODO can we write to sockets without having to copy data from user space to kernel space?

// XXX have per-torrent peer ID

// XXX there is a possible deadlock in the protocol: the peer keeps
// sending us requests, we keep sending rejects. If our control write
// queue fills up, we'll block and stop reading messages, causing the
// peer's write queue to also fill up. Now neither client can make
// progress.
//
// This will probably only ever occur with misbehaving clients; sane
// clients won't flood us with requests.

// XXX handle read/write timeouts, both on disk i/o and network i/o

// XXX set user agent for HTTP requests

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/bits"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"honnef.co/go/bittorrent/channel"
	"honnef.co/go/bittorrent/oursync"
	"honnef.co/go/bittorrent/protocol"
)

type lockedRand struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func (rng *lockedRand) Intn(n int) int {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	return rng.rng.Intn(n)
}

// The maximum number of peers per torrent the code supports.
const maxPeersPerTorrent = 65535

// How often each peer updates the global and per-torrent traffic statistics.
const peerStatisticsInterval = 500 * time.Millisecond

type request struct {
	Index  uint32
	Begin  uint32
	Length uint32
}

func verifyBlock(data []byte, checksum []byte) bool {
	h := sha1.Sum(data)
	return bytes.Equal(h[:], checksum)
}

type trackerSession struct {
	PeerID       [20]byte
	nextAnnounce time.Time
	up           oursync.Uint64
	down         oursync.Uint64
}

type Announce struct {
	InfoHash protocol.InfoHash
	Tracker  string
	PeerID   [20]byte
	Event    string
	Up       uint64
	Down     uint64
	// XXX left

	Created time.Time
	Fails   []struct {
		When time.Time
		Err  error
	}
	NextTry time.Time
}

type Bitset struct {
	count int
	// OPT: don't use big.Int. use our own representation that matches
	// the wire protocol, so that we don't have to convert between the
	// two all the time
	bits big.Int
}

func (set Bitset) Copy() Bitset {
	nset := Bitset{
		count: set.count,
	}
	nset.bits.Set(&set.bits)
	return nset
}

func (set Bitset) Get(bit uint32) bool {
	return set.bits.Bit(int(bit)) == 1
}

func (set *Bitset) Set(bit uint32) {
	if set.bits.Bit(int(bit)) == 1 {
		return
	}
	set.count++
	set.bits.SetBit(&set.bits, int(bit), 1)
}

func (set *Bitset) Unset(bit uint32) {
	if set.bits.Bit(int(bit)) == 0 {
		return
	}
	set.count--
	set.bits.SetBit(&set.bits, int(bit), 0)
}

// SetBitfield populates set from a bittorrent bitfield.
// The first byte of the bitfield corresponds to indices 0 - 7 from high bit to low bit, respectively. The next one 8-15, etc.
func (set *Bitset) SetBitfield(buf []byte) {
	// OPT optimize this, probably by writing our own bitset type instead of using big.Int.
	// Note that we cannot use big.Int.SetBytes, because the byte layout doesn't match that of the bitfield.
	bit := 0
	for _, b := range buf {
		for n := 7; n >= 0; n-- {
			bit++
			if b&1<<n != 0 {
				set.count++
				set.bits.SetBit(&set.bits, bit, 1)
			} else {
				set.bits.SetBit(&set.bits, bit, 0)
			}
		}
	}
}

func (set Bitset) Intersect(other Bitset) Bitset {
	out := Bitset{}

	if set.count == 0 || other.count == 0 {
		return out
	}

	out.bits.And(&set.bits, &other.bits)

	// OPT: we'll call Intersect a lot, and Bytes allocates, so this is no good.
	// We could use FillBytes, but the loop is still O(n).
	for _, b := range out.bits.Bytes() {
		out.count += bits.OnesCount8(b)
	}
	return out
}

func (set *Bitset) String() string {
	return fmt.Sprintf("%d bits set", set.count)
}

// OPT track HaveAll separately

// OPT don't spread requests for the same piece among many peers, it
// wastes their disk caches. "The logic in libtorrent switches into
// the request-whole-pieces mode when a peer sends blocks fast enough
// to send an entire piece in 30 seconds."
//
// At an 8 MB piece piece, 30 seconds means 267 kB/s. We might be able
// to always default to requesting entire pieces, based on modern
// internet speeds. However, 8 MB pieces have 500 blocks, so we can't
// truly request them all at once without risking overflowing the
// peer's request queue.

// OPT: this data structure is fine for keeping a priority-sorted list
// of pieces, but finding a peer-piece combination requires looping
// over each piece and checking with the peer until we find one. IOW,
// it's O(n) worst case.
//
// TODO: prioritize pieces we've already downloading
type Pieces struct {
	// mapping from piece to entry in sorted_pieces
	pieceIndices []uint32

	// pieces sorted by availability, in ascending order, indexed into by piece_indices
	sortedPieces []uint32

	// mapping from piece to availability
	availabilities []uint16 // supports at most 65536 peers

	// mapping from priority to one past the last item in sorted_pieces that is part of that priority
	buckets []uint32

	// Mapping from piece to requested or downloaded blocks. This map
	// only contains entries for partially downloaded pieces.
	requestedBlocks map[uint32]*Bitset

	// Mapping from piece to downloaded blocks. This map only contains
	// entries for partially downloaded pieces.
	downloadedBlocks map[uint32]*Bitset

	// Pieces we have. These have been hashed and written to disk.
	have Bitset

	// The size of a single piece, in blocks
	pieceSize int
}

// Half-open interval [start, end) of blocks to request for piece.
//
// TODO(dh): give this type a better name
type blockRange struct {
	piece      uint32
	start, end int
}

// HavePiece records that we have a piece.
func (t *Pieces) HavePiece(piece uint32) {
	t.have.Set(piece)
	delete(t.requestedBlocks, piece)
	delete(t.downloadedBlocks, piece)
}

// NeedPiece records that we need a piece. This needs to be called if a downloaded piece failed verification, so that we request it again.
// This shouldn't be used on a piece that has already been marked as completed by Have.
func (t *Pieces) NeedPiece(piece uint32) {
	// TODO(dh): record which peers we got the piece from, try to find out which peer is to blame
	delete(t.requestedBlocks, piece)
	delete(t.downloadedBlocks, piece)
}

func (t *Pieces) NeedBlock(piece uint32, block uint32) {
	req, ok := t.requestedBlocks[piece]
	if !ok {
		panic(fmt.Sprintf("called NeedBlock(%d, %d), but piece %d doesn't have any currently requested blocks", piece, block, piece))
	}
	if req.bits.Bit(int(block)) == 0 {
		panic(fmt.Sprintf("called NeedBlock(%d, %d), but block %d wasn't requested", piece, block, block))
	}
	req.Unset(block)
}

func (t *Pieces) HaveBlock(piece uint32, block uint32) (complete bool) {
	req, ok := t.downloadedBlocks[piece]
	if !ok {
		req = new(Bitset)
		t.downloadedBlocks[piece] = req
	}
	req.Set(block)
	return req.count == t.pieceSize
}

// TODO(dh): this API cannot support end-game mode
func (t *Pieces) Pick(peer *Peer, numBlocks int) (ranges []blockRange, ok bool) {
	// Based on commonly used piece sizes, large torrents have between
	// 4096 and 24320 pieces. However, every once in a while, someone
	// uses the default piece size of 128 KiB even for very large
	// torrents, leading to hundreds of thousands of pieces. The
	// current implementation of the piece picker is technically
	// O(pieces) for picking a single piece, and O(pieces²) for
	// picking all pieces. However, in practice, only a small fraction
	// of all pieces get looked at.

	if !peer.amInterested {
		panic(fmt.Sprintf("trying to pick piece for peer %s we're not interested in", peer))
	}

	if peer.have.count == 0 {
		panic(fmt.Sprintf("interested in peer %s with no pieces", peer))
	}

	if t.availabilities[t.sortedPieces[len(t.sortedPieces)-1]] == 0 {
		panic(fmt.Sprintf("peer %s has pieces but we think nobody has any pieces", peer))
	}

	// OPT(dh): when the peer has much fewer pieces than the rest of
	// the swarm, then it should be faster to loop over the peer's
	// pieces and return the one with the highest priority. It's the
	// same O(pieces) because we have to look at each piece to know if
	// the peer has it, but since that's stored in a bitmap we can
	// check 64 pieces at a time.

	blocksForPiece := func(piece uint32, max int) (ranges []blockRange, n int) {
		if max > t.pieceSize {
			max = t.pieceSize
		}
		reqBlocks, ok := t.requestedBlocks[piece]
		if !ok {
			reqBlocks = new(Bitset)
			t.requestedBlocks[piece] = reqBlocks
		}
		if reqBlocks.count == 0 {
			// We haven't requested any blocks of this piece, so we can request from 0 to max
			ranges = append(ranges, blockRange{piece: piece, start: 0, end: max})
		} else if reqBlocks.count == t.pieceSize {
			// We have already requested all blocks in this piece
		} else {
			// Find contiguous ranges of blocks to request
			start := -1
			for bit := 0; bit < t.pieceSize; bit++ {
				if max < 0 {
					panic("max became negative")
				}
				if max == 0 {
					break
				}

				if reqBlocks.Get(uint32(bit)) {
					if start != -1 {
						// End range
						ranges = append(ranges, blockRange{piece: piece, start: start, end: bit})
						max -= bit - start
						start = -1
					}
				} else {
					if start == -1 {
						// Start range
						start = bit
					} else if bit-start == max {
						// End range early, we've gotten enough blocks
						ranges = append(ranges, blockRange{piece: piece, start: start, end: bit})
						start = -1
						max = 0
						break
					}
				}
			}

			if start != -1 {
				// We ran out of blocks, end range
				ranges = append(ranges, blockRange{piece: piece, start: start, end: t.pieceSize})
				max -= t.pieceSize - start
			}
		}

		n = 0
		for _, r := range ranges {
			n += r.end - r.start
			for bit := r.start; bit < r.end; bit++ {
				// OPT surely there's a better way than setting each bit individually
				reqBlocks.Set(uint32(bit))
			}
		}

		if len(ranges) > 0 && n == 0 {
			panic("have ranges but n == 0")
		}
		return ranges, n
	}

	// XXX prefer partial pieces
	for _, piece := range t.sortedPieces {
		if numBlocks < 0 {
			panic("numBlocks became negative")
		}
		if numBlocks == 0 {
			break
		}

		// TODO(dh): can we ever reach a piece with zero availability?
		// I don't think so; we only request from peers that we're
		// interested in, and we shouldn't be interested in peers that
		// have no pieces we don't also have.

		// OPT(dh): can we deprioritize pieces that we already have,
		// by giving them the smallest priority possible? That way
		// we'll have to iterate over much fewer total pieces. Or can
		// we just remove them altogether?
		if t.have.Get(piece) {
			// Don't pick a piece we already have
			continue
		}

		if !peer.have.Get(piece) {
			// Don't pick a piece the peer doesn't have
			continue
		}

		pranges, n := blocksForPiece(piece, numBlocks)
		ranges = append(ranges, pranges...)
		numBlocks -= n
	}

	return ranges, len(ranges) != 0
}

// XXX add method to mark blocks as still needed

func (t *Pieces) incAvailability(piece uint32) {
	avail := t.availabilities[piece]
	t.buckets[avail]--
	t.availabilities[piece]++

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.sortedPieces[other_index]

	t.sortedPieces[other_index], t.sortedPieces[index] = t.sortedPieces[index], t.sortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]

	if uint32(avail+1) >= uint32(len(t.buckets)) {
		t.buckets = append(t.buckets, other_index+1)
	}
}

func (t *Pieces) decAvailability(piece uint32) {
	log.Println("dec", piece)

	t.availabilities[piece]--
	avail := t.availabilities[piece]

	index := t.pieceIndices[piece]
	other_index := t.buckets[avail]
	other_piece := t.sortedPieces[other_index]
	t.buckets[avail]++

	log.Println(other_index)

	t.sortedPieces[other_index], t.sortedPieces[index] = t.sortedPieces[index], t.sortedPieces[other_index]
	t.pieceIndices[other_piece], t.pieceIndices[piece] = t.pieceIndices[piece], t.pieceIndices[other_piece]
}

type Action interface {
	Run() (result interface{}, stopped bool, err error)
	Pause()
	Stop()
}

type BaseAction struct {
	Torrent   *Torrent
	PauseCh   chan struct{}
	StopCh    chan struct{}
	UnpauseCh chan struct{}
}

func (l *BaseAction) Pause()   { channel.TrySend(l.PauseCh, struct{}{}) }
func (l *BaseAction) Unpause() { channel.TrySend(l.UnpauseCh, struct{}{}) }
func (l *BaseAction) Stop()    { channel.TrySend(l.StopCh, struct{}{}) }

type Verify struct {
	BaseAction
}

func (l *Verify) Run() (result interface{}, stopped bool, err error) {
	log.Println("started verifying", l.Torrent)
	defer func() {
		if stopped {
			log.Println("stopped verifying", l.Torrent)
		} else {
			log.Println("finished verifying", l.Torrent)
		}
	}()

	// OPT use more than one goroutine to verify in parallel; we get about 1 GB/s on one core
	pieceSize := l.Torrent.Metainfo.Info.PieceLength
	numPieces := l.Torrent.NumPieces()

	res := Bitset{}

	// OPT use a normal MultiReader to avoid the unnecessary cost of
	// mapping from piece to file. We just want to read everything
	// sequentially.

	buf := make([]byte, pieceSize)
	piece := uint32(0)

	for {
		select {
		case <-l.PauseCh:
			<-l.UnpauseCh
		case <-l.StopCh:
			return res, true, nil
		default:
		}

		n, err := l.Torrent.data.ReadAt(buf, int64(piece)*int64(pieceSize))
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				if piece+1 != uint32(numPieces) {
					break
				} else {
					buf = buf[:n]
				}
			} else if err == io.EOF {
				break
			} else {
				// XXX report the error
				return res, false, err
			}
		}

		off := piece * 20
		if verifyBlock(buf, l.Torrent.Metainfo.Info.Pieces[off:off+20]) {
			res.Set(piece)
		}

		piece++
	}

	return res, false, nil
}

func NewVerify(torr *Torrent) Action {
	return &Verify{
		BaseAction: BaseAction{
			Torrent:   torr,
			PauseCh:   make(chan struct{}, 1),
			StopCh:    make(chan struct{}, 1),
			UnpauseCh: make(chan struct{}, 1),
		},
	}
}

// TODO(dh): add necessary interfaces so that we don't depend on the file system. something that can open paths and return io.ReaderAts.

// OPT cache open files
type dataStorage struct {
	files  []dataStorageFile
	length int64
}

type dataStorageFile struct {
	Path   string
	Offset int64
	Size   int64
}

func (ds *dataStorage) add(path string, length int64) {
	ds.files = append(ds.files, dataStorageFile{path, ds.length, length})
	ds.length += length
}

func (ds *dataStorage) relevantFiles(n int, off int64) []dataStorageFile {
	// Skip past the requested offset.
	skipFiles := sort.Search(len(ds.files), func(i int) bool {
		// This function returns whether Files[i] will
		// contribute any bytes to our output.
		file := ds.files[i]
		return file.Offset+file.Size > off
	})
	return ds.files[skipFiles:]
}

func (ds *dataStorage) ReadAt(p []byte, off int64) (int, error) {
	wantN := len(p)

	files := ds.relevantFiles(len(p), off)
	// How far to skip in the first file.
	needSkip := off
	if len(files) > 0 {
		needSkip -= files[0].Offset
	}

	n := 0
	for len(files) > 0 && len(p) > 0 {
		readP := p
		fileSize := files[0].Size
		if int64(len(readP)) > fileSize-needSkip {
			readP = readP[:fileSize-needSkip]
		}

		f, err := os.Open(files[0].Path)
		if err == nil {
			rn, err := f.ReadAt(readP, needSkip)
			f.Close()
			if err != nil {
				if err == io.EOF {
					// file is shorter than it should be. pad with zeroes
					for i := range readP[rn:] {
						readP[i+rn] = 0
					}
				} else {
					return n, err
				}
			}
		} else {
			// if the file is missing we pad with zeros
			for i := range readP {
				readP[i] = 0
			}
		}
		// The return value of ReadAt doesn't matter. Either it read exactly len(readP) bytes, or it returned a non-nil error and we padded with zeroes.
		pn := len(readP)
		n += pn
		p = p[pn:]
		if int64(pn)+needSkip == fileSize {
			files = files[1:]
		}
		needSkip = 0
	}

	if n != wantN {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func (ds *dataStorage) WriteAt(p []byte, off int64) (int, error) {
	files := ds.relevantFiles(len(p), off)
	// How far to skip in the first file.
	needSkip := off
	if len(files) > 0 {
		needSkip -= files[0].Offset
	}

	n := 0
	for len(files) > 0 && len(p) > 0 {
		writeP := p
		fileSize := files[0].Size
		if int64(len(writeP)) > fileSize-needSkip {
			writeP = writeP[:fileSize-needSkip]
		}

		// OPT(dh): support preallocating files
		f, err := os.OpenFile(files[0].Path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return n, err
		}
		rn, err := f.WriteAt(writeP, needSkip)
		n += rn
		f.Close()
		if err != nil {
			return n, err
		}
		pn := len(writeP)
		p = p[pn:]
		if int64(pn)+needSkip == fileSize {
			files = files[1:]
		}
		needSkip = 0
	}

	return n, nil
}
