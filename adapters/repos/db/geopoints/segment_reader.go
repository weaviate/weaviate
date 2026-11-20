//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package geopoints

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"sync"

	"github.com/weaviate/sroar"
)

// InnerReader answers a query from one layer of a bucket, a memtable or a
// segment. For the two filters, matches go to out, the layer's deletions to
// deleted, and the bucket subtracts the second from the first once all layers
// are in. A doc ID is added once and deleted at most once, so the order of the
// layers does not matter. A nearest search needs the deletions of every layer
// before it reads a point, so it takes them separately.
type InnerReader interface {
	WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error
	WithinBox(ctx context.Context, q *BoxQuery, out, deleted *sroar.Bitmap) error
	CollectDeletions(deleted *sroar.Bitmap) error
	Nearest(ctx context.Context, n *Nearest) error
}

// source reads a byte range of a segment body: a slice of the mapping, or a
// fresh buffer with pread.
type source interface {
	read(off, n uint64) ([]byte, error)
	size() uint64
}

type memSource []byte

func (s memSource) read(off, n uint64) ([]byte, error) {
	if off+n > uint64(len(s)) || off+n < off {
		return nil, fmt.Errorf("geopoints: read [%d, %d) past the body of %d bytes", off, off+n, len(s))
	}
	return s[off : off+n], nil
}

func (s memSource) size() uint64 { return uint64(len(s)) }

type preadSource struct {
	ra   io.ReaderAt
	base int64
	n    uint64
}

func (s *preadSource) read(off, n uint64) ([]byte, error) {
	if off+n > s.n || off+n < off {
		return nil, fmt.Errorf("geopoints: read [%d, %d) past the body of %d bytes", off, off+n, s.n)
	}
	buf := make([]byte, n)
	if _, err := s.ra.ReadAt(buf, s.base+int64(off)); err != nil && (err != io.EOF || n != 0) {
		return nil, fmt.Errorf("geopoints: pread %d bytes at %d: %w", n, off, err)
	}
	return buf, nil
}

func (s *preadSource) size() uint64 { return s.n }

type SegmentReader struct {
	src source
	hdr bodyHeader
}

func NewSegmentReader(body []byte) (*SegmentReader, error) {
	return newSegmentReader(memSource(body))
}

func NewSegmentReaderPread(ra io.ReaderAt, offset, size int64) (*SegmentReader, error) {
	return newSegmentReader(&preadSource{ra: ra, base: offset, n: uint64(size)})
}

func newSegmentReader(src source) (*SegmentReader, error) {
	raw, err := src.read(0, bodyHeaderSize)
	if err != nil {
		return nil, err
	}
	hdr, err := parseBodyHeader(raw)
	if err != nil {
		return nil, err
	}
	if hdr.size() != src.size() {
		return nil, fmt.Errorf("geopoints body declares %d bytes but holds %d", hdr.size(), src.size())
	}
	if hdr.pointCount > math.MaxUint32 {
		return nil, fmt.Errorf("geopoints body declares %d points, more than a leaf can address", hdr.pointCount)
	}
	return &SegmentReader{src: src, hdr: hdr}, nil
}

func (r *SegmentReader) PointCount() uint64 {
	return r.hdr.pointCount
}

// Deletions are the doc IDs this segment deletes from older ones. The bitmap
// may alias the mapping, so it is read-only.
func (r *SegmentReader) Deletions() (*sroar.Bitmap, error) {
	if r.hdr.deletionsLen == 0 {
		return sroar.NewBitmap(), nil
	}
	buf, err := r.src.read(bodyHeaderSize, r.hdr.deletionsLen)
	if err != nil {
		return nil, err
	}
	return sroar.FromBuffer(buf), nil
}

// CollectDeletions adds the doc IDs this segment deletes to deleted.
func (r *SegmentReader) CollectDeletions(deleted *sroar.Bitmap) error {
	if r.hdr.deletionsLen == 0 {
		return nil
	}
	del, err := r.Deletions()
	if err != nil {
		return err
	}
	deleted.Or(del)
	return nil
}

func (r *SegmentReader) WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error {
	if err := r.CollectDeletions(deleted); err != nil {
		return err
	}
	matches := collectorPool.Get().(*collector)
	defer matches.release()
	err := r.descend(ctx,
		func(b Box) bool { return !c.IntersectsBox(b) || c.rejectsBox(b) },
		nil,
		func(n node) error { return r.collectLeaf(c, n, matches) })
	if err != nil {
		return err
	}
	matches.flush(out)
	return nil
}

func (r *SegmentReader) WithinBox(ctx context.Context, q *BoxQuery, out, deleted *sroar.Bitmap) error {
	if err := r.CollectDeletions(deleted); err != nil {
		return err
	}
	matches := collectorPool.Get().(*collector)
	defer matches.release()
	err := r.descend(ctx,
		func(b Box) bool { return !q.IntersectsBox(b) },
		nil,
		func(n node) error { return r.collectLeafInBox(q, n, matches) })
	if err != nil {
		return err
	}
	matches.flush(out)
	return nil
}

// Nearest offers n the points of this segment that can still enter its result,
// descending into the nearer child first so the bound tightens early.
func (r *SegmentReader) Nearest(ctx context.Context, n *Nearest) error {
	return r.descend(ctx, n.prunes, n.approach, func(leaf node) error {
		ids, err := r.leafDocIDs(leaf)
		if err != nil {
			return err
		}
		lats, lons, err := r.leafCoordinates(leaf)
		if err != nil {
			return err
		}
		for i := uint64(0); i < uint64(leaf.B); i++ {
			n.offer(binary.LittleEndian.Uint64(ids[i*8:]),
				math.Float32frombits(binary.LittleEndian.Uint32(lats[i*4:])),
				math.Float32frombits(binary.LittleEndian.Uint32(lons[i*4:])))
		}
		return nil
	})
}

// descend walks the tree depth-first: prune drops a subtree by its box, leaf
// handles each leaf that was not dropped. With order, the child whose box it
// ranks lower is visited first; without, the children are visited as stored.
func (r *SegmentReader) descend(ctx context.Context, prune func(Box) bool,
	order func(Box) float32, leaf func(node) error,
) error {
	if r.hdr.nodeCount == 0 {
		return nil
	}

	// nodes are read in 4 KiB blocks as the descent reaches them: the layout is
	// depth-first, so a narrow query touches a few blocks, not the whole table
	const nodesPerBlock = 4096 / nodeSize
	var block []byte
	blockStart := uint32(math.MaxUint32)
	nodeAt := func(i uint32) (node, error) {
		if i >= r.hdr.nodeCount {
			return node{}, fmt.Errorf("geopoints: node %d out of %d", i, r.hdr.nodeCount)
		}
		if start := i - i%nodesPerBlock; start != blockStart {
			count := min(uint32(nodesPerBlock), r.hdr.nodeCount-start)
			b, err := r.src.read(r.hdr.nodesOffset()+uint64(start)*nodeSize, uint64(count)*nodeSize)
			if err != nil {
				return node{}, err
			}
			block, blockStart = b, start
		}
		return unmarshalNode(block[(i-blockStart)*nodeSize:]), nil
	}

	stack := make([]uint32, 0, 64)
	stack = append(stack, 0)
	visited := uint32(0)
	for len(stack) > 0 {
		// a corrupt tree whose children point back up must not loop
		visited++
		if visited > r.hdr.nodeCount {
			return fmt.Errorf("geopoints: tree of %d nodes does not terminate", r.hdr.nodeCount)
		}
		if visited%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		n, err := nodeAt(stack[len(stack)-1])
		if err != nil {
			return err
		}
		stack = stack[:len(stack)-1]

		if prune(n.Box) {
			continue
		}
		if n.Kind != nodeKindInner {
			if err := leaf(n); err != nil {
				return err
			}
			continue
		}
		first, second := n.B, n.A // the stack pops B first, as stored
		if order != nil {
			a, err := nodeAt(n.A)
			if err != nil {
				return err
			}
			b, err := nodeAt(n.B)
			if err != nil {
				return err
			}
			if order(a.Box) < order(b.Box) {
				first, second = n.A, n.B
			}
		}
		stack = append(stack, second, first)
	}
	return nil
}

// leafDocIDs reads the leaf's doc IDs, 8 bytes each.
func (r *SegmentReader) leafDocIDs(n node) ([]byte, error) {
	start, count := uint64(n.A), uint64(n.B)
	if start+count > r.hdr.pointCount {
		return nil, fmt.Errorf("geopoints: leaf [%d, %d) past %d points", start, start+count, r.hdr.pointCount)
	}
	return r.src.read(r.hdr.pointsOffset()+start*8, count*8)
}

// leafCoordinates reads the leaf's latitudes and longitudes, 4 bytes each. The
// caller has checked the leaf's range with leafDocIDs.
func (r *SegmentReader) leafCoordinates(n node) (lats, lons []byte, err error) {
	start, count := uint64(n.A), uint64(n.B)
	base := r.hdr.pointsOffset()
	lats, err = r.src.read(base+r.hdr.pointCount*8+start*4, count*4)
	if err != nil {
		return nil, nil, err
	}
	lons, err = r.src.read(base+r.hdr.pointCount*12+start*4, count*4)
	if err != nil {
		return nil, nil, err
	}
	return lats, lons, nil
}

// collectLeaf adds the leaf's matches to the collector. A leaf's doc IDs are
// sorted, so they form one sorted run.
func (r *SegmentReader) collectLeaf(c *Circle, n node, matches *collector) error {
	count := uint64(n.B)
	ids, err := r.leafDocIDs(n)
	if err != nil {
		return err
	}

	if c.ContainsBox(n.Box) {
		for i := uint64(0); i < count; i++ {
			matches.add(binary.LittleEndian.Uint64(ids[i*8:]))
		}
	} else {
		lats, lons, err := r.leafCoordinates(n)
		if err != nil {
			return err
		}
		for i := uint64(0); i < count; i++ {
			lat := math.Float32frombits(binary.LittleEndian.Uint32(lats[i*4:]))
			lon := math.Float32frombits(binary.LittleEndian.Uint32(lons[i*4:]))
			if c.containsFast(lat, lon) {
				matches.add(binary.LittleEndian.Uint64(ids[i*8:]))
			}
		}
	}
	matches.endRun()
	return nil
}

// collectLeafInBox is collectLeaf for a box query.
func (r *SegmentReader) collectLeafInBox(q *BoxQuery, n node, matches *collector) error {
	count := uint64(n.B)
	ids, err := r.leafDocIDs(n)
	if err != nil {
		return err
	}

	if q.ContainsBox(n.Box) {
		for i := uint64(0); i < count; i++ {
			matches.add(binary.LittleEndian.Uint64(ids[i*8:]))
		}
	} else {
		lats, lons, err := r.leafCoordinates(n)
		if err != nil {
			return err
		}
		for i := uint64(0); i < count; i++ {
			lat := math.Float32frombits(binary.LittleEndian.Uint32(lats[i*4:]))
			lon := math.Float32frombits(binary.LittleEndian.Uint32(lons[i*4:]))
			if q.Contains(lat, lon) {
				matches.add(binary.LittleEndian.Uint64(ids[i*8:]))
			}
		}
	}
	matches.endRun()
	return nil
}

// collector gathers a layer's matches as sorted runs, one per leaf, and turns
// them into a bitmap once. A few runs are or'ed in one by one; many are set in
// a dense bitset over the doc ID range first, since or'ing a run into an
// already dense bitmap walks the whole bitmap every time.
type collector struct {
	ids      []uint64
	runEnds  []int
	dense    []uint64
	min, max uint64
}

// collectorPool keeps the buffers of recent queries: a broad one fills a
// megabyte or more, which would otherwise be garbage on every query.
var collectorPool = sync.Pool{New: func() any { return &collector{} }}

func (c *collector) release() {
	const keep = 1 << 20 // entries; larger buffers are left to the garbage collector
	if cap(c.ids) > keep || cap(c.dense) > keep {
		return
	}
	c.ids, c.runEnds, c.dense = c.ids[:0], c.runEnds[:0], c.dense[:0]
	collectorPool.Put(c)
}

func (c *collector) add(id uint64) {
	if len(c.ids) == 0 {
		c.min, c.max = id, id
	} else {
		c.min, c.max = min(c.min, id), max(c.max, id)
	}
	c.ids = append(c.ids, id)
}

func (c *collector) endRun() {
	if n := len(c.ids); len(c.runEnds) == 0 || c.runEnds[len(c.runEnds)-1] < n {
		c.runEnds = append(c.runEnds, n)
	}
}

func (c *collector) flush(out *sroar.Bitmap) {
	if len(c.ids) == 0 {
		return
	}
	const fewRuns = 16
	if len(c.runEnds) <= fewRuns {
		start := 0
		for _, end := range c.runEnds {
			addSorted(out, c.ids[start:end])
			start = end
		}
		return
	}

	// a dense bitset, unless the doc IDs are so spread out that it would dwarf
	// the matches; 8 words per match is 64 bytes against their 8
	words := (c.max-c.min)/64 + 1
	if words > uint64(len(c.ids))*8 {
		slices.Sort(c.ids)
		out.Or(sroar.FromSortedList(c.ids))
		return
	}
	if uint64(cap(c.dense)) < words {
		c.dense = make([]uint64, words)
	}
	dense := c.dense[:words]
	clear(dense)
	for _, id := range c.ids {
		d := id - c.min
		dense[d>>6] |= 1 << (d & 63)
	}
	// the bitset enumerates in order, back into the slice it was built from
	sorted := c.ids[:0]
	for w, word := range dense {
		for word != 0 {
			sorted = append(sorted, c.min+uint64(w)*64+uint64(bits.TrailingZeros64(word)))
			word &= word - 1
		}
	}
	out.Or(sroar.FromSortedList(sorted))
}

// addSorted ors ids into out as a sorted run, sorting first if they are not.
func addSorted(out *sroar.Bitmap, ids []uint64) {
	if len(ids) == 0 {
		return
	}
	if !slices.IsSorted(ids) {
		slices.Sort(ids)
	}
	out.Or(sroar.FromSortedList(ids))
}

// pointChunk bounds the scratch a pread source allocates per read to a megabyte.
const pointChunk = 1 << 16

// appendPoints appends the segment's points to dst, leaving out those drop
// reports; drop may be nil.
func (r *SegmentReader) appendPoints(dst []Point, drop func(docID uint64) bool) ([]Point, error) {
	n := r.hdr.pointCount
	base := r.hdr.pointsOffset()
	for start := uint64(0); start < n; start += pointChunk {
		count := min(uint64(pointChunk), n-start)
		ids, err := r.src.read(base+start*8, count*8)
		if err != nil {
			return dst, err
		}
		lats, err := r.src.read(base+n*8+start*4, count*4)
		if err != nil {
			return dst, err
		}
		lons, err := r.src.read(base+n*12+start*4, count*4)
		if err != nil {
			return dst, err
		}
		for i := uint64(0); i < count; i++ {
			id := binary.LittleEndian.Uint64(ids[i*8:])
			if drop != nil && drop(id) {
				continue
			}
			dst = append(dst, Point{
				DocID: id,
				Lat:   math.Float32frombits(binary.LittleEndian.Uint32(lats[i*4:])),
				Lon:   math.Float32frombits(binary.LittleEndian.Uint32(lons[i*4:])),
			})
		}
	}
	return dst, nil
}

func (r *SegmentReader) Points() ([]Point, error) {
	return r.appendPoints(make([]Point, 0, r.hdr.pointCount), nil)
}
