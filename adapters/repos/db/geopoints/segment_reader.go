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

	"github.com/weaviate/sroar"
)

// InnerReader answers a circle query from one layer of a bucket, a memtable or
// a segment: matches go to out, the layer's deletions to deleted, and the
// bucket subtracts the second from the first once all layers are in. A doc ID
// is added once and deleted at most once, so the order of the layers does not
// matter.
type InnerReader interface {
	WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error
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

func (r *SegmentReader) WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error {
	if r.hdr.deletionsLen > 0 {
		del, err := r.Deletions()
		if err != nil {
			return err
		}
		deleted.Or(del)
	}
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

		if !c.IntersectsBox(n.Box) {
			continue
		}
		if n.Kind == nodeKindInner {
			stack = append(stack, n.A, n.B)
			continue
		}
		if err := r.collectLeaf(c, n, out); err != nil {
			return err
		}
	}
	return nil
}

func (r *SegmentReader) collectLeaf(c *Circle, n node, out *sroar.Bitmap) error {
	start, count := uint64(n.A), uint64(n.B)
	if start+count > r.hdr.pointCount {
		return fmt.Errorf("geopoints: leaf [%d, %d) past %d points", start, start+count, r.hdr.pointCount)
	}
	base := r.hdr.pointsOffset()
	ids, err := r.src.read(base+start*8, count*8)
	if err != nil {
		return err
	}

	if c.ContainsBox(n.Box) {
		for i := uint64(0); i < count; i++ {
			out.Set(binary.LittleEndian.Uint64(ids[i*8:]))
		}
		return nil
	}

	lats, err := r.src.read(base+r.hdr.pointCount*8+start*4, count*4)
	if err != nil {
		return err
	}
	lons, err := r.src.read(base+r.hdr.pointCount*12+start*4, count*4)
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		lat := math.Float32frombits(binary.LittleEndian.Uint32(lats[i*4:]))
		lon := math.Float32frombits(binary.LittleEndian.Uint32(lons[i*4:]))
		if c.Contains(lat, lon) {
			out.Set(binary.LittleEndian.Uint64(ids[i*8:]))
		}
	}
	return nil
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
