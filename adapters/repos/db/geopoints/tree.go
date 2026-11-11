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
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"
)

// MaxPointsPerLeaf is Lucene's BKD default: a leaf is read whole, so it trades
// tree depth against wasted distance checks on the boundary.
const MaxPointsPerLeaf = 512

// Segment body layout, little-endian. It follows the lsmkv segment header and
// is the whole data section: the index section is empty, like the
// roaringsetrange strategy's, so the generic segment loader never parses it.
//
//	u16 version        (bodyVersion)
//	u16 reserved
//	u32 node count
//	u64 point count
//	u64 deletions length
//	deletions          serialized sroar bitmap
//	nodes              node count × nodeSize
//	doc IDs            point count × u64
//	latitudes          point count × f32
//	longitudes         point count × f32
//
// Points are stored by leaf, so a leaf is a contiguous range of all three
// columns and the tree needs only (start, count) per leaf.
const (
	bodyVersion    = uint16(1)
	bodyHeaderSize = 2 + 2 + 4 + 8 + 8
	nodeSize       = 32

	nodeKindInner = uint32(0)
	nodeKindLeaf  = uint32(1)
)

// node: an inner node's A and B are its children, a leaf's its first point
// and its point count.
type node struct {
	Box  Box
	A, B uint32
	Kind uint32
}

func (n *node) marshal(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], math.Float32bits(n.Box.MinLat))
	binary.LittleEndian.PutUint32(dst[4:], math.Float32bits(n.Box.MaxLat))
	binary.LittleEndian.PutUint32(dst[8:], math.Float32bits(n.Box.MinLon))
	binary.LittleEndian.PutUint32(dst[12:], math.Float32bits(n.Box.MaxLon))
	binary.LittleEndian.PutUint32(dst[16:], n.A)
	binary.LittleEndian.PutUint32(dst[20:], n.B)
	binary.LittleEndian.PutUint32(dst[24:], n.Kind)
	binary.LittleEndian.PutUint32(dst[28:], 0)
}

func unmarshalNode(src []byte) node {
	return node{
		Box: Box{
			MinLat: math.Float32frombits(binary.LittleEndian.Uint32(src[0:])),
			MaxLat: math.Float32frombits(binary.LittleEndian.Uint32(src[4:])),
			MinLon: math.Float32frombits(binary.LittleEndian.Uint32(src[8:])),
			MaxLon: math.Float32frombits(binary.LittleEndian.Uint32(src[12:])),
		},
		A:    binary.LittleEndian.Uint32(src[16:]),
		B:    binary.LittleEndian.Uint32(src[20:]),
		Kind: binary.LittleEndian.Uint32(src[24:]),
	}
}

func BodySize(n int, deletionsLen int) int {
	return bodyHeaderSize + deletionsLen + nodeCount(n)*nodeSize + n*16
}

// nodeCount: the split is always at the middle, so every split adds two nodes.
func nodeCount(n int) int {
	if n == 0 {
		return 0
	}
	return 2*leafCount(n) - 1
}

func leafCount(n int) int {
	if n <= MaxPointsPerLeaf {
		return 1
	}
	mid := n / 2
	return leafCount(mid) + leafCount(n-mid)
}

// Body is a segment's content before serialization. WriteTo lays the tree out
// in the format above, reordering the points in place, so neither a flush nor
// a compaction holds a second copy of them.
type Body struct {
	Points    []Point
	Deletions []byte
}

func (b *Body) Size() int {
	return BodySize(len(b.Points), len(b.Deletions))
}

func (b *Body) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, b.Size()))
	if _, err := b.WriteTo(context.Background(), buf); err != nil {
		panic(err) // a bytes.Buffer does not fail
	}
	return buf.Bytes()
}

// WriteTo writes exactly Size bytes, checking ctx along the way.
func (b *Body) WriteTo(ctx context.Context, w io.Writer) (int64, error) {
	cw := &countingWriter{w: w}
	err := b.write(ctx, cw)
	return cw.n, err
}

// Build is the body over points, reordered in place, and deletions (may be nil).
func Build(points []Point, deletions []byte) []byte {
	return (&Body{Points: points, Deletions: deletions}).Bytes()
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

const scratchSize = 64 << 10

func (b *Body) write(ctx context.Context, w io.Writer) error {
	var hdr [bodyHeaderSize]byte
	binary.LittleEndian.PutUint16(hdr[0:], bodyVersion)
	binary.LittleEndian.PutUint16(hdr[2:], 0)
	binary.LittleEndian.PutUint64(hdr[8:], uint64(len(b.Points)))
	binary.LittleEndian.PutUint64(hdr[16:], uint64(len(b.Deletions)))

	var nodes []node
	if len(b.Points) > 0 {
		bd := builder{ctx: ctx, points: b.Points, nodes: make([]node, 0, nodeCount(len(b.Points)))}
		bd.split(0, len(b.Points))
		if bd.err != nil {
			return bd.err
		}
		nodes = bd.nodes
	}
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(nodes)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(b.Deletions); err != nil {
		return err
	}

	scratch := make([]byte, scratchSize)
	for lo := 0; lo < len(nodes); lo += scratchSize / nodeSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		hi := min(lo+scratchSize/nodeSize, len(nodes))
		for i := lo; i < hi; i++ {
			nodes[i].marshal(scratch[(i-lo)*nodeSize:])
		}
		if _, err := w.Write(scratch[:(hi-lo)*nodeSize]); err != nil {
			return err
		}
	}

	points := b.Points
	for lo := 0; lo < len(points); lo += scratchSize / 8 {
		if err := ctx.Err(); err != nil {
			return err
		}
		hi := min(lo+scratchSize/8, len(points))
		for i := lo; i < hi; i++ {
			binary.LittleEndian.PutUint64(scratch[(i-lo)*8:], points[i].DocID)
		}
		if _, err := w.Write(scratch[:(hi-lo)*8]); err != nil {
			return err
		}
	}
	for lo := 0; lo < len(points); lo += scratchSize / 4 {
		hi := min(lo+scratchSize/4, len(points))
		for i := lo; i < hi; i++ {
			binary.LittleEndian.PutUint32(scratch[(i-lo)*4:], math.Float32bits(points[i].Lat))
		}
		if _, err := w.Write(scratch[:(hi-lo)*4]); err != nil {
			return err
		}
	}
	for lo := 0; lo < len(points); lo += scratchSize / 4 {
		hi := min(lo+scratchSize/4, len(points))
		for i := lo; i < hi; i++ {
			binary.LittleEndian.PutUint32(scratch[(i-lo)*4:], math.Float32bits(points[i].Lon))
		}
		if _, err := w.Write(scratch[:(hi-lo)*4]); err != nil {
			return err
		}
	}
	return nil
}

type builder struct {
	ctx    context.Context
	points []Point
	nodes  []node
	err    error
}

// split builds the subtree over points[lo:hi] and returns its node index. It
// splits the wider dimension, longitude scaled by the cosine of the mid
// latitude so leaves stay square-ish on the ground.
func (b *builder) split(lo, hi int) uint32 {
	idx := uint32(len(b.nodes))
	if b.err != nil {
		return idx
	}
	b.nodes = append(b.nodes, node{Box: boundingBox(b.points[lo:hi])})

	if hi-lo <= MaxPointsPerLeaf {
		// a leaf's doc IDs are stored sorted so a query can build its result
		// from sorted runs instead of setting bits one at a time
		slices.SortFunc(b.points[lo:hi], func(a, b Point) int { return cmp.Compare(a.DocID, b.DocID) })
		b.nodes[idx].Kind = nodeKindLeaf
		b.nodes[idx].A = uint32(lo)
		b.nodes[idx].B = uint32(hi - lo)
		if idx%64 == 0 {
			b.err = b.ctx.Err()
		}
		return idx
	}

	box := b.nodes[idx].Box
	midLat := float64(box.MinLat+box.MaxLat) / 2
	lonExtent := float64(box.MaxLon-box.MinLon) * math.Abs(math.Cos(midLat*math.Pi/180))
	byLat := float64(box.MaxLat-box.MinLat) >= lonExtent

	// a median partition instead of a sort per level: n log n, not n log² n
	mid := lo + (hi-lo)/2
	selectNth(b.points[lo:hi], mid-lo, byLat)
	left := b.split(lo, mid)
	right := b.split(mid, hi)
	b.nodes[idx].Kind = nodeKindInner
	b.nodes[idx].A = left
	b.nodes[idx].B = right
	return idx
}

// lessPoint orders by the split coordinate, then doc ID for determinism.
func lessPoint(a, b *Point, byLat bool) bool {
	var ca, cb float32
	if byLat {
		ca, cb = a.Lat, b.Lat
	} else {
		ca, cb = a.Lon, b.Lon
	}
	if ca != cb {
		return ca < cb
	}
	return a.DocID < b.DocID
}

// selectNth puts the element a sort would place at k there, with everything
// before it ordering no later and everything after it no earlier. Quickselect,
// median-of-three pivot; the doc ID tie-break keeps duplicates from degrading it.
func selectNth(points []Point, k int, byLat bool) {
	lo, hi := 0, len(points)-1
	for hi > lo {
		if hi-lo < 16 {
			insertionSort(points[lo:hi+1], byLat)
			return
		}
		mid := lo + (hi-lo)/2
		// median of three into points[mid], then move it out of the way
		if lessPoint(&points[mid], &points[lo], byLat) {
			points[mid], points[lo] = points[lo], points[mid]
		}
		if lessPoint(&points[hi], &points[lo], byLat) {
			points[hi], points[lo] = points[lo], points[hi]
		}
		if lessPoint(&points[hi], &points[mid], byLat) {
			points[hi], points[mid] = points[mid], points[hi]
		}
		pivot := points[mid]
		points[mid], points[hi-1] = points[hi-1], points[mid]

		// Hoare partition around the pivot now sitting at hi-1
		i, j := lo, hi-1
		for {
			for i++; lessPoint(&points[i], &pivot, byLat); i++ {
			}
			for j--; lessPoint(&pivot, &points[j], byLat); j-- {
			}
			if i >= j {
				break
			}
			points[i], points[j] = points[j], points[i]
		}
		points[i], points[hi-1] = points[hi-1], points[i]

		switch {
		case k == i:
			return
		case k < i:
			hi = i - 1
		default:
			lo = i + 1
		}
	}
}

func insertionSort(points []Point, byLat bool) {
	for i := 1; i < len(points); i++ {
		for j := i; j > 0 && lessPoint(&points[j], &points[j-1], byLat); j-- {
			points[j], points[j-1] = points[j-1], points[j]
		}
	}
}

func boundingBox(points []Point) Box {
	box := Box{MinLat: math.MaxFloat32, MaxLat: -math.MaxFloat32, MinLon: math.MaxFloat32, MaxLon: -math.MaxFloat32}
	for i := range points {
		box.MinLat = min(box.MinLat, points[i].Lat)
		box.MaxLat = max(box.MaxLat, points[i].Lat)
		box.MinLon = min(box.MinLon, points[i].Lon)
		box.MaxLon = max(box.MaxLon, points[i].Lon)
	}
	return box
}

type bodyHeader struct {
	nodeCount    uint32
	pointCount   uint64
	deletionsLen uint64
}

func parseBodyHeader(src []byte) (bodyHeader, error) {
	if len(src) < bodyHeaderSize {
		return bodyHeader{}, fmt.Errorf("geopoints body of %d bytes is shorter than its %d byte header",
			len(src), bodyHeaderSize)
	}
	if v := binary.LittleEndian.Uint16(src[0:]); v != bodyVersion {
		return bodyHeader{}, fmt.Errorf("geopoints body version %d, want %d", v, bodyVersion)
	}
	return bodyHeader{
		nodeCount:    binary.LittleEndian.Uint32(src[4:]),
		pointCount:   binary.LittleEndian.Uint64(src[8:]),
		deletionsLen: binary.LittleEndian.Uint64(src[16:]),
	}, nil
}

func (h bodyHeader) nodesOffset() uint64 {
	return bodyHeaderSize + h.deletionsLen
}

func (h bodyHeader) pointsOffset() uint64 {
	return h.nodesOffset() + uint64(h.nodeCount)*nodeSize
}

func (h bodyHeader) size() uint64 {
	return h.pointsOffset() + h.pointCount*16
}
