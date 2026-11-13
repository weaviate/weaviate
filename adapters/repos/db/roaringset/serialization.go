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

package roaringset

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// SegmentNode was replaced by SegmentNodeList for WAL, but it is still used in
// the bitmap layers.
//
// SegmentNode stores one Key-Value pair (without its index) in
// the LSM Segment.  It uses a single []byte internally. As a result there is
// no decode step required at runtime. Instead you can use
//
//   - [*SegmentNode.Additions]
//   - [*SegmentNode.AdditionsWithCopy]
//   - [*SegmentNode.Deletions]
//   - [*SegmentNode.DeletionsWithCopy]
//   - [*SegmentNode.PrimaryKey]
//
// to access the contents. Those helpers in turn do not require a decoding
// step. The accessor methods that return Roaring Bitmaps only point to
// existing memory (methods without WithCopy suffix), or in the worst case copy
// one byte slice (methods with WithCopy suffix).
//
// This makes the SegmentNode very fast to access at query time, even when it
// contains a large amount of data.
//
// The internal structure of the data is:
//
//	byte begin-start    | description
//	--------------------|-----------------------------------------------------
//	0-8                 | uint64 indicating the total length of the node,
//	                    | this is used in cursors to identify the next node.
//	8-16                | uint64 length indicator for additions sraor bm -> x
//	16-(x+16)           | additions bitmap
//	(x+16)-(x+24)       | uint64 length indicator for deletions sroar bm -> y
//	(x+24)-(x+y+24)     | deletions bitmap
//	(x+y+24)-(x+y+28)   | uint32 length indicator for primary key length -> z
//	(x+y+28)-(x+y+z+28) | primary key
type SegmentNode struct {
	data []byte
}

// Len indicates the total length of the [SegmentNode]. When reading multiple
// segments back-2-back, such as in a cursor situation, the offset of element
// (n+1) is the offset of element n + Len()
func (sn *SegmentNode) Len() uint64 {
	return binary.LittleEndian.Uint64(sn.data[0:8])
}

// Additions returns the additions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.AdditionsWithCopy].
//
// It returns nil when the node holds no additions (length indicator 0);
// callers needing a non-nil bitmap must substitute an empty one.
func (sn *SegmentNode) Additions() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	buf := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	if len(buf) == 0 {
		return nil
	}
	return sroar.FromBuffer(buf)
}

// AdditionsCloneToBuf clones the node's additions bitmap straight into a
// buffer taken from pool, skipping the intermediate bitmap over the node's
// memory. The clone is safe to use and mutate beyond the node's lifetime, up
// to the pooled buffer's capacity. It returns (nil, nil) when the node holds
// no additions — the release is non-nil exactly when the bitmap is.
func (sn *SegmentNode) AdditionsCloneToBuf(pool BitmapBufPool) (*sroar.Bitmap, func()) {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	buf := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	if len(buf) == 0 {
		return nil, nil
	}
	return pool.CloneBytesToBuf(buf)
}

// AdditionsWithCopy returns the additions roaring bitmap without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNode.Additions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNode.Additions].
func (sn *SegmentNode) AdditionsWithCopy() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	return sroar.FromBufferWithCopy(rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

// AdditionsOwningBuf returns the additions bitmap over the node's memory and
// takes over release, which frees that memory. The bitmap grows over the rest
// of the node's buffer before taking buffers from pool, so nothing may read sn
// after this call. With no additions it returns a nil bitmap and hands release
// back uncalled.
func (sn *SegmentNode) AdditionsOwningBuf(pool BitmapBufPool, release func()) (*sroar.Bitmap, func()) {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	buf := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	if len(buf) == 0 {
		return nil, release
	}
	requireEvenLength(buf)
	return fromBufferToBuf(pool, buf, release)
}

// Deletions returns the deletions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.DeletionsWithCopy].
//
// It returns nil when the node holds no deletions (length indicator 0);
// callers needing a non-nil bitmap must substitute an empty one.
func (sn *SegmentNode) Deletions() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	buf := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	if len(buf) == 0 {
		return nil
	}
	return sroar.FromBuffer(buf)
}

// DeletionsWithCopy returns the deletions roaring bitmap without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNode.Deletions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNode.Deletions].
func (sn *SegmentNode) DeletionsWithCopy() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return sroar.FromBufferWithCopy(rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

func (sn *SegmentNode) PrimaryKey() []byte {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return rw.ReadBytesFromBufferWithUint32LengthIndicator()
}

// NewSegmentNode builds a node into a fresh allocation, compacting nothing. Both
// segment writers build through [NewSegmentNodeCompacted]; this is the plain form
// its output is compared against.
func NewSegmentNode(
	key []byte, additions, deletions *sroar.Bitmap,
) (*SegmentNode, error) {
	if len(key) > math.MaxUint32 {
		return nil, fmt.Errorf("key too long: %d bytes, max is %d",
			len(key), math.MaxUint32)
	}

	additionsBuf := additions.ToBuffer()
	deletionsBuf := deletions.ToBuffer()

	// offset + 2*uint64 length indicators + uint32 length indicator + payloads
	expectedSize := 8 + 8 + 8 + 4 + len(additionsBuf) + len(deletionsBuf) + len(key)
	sn := SegmentNode{
		data: make([]byte, expectedSize),
	}

	rw := byteops.NewReadWriter(sn.data)

	rw.WriteUint64(uint64(expectedSize))
	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(additionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(deletionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(key); err != nil {
		return nil, err
	}

	return &sn, nil
}

// NewSegmentNodeCompacted builds the node into scratch, which grows as needed
// and is reused by the next call. Write the node through [SegmentNode.ToBuffer].
// The node points into scratch, so nothing may hold it past the next call.
// Neither bitmap may live in scratch, because sroar leaves an overlapping side
// undefined and nothing here checks for one. A nil bitmap is an empty side.
func NewSegmentNodeCompacted(
	key []byte, additions, deletions *sroar.Bitmap, scratch *SegmentNodeScratch,
) (SegmentNode, error) {
	// Ahead of Reserve, so no buffer is sized around a key length the node
	// cannot record.
	if len(key) > math.MaxUint32 {
		return SegmentNode{}, fmt.Errorf("key too long: %d bytes, max is %d",
			len(key), math.MaxUint32)
	}

	// offset + 2*uint64 length indicators + uint32 length indicator
	const overhead = 8 + 8 + 8 + 4
	scratch.Reserve(overhead + len(key) + additions.LenInBytes() + deletions.LenInBytes())
	aSize := scratch.CompactAt(additions, 16)
	dSize := scratch.CompactAt(deletions, 24+aSize)
	buf := scratch.Node(overhead + len(key) + aSize + dSize)

	// The payloads are already in place. Every remaining byte is written here,
	// because scratch still holds the previous node's bytes.
	rw := byteops.NewReadWriter(buf)
	rw.WriteUint64(uint64(len(buf)))
	rw.WriteUint64(uint64(aSize))
	rw.MoveBufferToAbsolutePosition(uint64(16 + aSize))
	rw.WriteUint64(uint64(dSize))
	rw.MoveBufferToAbsolutePosition(uint64(24 + aSize + dSize))
	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(key); err != nil {
		return SegmentNode{}, err
	}

	return SegmentNode{data: buf}, nil
}

// SegmentNodeScratch is the buffer NewSegmentNodeCompacted builds nodes in, kept
// by its caller across nodes. CompactAt compacts each payload straight into it,
// at the offset it was given.
type SegmentNodeScratch struct {
	buf []byte
	// next is where the region scratchPool.Get hands out next starts.
	next int
	// reserve is the node size the buffer grows to at once, if it must grow.
	reserve int
	// dst is the struct CompactedToBuf builds into, held so CompactedToBuf does not
	// allocate one per payload. CompactAt discards it once the bytes are in buf.
	dst sroar.Bitmap
}

// Reserve sizes the next growth for a node of about n bytes, so a node that
// outgrows the buffer reallocates it once rather than per payload.
func (s *SegmentNodeScratch) Reserve(n int) {
	s.reserve = n
}

// CompactAt compacts bm into the buffer at start and returns its size. An empty
// bm takes no room, where CompactedToBuf would still write an empty bitmap.
// start must not precede the end of the previous payload, which growth would drop.
func (s *SegmentNodeScratch) CompactAt(bm *sroar.Bitmap, start int) int {
	if bm.IsEmpty() {
		return 0
	}
	s.next = start
	bm.CompactedToBuf(&s.dst, scratchPool{s})
	s.dst = sroar.Bitmap{}
	return s.next - start
}

// Node returns the first n bytes of the buffer, holding the payloads put so far.
func (s *SegmentNodeScratch) Node(n int) []byte {
	s.grow(n)
	return s.buf
}

// scratchPool is the sroar.BufPool CompactAt hands CompactedToBuf. It is valid
// only within that call, so SegmentNodeScratch does not export it.
type scratchPool struct{ *SegmentNodeScratch }

// Get hands sroar the next region, capped so it does not adopt the rest of the
// buffer.
func (p scratchPool) Get(minBytes int) ([]byte, func()) {
	start, end := p.next, p.next+minBytes
	p.grow(end)
	p.next = end
	return p.buf[start:end:end], nil
}

// grow sets the buffer's length to n, keeping what it holds.
func (s *SegmentNodeScratch) grow(n int) {
	if n > cap(s.buf) {
		buf := make([]byte, n, max(n, s.reserve))
		copy(buf, s.buf)
		s.buf = buf
	}
	s.buf = s.buf[:n]
}

// ToBuffer returns the internal buffer without copying data. Only use this,
// when you can be sure that it's safe to share the data, or create your own
// copy.
//
// It truncates the buffer at is own length, in case it was initialized with a
// long buffer that only had a beginning offset, but no end. Such a situation
// may occur with cursors. If we then returned the whole buffer and don't know
// what the caller plans on doing with the data, we risk passing around too
// much memory. Truncating at the length prevents this and has no other
// negative effects.
func (sn *SegmentNode) ToBuffer() []byte {
	return sn.data[:sn.Len()]
}

// NewSegmentNodeFromBuffer creates a new segment node by using the underlying
// buffer without copying data. Only use this when you can be sure that it's
// safe to share the data or create your own copy.
func NewSegmentNodeFromBuffer(buf []byte) *SegmentNode {
	return &SegmentNode{data: buf}
}
