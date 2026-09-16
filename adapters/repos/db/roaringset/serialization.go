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

// AdditionsUnlimited returns the additions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.AdditionsWithCopy].
// CAUTION: bitmap uses entire capacity of underlying buffer. By expanding it may overwrite
// node's data after additions bitmap
//
// It returns nil when the node holds no additions (length indicator 0), the
// same contract as [*SegmentNode.Additions].
func (sn *SegmentNode) AdditionsUnlimited() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	buf := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	if len(buf) == 0 {
		return nil
	}
	return sroar.FromBufferUnlimited(buf)
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

// NewSegmentNodeCompacted builds the node into scratch, growing it as needed and
// handing it back for the next call. Write the node through [SegmentNode.ToBuffer];
// scratch runs to the largest node built so far, so writing it writes trailing
// bytes that belong to no node. The node points into scratch, so nothing may hold
// it past the next call, and neither bitmap may live in scratch: sroar leaves an
// overlapping side undefined, and nothing here checks for one. Whether such a
// caller loses the side, crashes, or is saved by a reallocation depends on
// whether the node it is building happens to outgrow the scratch it was handed.
// A nil bitmap is an empty side.
func NewSegmentNodeCompacted(
	key []byte, additions, deletions *sroar.Bitmap, scratch []byte,
) (SegmentNode, []byte, error) {
	buf := scratch
	// Ahead of the callbacks below, so none of them sizes a node around a key
	// length the node cannot record.
	if len(key) > math.MaxUint32 {
		return SegmentNode{}, scratch, fmt.Errorf("key too long: %d bytes, max is %d",
			len(key), math.MaxUint32)
	}

	// offset + 2*uint64 length indicators + uint32 length indicator
	const overhead = 8 + 8 + 8 + 4

	var aSize, dSize int
	// IsEmpty walks every container until it finds a non-empty one, so an emptied
	// bitmap pays a full walk; the switch below would ask twice.
	addEmpty, delEmpty := additions.IsEmpty(), deletions.IsEmpty()

	// An empty side skips CompactedToBuf, which would hand back a non-empty
	// region where NewSegmentNode writes a zero length indicator and Additions
	// reads it back as nil. Where both sides are present the calls nest: the
	// deletions region starts after the additions one, so the inner callback is
	// the first point at which both sizes are known, and it must not touch
	// deletions — sroar sizes the outer result before calling back.
	writeAdditions := func() {
		additions.CompactedToBuf(func(size int) []byte {
			aSize = size
			buf = byteops.Resize(buf, overhead+aSize+dSize+len(key))
			// The cap stops CompactedToBuf adopting the deletions region and the
			// key as this bitmap's spare capacity.
			return buf[16 : 16+aSize : 16+aSize]
		})
	}

	switch {
	case !addEmpty && !delEmpty:
		deletions.CompactedToBuf(func(size int) []byte {
			dSize = size
			writeAdditions()
			return buf[24+aSize : 24+aSize+dSize : 24+aSize+dSize]
		})
	case !addEmpty:
		writeAdditions()
	case !delEmpty:
		deletions.CompactedToBuf(func(size int) []byte {
			dSize = size
			buf = byteops.Resize(buf, overhead+dSize+len(key))
			return buf[24 : 24+dSize : 24+dSize]
		})
	default:
		buf = byteops.Resize(buf, overhead+len(key))
	}

	// The payloads are already in place; this writes every remaining byte,
	// because Resize hands back the previous node's bytes still in it.
	rw := byteops.NewReadWriter(buf)
	rw.WriteUint64(uint64(len(buf)))
	rw.WriteUint64(uint64(aSize))
	rw.MoveBufferToAbsolutePosition(uint64(16 + aSize))
	rw.WriteUint64(uint64(dSize))
	rw.MoveBufferToAbsolutePosition(uint64(24 + aSize + dSize))
	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(key); err != nil {
		return SegmentNode{}, buf[:cap(buf)], err
	}

	return SegmentNode{data: buf}, buf[:cap(buf)], nil
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
