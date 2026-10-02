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

package roaringsetrange

import (
	"encoding/binary"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// SegmentNode stores one Key-Value pair in
// the LSM Segment.  It uses a single []byte internally. As a result there is
// no decode step required at runtime. Instead you can use
//
//   - [*SegmentNode.Key]
//   - [*SegmentNode.Additions]
//   - [*SegmentNode.Deletions]
//
// to access the contents. Those helpers in turn do not require a decoding
// step. The accessor methods that return Roaring Bitmaps only point to
// existing memory.
//
// This makes the SegmentNode very fast to access at query time, even when it
// contains a large amount of data.
//
// The internal structure of the data is:
//
//	byte begin-start    | description
//	--------------------|-----------------------------------------------------
//	0:8                 | uint64 indicating the total length of the node,
//	                    | this is used in cursors to identify the next node.
//	8:9					| key
//	9:17                | uint64 length indicator for additions sraor bitmap (x)
//	17:(17+x)           | additions bitmap
//	(17+x):(25+x)       | uint64 length indicator for deletions sroar bitmap (y)
//	(25+x):(25+x+y)     | deletions bitmap
//						| deletion indicator and bitmaps are used only for key == 0
type SegmentNode struct {
	data []byte
	rw   byteops.ReadWriter
}

// The layout above gives the fixed-width fields these byte sizes.
const (
	NodeLengthSize   = 8
	KeySize          = 1
	BitmapLengthSize = 8
)

// AdditionsStart is where the layout above puts the additions payload, and is
// therefore what a node costs before it. The deletions length indicator and
// payload follow that payload, and only key 0 carries them.
const AdditionsStart = NodeLengthSize + KeySize + BitmapLengthSize

// Len indicates the total length of the [SegmentNode]. When reading multiple
// segments back-2-back, such as in a cursor situation, the offset of element
// (n+1) is the offset of element n + Len()
func (sn *SegmentNode) Len() uint64 {
	return binary.LittleEndian.Uint64(sn.data[:NodeLengthSize])
}

func (sn *SegmentNode) Key() uint8 {
	sn.rw.MoveBufferToAbsolutePosition(NodeLengthSize)
	return sn.rw.ReadUint8()
}

// Additions returns the additions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur.
func (sn *SegmentNode) Additions() *sroar.Bitmap {
	sn.rw.MoveBufferToAbsolutePosition(NodeLengthSize + KeySize)
	return sroar.FromBuffer(sn.rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

// Deletions returns the deletions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur.
func (sn *SegmentNode) Deletions() *sroar.Bitmap {
	sn.rw.MoveBufferToAbsolutePosition(NodeLengthSize)
	if key := sn.rw.ReadUint8(); key != 0 {
		return nil
	}
	sn.rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return sroar.FromBuffer(sn.rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

// NewSegmentNode has no production caller. The tests build input segments with it
// and compare NewSegmentNodeCompacted's and the flush writer's bytes against it.
func NewSegmentNode(key uint8, additions, deletions *sroar.Bitmap) (*SegmentNode, error) {
	additionsBuf := additions.ToBuffer()
	var deletionsBuf []byte

	expectedSize := AdditionsStart + len(additionsBuf)

	if key == 0 {
		deletionsBuf = deletions.ToBuffer()
		expectedSize += BitmapLengthSize + len(deletionsBuf)
	}

	data := make([]byte, expectedSize)
	rw := byteops.NewReadWriter(data)

	// reserve the node length field, which is written at the very end
	rw.MoveBufferPositionForward(NodeLengthSize)
	rw.CopyBytesToBuffer([]byte{key})

	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(additionsBuf); err != nil {
		return nil, err
	}

	if key == 0 {
		if err := rw.CopyBytesToBufferWithUint64LengthIndicator(deletionsBuf); err != nil {
			return nil, err
		}
	}

	offset := rw.Position
	rw.MoveBufferToAbsolutePosition(0)
	rw.WriteUint64(uint64(offset))

	return &SegmentNode{
		data: data,
		rw:   rw,
	}, nil
}

// NewSegmentNodeCompacted builds the node into scratch, growing it as needed and
// handing it back for the next call. Write the node through [SegmentNode.ToBuffer].
// Scratch runs to the largest node built so far, so writing it appends trailing
// bytes belonging to no node. The node points into scratch, so nothing may hold
// it past the next call. Neither bitmap may live in scratch, because sroar leaves
// an overlapping side undefined and nothing here checks for one. A nil bitmap is an
// empty side, and a non-zero key records no deletions.
func NewSegmentNodeCompacted(key uint8, additions, deletions *sroar.Bitmap,
	scratch []byte,
) (SegmentNode, []byte) {
	overhead := AdditionsStart
	if key == 0 {
		overhead = AdditionsStart + BitmapLengthSize
	}

	buf := scratch
	var additionsSize, deletionsSize int
	// IsEmpty walks every container until it finds a non-empty one, and the
	// switch below tests each side in two of its arms, so each is asked once.
	additionsEmpty := additions.IsEmpty()
	noDeletions := key != 0 || deletions.IsEmpty()

	// An empty side skips CompactedToBuf. Calling it would hand back a non-empty
	// region where a zero length indicator belongs, and Additions would read a
	// bitmap there. Where both sides are present the two calls nest, because the
	// deletions region starts after the additions one. The inner callback is the
	// first point at which both sizes are known, and it must not touch deletions,
	// which sroar sizes before it calls back.
	writeAdditions := func() {
		additions.CompactedToBuf(func(size int) []byte {
			additionsSize = size
			buf = byteops.Resize(buf, overhead+additionsSize+deletionsSize)
			end := AdditionsStart + additionsSize
			// The three-index slice holds sroar to this payload's own region,
			// which it would otherwise adopt to the end of buf's capacity.
			return buf[AdditionsStart:end:end]
		})
	}
	deletionsRegion := func() []byte {
		start := AdditionsStart + additionsSize + BitmapLengthSize
		end := start + deletionsSize
		return buf[start:end:end]
	}

	switch {
	case !additionsEmpty && !noDeletions:
		deletions.CompactedToBuf(func(size int) []byte {
			deletionsSize = size
			writeAdditions()
			return deletionsRegion()
		})
	case !additionsEmpty:
		writeAdditions()
	case !noDeletions:
		deletions.CompactedToBuf(func(size int) []byte {
			deletionsSize = size
			buf = byteops.Resize(buf, overhead+deletionsSize)
			return deletionsRegion()
		})
	default:
		buf = byteops.Resize(buf, overhead)
	}

	// The payloads are already in place. Every remaining byte is written here,
	// because a reused scratch still holds the previous node's bytes.
	rw := byteops.NewReadWriter(buf)
	rw.WriteUint64(uint64(len(buf)))
	rw.WriteByte(key)
	rw.WriteUint64(uint64(additionsSize))
	if key == 0 {
		rw.MoveBufferToAbsolutePosition(uint64(AdditionsStart + additionsSize))
		rw.WriteUint64(uint64(deletionsSize))
	}

	return SegmentNode{data: buf, rw: rw}, buf[:cap(buf)]
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
	return &SegmentNode{
		data: buf,
		rw:   byteops.NewReadWriter(buf),
	}
}
