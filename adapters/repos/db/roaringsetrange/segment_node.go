//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

// Len indicates the total length of the [SegmentNode]. When reading multiple
// segments back-2-back, such as in a cursor situation, the offset of element
// (n+1) is the offset of element n + Len()
func (sn *SegmentNode) Len() uint64 {
	return binary.LittleEndian.Uint64(sn.data[0:8])
}

func (sn *SegmentNode) Key() uint8 {
	sn.rw.MoveBufferToAbsolutePosition(8)
	return sn.rw.ReadUint8()
}

// Additions returns the additions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur.
func (sn *SegmentNode) Additions() *sroar.Bitmap {
	sn.rw.MoveBufferToAbsolutePosition(9)
	return sroar.FromBuffer(sn.rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

// Deletions returns the deletions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur.
func (sn *SegmentNode) Deletions() *sroar.Bitmap {
	sn.rw.MoveBufferToAbsolutePosition(8)
	if key := sn.rw.ReadUint8(); key != 0 {
		return nil
	}
	sn.rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return sroar.FromBuffer(sn.rw.ReadBytesFromBufferWithUint64LengthIndicator())
}

func NewSegmentNode(key uint8, additions, deletions *sroar.Bitmap) (*SegmentNode, error) {
	additionsBuf := additions.ToBuffer()
	var deletionsBuf []byte

	// total len + key + length indicators + payload
	expectedSize := 8 + 1 + 8 + len(additionsBuf)

	if key == 0 {
		deletionsBuf = deletions.ToBuffer()
		expectedSize += 8 + len(deletionsBuf)
	}

	data := make([]byte, expectedSize)
	rw := byteops.NewReadWriter(data)

	// reserve the first 8 bytes for the offset, which will be written at the very end
	rw.MoveBufferPositionForward(8)
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
