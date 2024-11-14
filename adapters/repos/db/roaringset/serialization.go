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

package roaringset

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/internal/byteops"
)

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
func (sn *SegmentNode) Additions() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	return sroar.FromBuffer(rw.ReadBytesFromBufferWithUint64LengthIndicator())
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

// Deletions returns the deletions roaring bitmap with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.DeletionsWithCopy].
func (sn *SegmentNode) Deletions() *sroar.Bitmap {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return sroar.FromBuffer(rw.ReadBytesFromBufferWithUint64LengthIndicator())
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

func NewSegmentNode(
	key []byte, additions, deletions *sroar.Bitmap,
) (*SegmentNode, error) {
	if len(key) > math.MaxUint32 {
		return nil, fmt.Errorf("key too long, max length is %d", math.MaxUint32)
	}

	additionsBuf := additions.ToBuffer()
	deletionsBuf := deletions.ToBuffer()

	// offset + 2*uint64 length indicators + uint32 length indicator + payloads
	expectedSize := 8 + 8 + 8 + 4 + len(additionsBuf) + len(deletionsBuf) + len(key)
	sn := SegmentNode{
		data: make([]byte, expectedSize),
	}

	rw := byteops.NewReadWriter(sn.data)

	// reserve the first 8 bytes for the offset, which we will write at the very
	// end
	rw.MoveBufferPositionForward(8)
	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(additionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(deletionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(key); err != nil {
		return nil, err
	}

	offset := rw.Position
	rw.MoveBufferToAbsolutePosition(0)
	rw.WriteUint64(uint64(offset))

	return &sn, nil
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

// KeyIndexAndWriteTo is a helper to flush a memtables full of SegmentNodes. It
// writes itself into the given writer and returns a [segmentindex.Key] with
// start and end indicators (respecting SegmentNode.Offset). Those keys can
// then be used to build an index for the nodes. The combination of index and
// node make up an LSM segment.
//
// RoaringSets do not support secondary keys, thus the segmentindex.Key will
// only ever contain a primary key.
func (sn *SegmentNode) KeyIndexAndWriteTo(w io.Writer, offset int) (segmentindex.Key, error) {
	out := segmentindex.Key{}

	n, err := w.Write(sn.data)
	if err != nil {
		return out, err
	}

	out.ValueStart = offset
	out.ValueEnd = offset + n
	out.Key = sn.PrimaryKey()

	return out, nil
}

// SegmentNodeList is inspired by the roaringset.SegmentNode, keeping the
// same fuctionality, but stores lists of []uint64 instead of bitmaps.
// This code is here tu support recovering from WALs created on 1.26
// As with SegmentNode, it uses a single []byte internally. As a result
// there is no decode step required at runtime. Instead you can use
//
//   - [*SegmentNodeList.Additions]
//   - [*SegmentNodeList.Deletions]
//   - [*SegmentNodeList.PrimaryKey]
//
// to access the contents. Those helpers in turn do not require a decoding
// step. Instead of returning Roaring Bitmaps, it returns []uint64 slices.
// This makes the indexing time much faster, as we don't have to create the
// roaring bitmaps for each WAL insert. It also makes it smaller on disk, as we
// don't have to store the roaring bitmap, but only the list of uint64s.
//
// The internal structure of the data is close to the original SegmentNode,
// storing []uint64 instead of roaring bitmaps. The structure is as follows:
//
//	byte begin-start    | description
//	--------------------|-----------------------------------------------------
//	0-8                 | uint64 indicating the total length of the node,
//	                    | this is used in cursors to identify the next node.
//	8-16                | uint64 length indicator for additions
//	                    | len(additions)*size(uint64) -> x
//	16-(x+16)           | additions []uint64
//	(x+16)-(x+24)       | uint64 length indicator for deletions
//	                    | len(deletions)*size(uint64) -> y
//	(x+24)-(x+y+24)     | deletions []uint64
//	(x+y+24)-(x+y+28)   | uint32 length indicator for primary key length -> z
//	(x+y+28)-(x+y+z+28) | primary key
type SegmentNodeList struct {
	data []byte
}

// Len indicates the total length of the [SegmentNodeList]. When reading multiple
// segments back-2-back, such as in a cursor situation, the offset of element
// (n+1) is the offset of element n + Len()
func (sn *SegmentNodeList) Len() uint64 {
	return binary.LittleEndian.Uint64(sn.data[0:8])
}

// Additions returns the additions []uint64 with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNodeList.AdditionsWithCopy].
func (sn *SegmentNodeList) Additions() []uint64 {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	bData := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	results := make([]uint64, len(bData)/8)
	for i := 0; i < len(bData); i += 8 {
		results[i/8] = binary.LittleEndian.Uint64(bData[i : i+8])
	}
	return results
}

// Deletions returns the deletions []uint64 with shared state. Only use
// this method if you can guarantee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNodeList.DeletionsWithCopy].
func (sn *SegmentNodeList) Deletions() []uint64 {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	bData := rw.ReadBytesFromBufferWithUint64LengthIndicator()
	results := make([]uint64, len(bData)/8)
	for i := 0; i < len(bData); i += 8 {
		results[i/8] = binary.LittleEndian.Uint64(bData[i : i+8])
	}
	return results
}

func (sn *SegmentNodeList) PrimaryKey() []byte {
	rw := byteops.NewReadWriter(sn.data)
	rw.MoveBufferToAbsolutePosition(8)
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	rw.DiscardBytesFromBufferWithUint64LengthIndicator()
	return rw.ReadBytesFromBufferWithUint32LengthIndicator()
}

func NewSegmentNodeList(
	key []byte, additions, deletions []uint64,
) (*SegmentNodeList, error) {
	if len(key) > math.MaxUint32 {
		return nil, fmt.Errorf("key too long, max length is %d", math.MaxUint32)
	}

	// convert the additions and deletions []uint64 to byte slices
	additionsBuf := make([]byte, 8*len(additions))
	for i, v := range additions {
		binary.LittleEndian.PutUint64(additionsBuf[i*8:], v)
	}

	deletionsBuf := make([]byte, 8*len(deletions))
	for i, v := range deletions {
		binary.LittleEndian.PutUint64(deletionsBuf[i*8:], v)
	}

	// offset + 2*uint64 length indicators + uint32 length indicator + payloads
	expectedSize := 8 + 8 + 8 + 4 + len(additionsBuf) + len(deletionsBuf) + len(key)
	sn := SegmentNodeList{
		data: make([]byte, expectedSize),
	}

	rw := byteops.NewReadWriter(sn.data)

	// reserve the first 8 bytes for the offset, which we will write at the very
	// end
	rw.MoveBufferPositionForward(8)
	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(additionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint64LengthIndicator(deletionsBuf); err != nil {
		return nil, err
	}

	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(key); err != nil {
		return nil, err
	}

	offset := rw.Position
	rw.MoveBufferToAbsolutePosition(0)
	rw.WriteUint64(uint64(offset))

	return &sn, nil
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
func (sn *SegmentNodeList) ToBuffer() []byte {
	return sn.data[:sn.Len()]
}

// NewSegmentNodeFromBuffer creates a new segment node by using the underlying
// buffer without copying data. Only use this when you can be sure that it's
// safe to share the data or create your own copy.
func NewSegmentNodeListFromBuffer(buf []byte) *SegmentNodeList {
	return &SegmentNodeList{data: buf}
}

// KeyIndexAndWriteTo is a helper to flush a memtables full of SegmentNodes. It
// writes itself into the given writer and returns a [segmentindex.Key] with
// start and end indicators (respecting SegmentNodeList.Offset). Those keys can
// then be used to build an index for the nodes. The combination of index and
// node make up an LSM segment.
//
// RoaringSets do not support secondary keys, thus the segmentindex.Key will
// only ever contain a primary key.
func (sn *SegmentNodeList) KeyIndexAndWriteTo(w io.Writer, offset int) (segmentindex.Key, error) {
	out := segmentindex.Key{}

	n, err := w.Write(sn.data)
	if err != nil {
		return out, err
	}

	out.ValueStart = offset
	out.ValueEnd = offset + n
	out.Key = sn.PrimaryKey()

	return out, nil
}
