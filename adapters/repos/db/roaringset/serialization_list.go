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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// SegmentNodeList is inspired by the roaringset.SegmentNode, keeping the
// same fuctionality, but stores lists of []uint64 instead of bitmaps.
// It stores one Key-Value pair (without its index) in the LSM Segment.
// As with SegmentNode, it uses a single []byte internally. As a result
// there is no decode step required at runtime. Instead you can use
//
//   - [*SegmentNodeList.Additions]
//   - [*SegmentNodeList.AdditionsWithCopy]
//   - [*SegmentNodeList.Deletions]
//   - [*SegmentNodeList.DeletionsWithCopy]
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

// AdditionsWithCopy returns the additions []uint64 without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNodeList.Additions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNodeList.Additions].
func (sn *SegmentNodeList) AdditionsWithCopy() []uint64 {
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

// DeletionsWithCopy returns the deletions []uint64 without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNodeList.Deletions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNodeList.Deletions].
func (sn *SegmentNodeList) DeletionsWithCopy() []uint64 {
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

	// offset + 2*uint64 length indicators + uint32 length indicator + payloads
	lenAdditions := 8 * len(additions)
	lenDeletions := 8 * len(deletions)
	expectedSize := 8 + 8 + 8 + 4 + lenAdditions + lenDeletions + len(key)
	sn := SegmentNodeList{
		data: make([]byte, expectedSize),
	}

	rw := byteops.NewReadWriter(sn.data)

	// reserve the first 8 bytes for the offset, which we will write at the very
	// end
	rw.MoveBufferPositionForward(8)
	rw.WriteUint64(uint64(lenAdditions))
	for _, v := range additions {
		rw.WriteUint64(v)
	}
	rw.WriteUint64(uint64(lenDeletions))
	for _, v := range deletions {
		rw.WriteUint64(v)
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
