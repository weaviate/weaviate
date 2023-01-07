package roaringset

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dgraph-io/sroar"
)

// A RoaringSet Segment Node stores one Key-Value pair (without its index) in
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
//	0-8                 | uint64 length indicator for additions sraor bm -> x
//	8-(x+8)             | additions bitmap
//	(x+8)-(x+16         | uint64 length indicator for deletions sroar bm -> y
//	(x+16)-(x+y+16)     | deletions bitmap
//	(x+y+16)-(x+y+20)   | uint32 length indicator for primary key length -> z
//	(x+y+20)-(x+y+z+20) | primary key
type SegmentNode struct {
	// Offset is not persisted, it can be used to cache the offset when reading
	// multiple nodes
	Offset int

	data []byte
}

// Additions returns the additions roaring bitmap with shared state. Only use
// this method if you can garantuee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.AdditionsWithCopy].
func (sn *SegmentNode) Additions() *sroar.Bitmap {
	additionsLength := binary.LittleEndian.Uint64(sn.data[0:8])
	return sroar.FromBuffer(sn.data[8 : 8+additionsLength])
}

// AdditionsWithCopy returns the additions roaring bitmap without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNode.Additions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNode.Additions].
func (sn *SegmentNode) AdditionsWithCopy() *sroar.Bitmap {
	additionsLength := binary.LittleEndian.Uint64(sn.data[0:8])
	return sroar.FromBufferWithCopy(sn.data[8 : 8+additionsLength])
}

// Deletions returns the deletions roaring bitmap with shared state. Only use
// this method if you can garantuee that you will only use it while holding a
// maintenance lock or can otherwise be sure that no compaction can occur. If
// you can't guarantee that, instead use [*SegmentNode.DeletionsWithCopy].
func (sn *SegmentNode) Deletions() *sroar.Bitmap {
	additionsLength := binary.LittleEndian.Uint64(sn.data[0:8])
	offset := additionsLength + 8
	deletionsLength := binary.LittleEndian.Uint64(sn.data[offset : offset+8])
	offset += 8
	return sroar.FromBuffer(sn.data[offset : offset+deletionsLength])
}

// DeletionsWithCopy returns the deletions roaring bitmap without sharing state. It
// creates a copy of the underlying buffer. This is safe to use indefinitely,
// but much slower than [*SegmentNode.Deletions] as it requires copying all the
// memory. If you know that you will only need the contents of the node for a
// duration of time where a lock is held that prevents compactions, it is more
// efficient to use [*SegmentNode.Deletions].
func (sn *SegmentNode) DeletionsWithCopy() *sroar.Bitmap {
	additionsLength := binary.LittleEndian.Uint64(sn.data[0:8])
	offset := additionsLength + 8
	deletionsLength := binary.LittleEndian.Uint64(sn.data[offset : offset+8])
	offset += 8
	return sroar.FromBufferWithCopy(sn.data[offset : offset+deletionsLength])
}

func (sn *SegmentNode) PrimaryKey() []byte {
	additionsLength := binary.LittleEndian.Uint64(sn.data[0:8])
	offset := additionsLength + 8
	deletionsLength := binary.LittleEndian.Uint64(sn.data[offset : offset+8])
	offset += 8 + deletionsLength
	keyLen := binary.LittleEndian.Uint32(sn.data[offset : offset+4])
	offset += 4
	return sn.data[offset : offset+uint64(keyLen)]
}

func NewSegmentNode(
	key []byte, additions, deletions *sroar.Bitmap,
) (*SegmentNode, error) {
	if len(key) > math.MaxUint32 {
		return nil, fmt.Errorf("cannot store keys longer than %d", math.MaxUint32)
	}

	additionsBuf := additions.ToBuffer()
	deletionsBuf := deletions.ToBuffer()

	expectedSize := 8 + 8 + 4 + len(additionsBuf) + len(deletionsBuf) + len(key)
	sn := SegmentNode{
		data: make([]byte, expectedSize),
	}

	offset := uint64(0)
	binary.LittleEndian.PutUint64(sn.data[offset:offset+8], uint64(len(additionsBuf)))
	offset += 8
	copy(sn.data[offset:offset+uint64(len(additionsBuf))], additionsBuf)
	offset += uint64(len(additionsBuf))

	binary.LittleEndian.PutUint64(sn.data[offset:offset+8], uint64(len(deletionsBuf)))
	offset += 8
	copy(sn.data[offset:offset+uint64(len(deletionsBuf))], deletionsBuf)
	offset += uint64(len(deletionsBuf))

	binary.LittleEndian.PutUint32(sn.data[offset:offset+4], uint32(len(key)))
	offset += 4
	copy(sn.data[offset:offset+uint64(len(key))], key)
	offset += uint64(len(key))

	return &sn, nil
}

// ToBuffer returns the internal buffer without copying data. Only use this,
// when you can be sure that it's safe to share the data, or create your own copy.
func (sn *SegmentNode) ToBuffer() []byte {
	return sn.data
}

// NewSegmentNodeFromBuffer creates a new segment node by using the underlying
// buffer without copying data. Only use this when you can be sure that it's
// safe to share the data or create your own copy.
func NewSegmentNodeFromBuffer(buf []byte) *SegmentNode {
	return &SegmentNode{data: buf}
}
