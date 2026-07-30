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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type Seeker interface {
	Seek(key []byte) (segmentindex.Node, error)
}

type SegmentCursor interface {
	First() ([]byte, BitmapLayer, error)
	Next() ([]byte, BitmapLayer, error)
	Seek([]byte) ([]byte, BitmapLayer, error)
}

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can either start at the beginning using [*SegmentCursor.First] or start
// at an arbitrary key that you may find using [*SegmentCursor.Seek]
type segmentCursor struct {
	index      Seeker
	data       []byte
	nextOffset uint64
}

// NewSegmentCursor creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*segmentCursor.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursor(data []byte, index Seeker) *segmentCursor {
	return &segmentCursor{index: index, data: data, nextOffset: 0}
}

func (c *segmentCursor) Next() ([]byte, BitmapLayer, error) {
	if c.nextOffset >= uint64(len(c.data)) {
		return nil, BitmapLayer{}, nil
	}

	sn := NewSegmentNodeFromBuffer(c.data[c.nextOffset:])
	c.nextOffset += sn.Len()
	// cursor consumers (compaction, sorting, aggregation) require non-nil
	// layers; substitute empties for the accessors' nil returns
	additions := sn.Additions()
	if additions == nil {
		additions = sroar.NewBitmap()
	}
	deletions := sn.Deletions()
	if deletions == nil {
		deletions = sroar.NewBitmap()
	}
	layer := BitmapLayer{
		Additions: additions,
		Deletions: deletions,
	}
	return sn.PrimaryKey(), layer, nil
}

func (c *segmentCursor) First() ([]byte, BitmapLayer, error) {
	c.nextOffset = 0
	return c.Next()
}

func (c *segmentCursor) Seek(key []byte) ([]byte, BitmapLayer, error) {
	node, err := c.index.Seek(key)
	if err != nil {
		return nil, BitmapLayer{}, err
	}
	c.nextOffset = node.Start
	return c.Next()
}

// SegmentCursorRaw iterates a segment's nodes yielding the raw serialized
// regions instead of bitmap views, allocating nothing per node. The slices
// alias the segment data, so they are only valid while the segment stays
// pinned; keys are never empty, so a nil key signals the end.
type SegmentCursorRaw struct {
	data       []byte
	nextOffset uint64
}

// NewSegmentCursorRaw takes the payload region of a segment, like
// [NewSegmentCursor].
func NewSegmentCursorRaw(data []byte) *SegmentCursorRaw {
	return &SegmentCursorRaw{data: data}
}

// Next returns the next node's key and serialized additions/deletions
// regions; an empty region comes back nil.
func (c *SegmentCursorRaw) Next() (key, additions, deletions []byte) {
	if c.nextOffset+8 > uint64(len(c.data)) {
		return nil, nil, nil
	}
	buf := c.data[c.nextOffset:]
	nodeLen := binary.LittleEndian.Uint64(buf[0:8])
	buf = buf[:nodeLen]

	addLen := binary.LittleEndian.Uint64(buf[8:16])
	pos := uint64(16)
	if addLen > 0 {
		additions = buf[pos : pos+addLen : pos+addLen]
	}
	pos += addLen
	delLen := binary.LittleEndian.Uint64(buf[pos : pos+8])
	pos += 8
	if delLen > 0 {
		deletions = buf[pos : pos+delLen : pos+delLen]
	}
	pos += delLen
	keyLen := uint64(binary.LittleEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	key = buf[pos : pos+keyLen]

	c.nextOffset += nodeLen
	return key, additions, deletions
}
