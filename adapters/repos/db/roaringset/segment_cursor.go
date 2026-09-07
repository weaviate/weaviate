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
	"github.com/weaviate/sroar"
)

type Seeker interface {
	// SeekPayloadStart returns where the node holding the smallest key >= key
	// begins, relative to the payload buffer NewSegmentCursor was given rather
	// than to the segment file, or lsmkv.NotFound past the highest key.
	SeekPayloadStart(key []byte) (uint64, error)
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
	start, err := c.index.SeekPayloadStart(key)
	if err != nil {
		return nil, BitmapLayer{}, err
	}
	c.nextOffset = start
	return c.Next()
}
