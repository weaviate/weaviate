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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/contentReader"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type Seeker interface {
	Seek(key []byte) (segmentindex.Node, error)
}

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can either start at the beginning using [*SegmentCursor.First] or start
// at an arbitrary key that you may find using [*SegmentCursor.Seek]
type SegmentCursor struct {
	index         Seeker
	contentReader contentReader.ContentReader
	nextOffset    uint64
}

// NewSegmentCursor creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*SegmentCursor.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursor(contentReader contentReader.ContentReader, index Seeker) *SegmentCursor {
	return &SegmentCursor{index: index, contentReader: contentReader, nextOffset: 0}
}

func (c *SegmentCursor) Next() ([]byte, BitmapLayer, error) {
	if c.nextOffset >= c.contentReader.Length() {
		return nil, BitmapLayer{}, nil
	}

	contReader, err := c.contentReader.NewWithOffsetStart(c.nextOffset)
	if err != nil {
		return nil, BitmapLayer{}, err
	}
	sn := NewSegmentNodeFromBuffer(contReader)
	c.nextOffset += sn.Len()
	layer := BitmapLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}
	return sn.PrimaryKey(), layer, nil
}

func (c *SegmentCursor) First() ([]byte, BitmapLayer, error) {
	c.nextOffset = 0
	return c.Next()
}

func (c *SegmentCursor) Seek(key []byte) ([]byte, BitmapLayer, error) {
	node, err := c.index.Seek(key)
	if err != nil {
		return nil, BitmapLayer{}, err
	}
	c.nextOffset = node.Start
	return c.Next()
}
