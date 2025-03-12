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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type SegmentCursor interface {
	First() (uint8, roaringset.BitmapLayer, bool)
	Next() (uint8, roaringset.BitmapLayer, bool)
}

// A SegmentCursorMmap iterates over all key-value pairs in a single disk segment.
// You can start at the beginning using [*SegmentCursorMmap.First] and move forward
// using [*SegmentCursorMmap.Next]
type SegmentCursorMmap struct {
	data       []byte
	nextOffset uint64
}

// NewSegmentCursorMmap creates a cursor for a single disk segment. Make sure that
// the data buf is already sliced correctly to start at the payload, as calling
// [*SegmentCursorMmap.First] will start reading at offset 0 relative to the passed
// in buffer. Similarly, the buffer may only contain payloads, as the buffer end
// is used to determine if more keys can be found.
//
// Therefore if the payload is part of a longer continuous buffer, the cursor
// should be initialized with data[payloadStartPos:payloadEndPos]
func NewSegmentCursorMmap(data []byte) *SegmentCursorMmap {
	return &SegmentCursorMmap{data: data, nextOffset: 0}
}

func (c *SegmentCursorMmap) First() (uint8, roaringset.BitmapLayer, bool) {
	c.nextOffset = 0
	return c.Next()
}

func (c *SegmentCursorMmap) Next() (uint8, roaringset.BitmapLayer, bool) {
	if c.nextOffset >= uint64(len(c.data)) {
		return 0, roaringset.BitmapLayer{}, false
	}

	sn := NewSegmentNodeFromBuffer(c.data[c.nextOffset:])
	c.nextOffset += sn.Len()

	return sn.Key(), roaringset.BitmapLayer{
		Additions: sn.Additions(),
		Deletions: sn.Deletions(),
	}, true
}

// ================================================================================

// A SegmentCursor iterates over all key-value pairs in a single disk segment.
// You can start at the beginning using [*SegmentCursorPread.First] and move forward
// using [*SegmentCursorPread.Next]
type SegmentCursorPread struct {
	readSeeker     io.ReadSeeker
	reader         *bufio.Reader
	lenBuf         []byte
	nodeBufs       [][]byte
	nodeBufPos     int
	nodeBufMinSize int
}

// NewSegmentCursorPread creates a cursor for a single disk segment. Make sure that
// the reader has offset = 0 set correctly to start at the payload, as calling
// [*SegmentCursorPread.First] will start reading at offset 0.
// Similarly, the reader may only read payload, as the EOF
// is used to determine if more keys can be found.
//
// bufferCount tells how many exclusive payload buffers should be used to return
// expected data. Set multiple buffers if data returned by First/Next will not be used
// before following call will be made, not to overwrite previously fetched values.
// (e.g. count 3 means, 3 buffers will be used internally and following calls to First/Next
// will return data in buffers: 1, 2, 3, 1, 2, 3, ...)
func NewSegmentCursorPread(readSeeker io.ReadSeeker, bufferCount int) *SegmentCursorPread {
	readSeeker.Seek(0, io.SeekStart)
	return &SegmentCursorPread{
		readSeeker:     readSeeker,
		reader:         bufio.NewReaderSize(readSeeker, 10*1024*1024),
		lenBuf:         make([]byte, 8),
		nodeBufs:       make([][]byte, bufferCount),
		nodeBufPos:     0,
		nodeBufMinSize: 0,
	}
}

func (c *SegmentCursorPread) First() (uint8, roaringset.BitmapLayer, bool) {
	c.readSeeker.Seek(0, io.SeekStart)
	c.reader.Reset(c.readSeeker)
	return c.read(true)
}

func (c *SegmentCursorPread) Next() (uint8, roaringset.BitmapLayer, bool) {
	return c.read(false)
}

func (c *SegmentCursorPread) read(isFirst bool) (uint8, roaringset.BitmapLayer, bool) {
	n, err := io.ReadFull(c.reader, c.lenBuf)
	if err == io.EOF || onlyChecksumRemaining(n, err) {
		return 0, roaringset.BitmapLayer{}, false
	}
	if err != nil {
		panic(fmt.Errorf("SegmentCursorReader::Next reading node length: %w", err))
	}

	nodeLen := binary.LittleEndian.Uint64(c.lenBuf)
	nodeBuf := c.getNodeBuf(int(nodeLen))

	_, err = io.ReadFull(c.reader, nodeBuf[8:])
	if err != nil {
		panic(fmt.Errorf("SegmentCursorReader::Next reading node: %w", err))
	}

	copy(nodeBuf, c.lenBuf)
	sn := NewSegmentNodeFromBuffer(nodeBuf)

	deletions := sn.Deletions()
	if isFirst {
		c.updateNodeBufMinSize(int(nodeLen) - len(deletions.ToBuffer()))
	}

	return sn.Key(), roaringset.BitmapLayer{
		Additions: sn.Additions(),
		Deletions: deletions,
	}, true
}

func (c *SegmentCursorPread) getNodeBuf(size int) []byte {
	pos := c.nodeBufPos
	c.nodeBufPos = (c.nodeBufPos + 1) % len(c.nodeBufs)

	if cap(c.nodeBufs[pos]) < size {
		newSize := c.nodeBufMinSize
		if newSize < size {
			newSize = size
		}
		c.nodeBufs[pos] = make([]byte, newSize)
	}
	return c.nodeBufs[pos][:size]
}

// First node's additions (non-null) contains all ids present in the segment.
// By setting minimum buffer size to maximum size in use, it is ensured, that
// existing buffer will fit data of following nodes
func (c *SegmentCursorPread) updateNodeBufMinSize(size int) {
	if c.nodeBufMinSize < size {
		c.nodeBufMinSize = size
	}
}

// ================================================================================

type GaplessSegmentCursor struct {
	cursor SegmentCursor

	started   bool
	key       uint8
	readKey   uint8
	readLayer roaringset.BitmapLayer
	readOk    bool
}

func NewGaplessSegmentCursor(cursor SegmentCursor) *GaplessSegmentCursor {
	return &GaplessSegmentCursor{cursor: cursor, started: false, key: 0}
}

func (c *GaplessSegmentCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.started = true

	c.readKey, c.readLayer, c.readOk = c.cursor.First()

	c.key = 1
	if c.readOk && c.readKey == 0 {
		return c.readKey, c.readLayer, c.readOk
	}
	return 0, roaringset.BitmapLayer{}, true
}

func (c *GaplessSegmentCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if !c.started {
		return c.First()
	}

	if c.key >= 65 {
		return 0, roaringset.BitmapLayer{}, false
	}

	for c.readOk && c.readKey < c.key {
		c.readKey, c.readLayer, c.readOk = c.cursor.Next()
	}

	currKey := c.key
	c.key++
	if c.readOk && c.readKey == currKey {
		return currKey, c.readLayer, true
	}
	return currKey, roaringset.BitmapLayer{}, true
}

// onlyChecksumRemaining determines if the only remaining segment contents
// are the checksum. Segment file checksums were introduced with
// https://github.com/weaviate/weaviate/pull/6620.
func onlyChecksumRemaining(n int, err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) &&
		n == segmentindex.ChecksumSize
}
