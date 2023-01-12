//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package roaringset

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// Compactor takes in a left and a right segment and merges them into a single
// segment. The input segments are represented by cursors without their
// respective segmentindexes. A new segmentindex is built from the merged nodes
// without taking the old indexes into account at all.
//
// The left segment must precede the right one in its creation time, as the
// compactor applies latest-takes-presence rules when there is a conflict.
//
// # Merging independent key/value pairs
//
// The new segment's nodes will be in sorted fashion (this is a requirement for
// the segment index and segment cursors to function). To achieve a sorted end
// result, the Compactor goes over both input cursors simultaneously and always
// works on the smaller of the two keys. After a key/value pair has been added
// to the output only the input cursor that provided the pair is advanced.
//
// # Merging key/value pairs with identical keys
//
// When both segment have a key/value pair with an overlapping key, the value
// has to be merged. The merge logic is not part of the compactor itself.
// Instead it makes use of [BitmapLayers.Merge].
//
// # Exit Criterium
//
// When both cursors no longer return values, all key/value pairs are
// considered compacted. The compactor then deals with metadata.
//
// # Index and Header metadata
//
// Only once the key/value pairs have been compacted, will the compactor write
// the primary index based on the new key/value payload. Finally, the input
// writer is rewinded to be able to write the header metadata at the beginning
// of the file. Because of this, the input writer must be an [io.WriteSeeker],
// such as [*os.File].
//
// The level of the resulting segment is the input level increased by one.
// Levels help the "eligible for compaction" cycle to find suitable compaction
// pairs.
type Compactor struct {
	left, right  *SegmentCursor
	currentLevel uint16

	w    io.WriteSeeker
	bufw *bufio.Writer

	scratchSpacePath string
}

// NewCompactor from left (older) and right (newer) seeker. See [Compactor] for
// an explanation of what goes on under the hood, and why the input
// requirements are the way they are.
func NewCompactor(w io.WriteSeeker,
	left, right *SegmentCursor, level uint16,
	scratchSpacePath string,
) *Compactor {
	return &Compactor{
		left:             left,
		right:            right,
		w:                w,
		bufw:             bufio.NewWriterSize(w, 256*1024),
		currentLevel:     level,
		scratchSpacePath: scratchSpacePath,
	}
}

// Do starts a compaction. See [Compactor] for an explanation of this process.
func (c *Compactor) Do() error {
	if err := c.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	kis, err := c.writeNodes()
	if err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	if err := c.writeIndexes(kis); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	dataEnd := uint64(kis[len(kis)-1].ValueEnd)

	if err := c.writeHeader(c.currentLevel+1, 0, 0,
		dataEnd); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return nil
}

func (c *Compactor) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

// nodeCompactor is a helper type to improve the code structure of merging
// nodes in a compaction
type nodeCompactor struct {
	left, right           *SegmentCursor
	keyLeft, keyRight     []byte
	valueLeft, valueRight BitmapLayer
	output                []segmentindex.Key
	offset                int
	bufw                  *bufio.Writer
}

func (c *Compactor) writeNodes() ([]segmentindex.Key, error) {
	nc := &nodeCompactor{
		left:  c.left,
		right: c.right,
		bufw:  c.bufw,
	}

	nc.init()

	if err := nc.loopThroughKeys(); err != nil {
		return nil, err
	}

	return nc.output, nil
}

func (c *nodeCompactor) init() {
	c.keyLeft, c.valueLeft, _ = c.left.First()
	c.keyRight, c.valueRight, _ = c.right.First()

	// the (dummy) header was already written, this is our initial offset
	c.offset = segmentindex.HeaderSize
}

func (c *nodeCompactor) loopThroughKeys() error {
	for {
		if c.keyLeft == nil && c.keyRight == nil {
			return nil
		}

		if c.keysEqual() {
			if err := c.mergeIdenticalKeys(); err != nil {
				return err
			}
		} else if c.leftKeySmallerOrRightNotSet() {
			if err := c.takeLeftKey(); err != nil {
				return err
			}
		} else {
			if err := c.takeRightKey(); err != nil {
				return err
			}
		}
	}
}

func (c *nodeCompactor) keysEqual() bool {
	return bytes.Equal(c.keyLeft, c.keyRight)
}

func (c *nodeCompactor) leftKeySmallerOrRightNotSet() bool {
	return (c.keyLeft != nil && bytes.Compare(c.keyLeft, c.keyRight) == -1) || c.keyRight == nil
}

func (c *nodeCompactor) mergeIdenticalKeys() error {
	layers := BitmapLayers{
		{Additions: c.valueLeft.Additions, Deletions: c.valueLeft.Deletions},
		{Additions: c.valueRight.Additions, Deletions: c.valueRight.Deletions},
	}
	merged, err := layers.Merge()
	if err != nil {
		return fmt.Errorf("merge bitmap layers for identical keys: %w", err)
	}

	sn, err := NewSegmentNode(c.keyRight, merged.Additions, merged.Deletions)
	if err != nil {
		return fmt.Errorf("new segment node for merged key: %w", err)
	}

	ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
	if err != nil {
		return fmt.Errorf("write individual node (merged key): %w", err)
	}

	c.offset = ki.ValueEnd
	c.output = append(c.output, ki)

	// advance both!
	c.keyLeft, c.valueLeft, _ = c.left.Next()
	c.keyRight, c.valueRight, _ = c.right.Next()
	return nil
}

func (c *nodeCompactor) takeLeftKey() error {
	sn, err := NewSegmentNode(c.keyLeft, c.valueLeft.Additions, c.valueLeft.Deletions)
	if err != nil {
		return fmt.Errorf("new segment node for left key: %w", err)
	}

	ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
	if err != nil {
		return fmt.Errorf("write individual node (left key): %w", err)
	}

	c.offset = ki.ValueEnd
	c.output = append(c.output, ki)
	c.keyLeft, c.valueLeft, _ = c.left.Next()
	return nil
}

func (c *nodeCompactor) takeRightKey() error {
	sn, err := NewSegmentNode(c.keyRight, c.valueRight.Additions, c.valueRight.Deletions)
	if err != nil {
		return fmt.Errorf("new segment node for right key: %w", err)
	}

	ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
	if err != nil {
		return fmt.Errorf("write individual node (right key): %w", err)
	}

	c.offset = ki.ValueEnd
	c.output = append(c.output, ki)
	c.keyRight, c.valueRight, _ = c.right.Next()
	return nil
}

func (c *Compactor) writeIndexes(keys []segmentindex.Key) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: 0,
		ScratchSpacePath:    c.scratchSpacePath,
	}

	_, err := indexes.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *Compactor) writeHeader(level, version, secondaryIndices uint16,
	startOfIndex uint64,
) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategyRoaringSet,
		IndexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}
