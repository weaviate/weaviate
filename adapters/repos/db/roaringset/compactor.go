//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringset

import (
	"bytes"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	// Tells if deletions or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupDeletions bool

	w    io.WriteSeeker
	bufw compactor.Writer
	mw   *compactor.MemoryWriter

	scratchSpacePath string

	enableChecksumValidation bool

	maxNewFileSize int64
	allocChecker   memwatch.AllocChecker
}

// NewCompactor from left (older) and right (newer) segment. See [Compactor]
// for an explanation of what goes on under the hood, and why the input
// requirements are the way they are.
//
// # Segment Layout
//
// The layout of the segment is
// - header
// - data
// - check-sum
//
// However, it is challenging to calculate the length of the data (which is
// part of the header) before writing the file:
//
// big files (overhead is not that relevant)
// - write empty header
// - write data
// - seek back to start
// - write real header
// - seek to original position (after data)
// - write checksum
//
// # Decision Logic
//
// For small files we use a custom buffered writer, that buffers everything
// and writes just once at the end. For larger files, we use the regular
// approach as outlined above using a standard go buffered writer.
//
// The threshold to consider a file small vs large is simply the size of the
// regular buffered writer. The idea is that we would allocate
// [SegmentWriterBufferSize] bytes in any case, so if we anticipate being able
// to write the entire file in less than [SegmentWriterBufferSize] bytes, there
// is no additional cost to using the fully-in-memory approach.
func NewCompactor(w io.WriteSeeker,
	left, right *SegmentCursor, level uint16,
	scratchSpacePath string, cleanupDeletions bool,
	enableChecksumValidation bool, maxNewFileSize int64, allocChecker memwatch.AllocChecker,
) *Compactor {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  "roaringset",
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(w, writeCB)
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	return &Compactor{
		left:                     left,
		right:                    right,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupDeletions:         cleanupDeletions,
		scratchSpacePath:         scratchSpacePath,
		enableChecksumValidation: enableChecksumValidation,
		maxNewFileSize:           maxNewFileSize,
		allocChecker:             allocChecker,
	}
}

// Do starts a compaction. See [Compactor] for an explanation of this process.
func (c *Compactor) Do() error {
	if err := c.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)

	kis, err := c.writeNodes(segmentFile)
	if err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	if err := c.writeIndexes(segmentFile, kis); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if c.mw == nil {
		if err := c.bufw.Flush(); err != nil {
			return fmt.Errorf("flush buffered: %w", err)
		}
	}

	var dataEnd uint64 = segmentindex.HeaderSize
	if len(kis) > 0 {
		dataEnd = uint64(kis[len(kis)-1].ValueEnd)
	}

	version := segmentindex.ChooseHeaderVersion(c.enableChecksumValidation)
	if err := compactor.WriteHeader(c.mw, c.w, c.bufw, segmentFile, c.currentLevel, version,
		0, dataEnd, segmentindex.StrategyRoaringSet); err != nil {
		return errors.Wrap(err, "write header")
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorRoaringSet segment checksum: %w", err)
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
	bufw                  io.Writer

	cleanupDeletions bool
	emptyBitmap      *sroar.Bitmap
}

func (c *Compactor) writeNodes(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	nc := &nodeCompactor{
		left:             c.left,
		right:            c.right,
		bufw:             f.BodyWriter(),
		cleanupDeletions: c.cleanupDeletions,
		emptyBitmap:      sroar.NewBitmap(),
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

	if additions, deletions, skip := c.cleanupValues(merged.Additions, merged.Deletions); !skip {
		sn, err := NewSegmentNode(c.keyRight, additions, deletions)
		if err != nil {
			return fmt.Errorf("new segment node for merged key: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
		if err != nil {
			return fmt.Errorf("write individual node (merged key): %w", err)
		}

		c.offset = ki.ValueEnd
		c.output = append(c.output, ki)
	}

	// advance both!
	c.keyLeft, c.valueLeft, _ = c.left.Next()
	c.keyRight, c.valueRight, _ = c.right.Next()
	return nil
}

func (c *nodeCompactor) takeLeftKey() error {
	if additions, deletions, skip := c.cleanupValues(c.valueLeft.Additions, c.valueLeft.Deletions); !skip {
		sn, err := NewSegmentNode(c.keyLeft, additions, deletions)
		if err != nil {
			return fmt.Errorf("new segment node for left key: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
		if err != nil {
			return fmt.Errorf("write individual node (left key): %w", err)
		}

		c.offset = ki.ValueEnd
		c.output = append(c.output, ki)
	}

	c.keyLeft, c.valueLeft, _ = c.left.Next()
	return nil
}

func (c *nodeCompactor) takeRightKey() error {
	if additions, deletions, skip := c.cleanupValues(c.valueRight.Additions, c.valueRight.Deletions); !skip {
		sn, err := NewSegmentNode(c.keyRight, additions, deletions)
		if err != nil {
			return fmt.Errorf("new segment node for right key: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(c.bufw, c.offset)
		if err != nil {
			return fmt.Errorf("write individual node (right key): %w", err)
		}

		c.offset = ki.ValueEnd
		c.output = append(c.output, ki)
	}

	c.keyRight, c.valueRight, _ = c.right.Next()
	return nil
}

func (c *nodeCompactor) cleanupValues(additions, deletions *sroar.Bitmap,
) (add, del *sroar.Bitmap, skip bool) {
	if !c.cleanupDeletions {
		return Condense(additions), Condense(deletions), false
	}
	if !additions.IsEmpty() {
		return Condense(additions), c.emptyBitmap, false
	}
	return nil, nil, true
}

func (c *Compactor) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: 0,
		ScratchSpacePath:    c.scratchSpacePath,
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  "roaringset",
			"operation": "writeIndices",
		}),
		AllocChecker: c.allocChecker,
	}
	_, err := f.WriteIndexes(indexes, c.maxNewFileSize)
	return err
}
