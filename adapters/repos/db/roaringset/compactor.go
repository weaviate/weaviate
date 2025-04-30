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
	"bufio"
	"bytes"
	"fmt"
	"io"

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
	bufw compactorWriter
	mw   *memoryWriter

	scratchSpacePath string

	enableChecksumValidation bool
}

type compactorWriter interface {
	segmentindex.SegmentWriter
	Reset(io.Writer)
}

type memoryWriter struct {
	buffer *[]byte
	pos    *int
	maxPos *int
	writer io.WriteSeeker
}

// NewMemoryWriterWrapper creates a new memoryWriter with initialized pointers
func newMemoryWriterWrapper(initialCapacity int64, writer io.WriteSeeker) *memoryWriter {
	buf := make([]byte, initialCapacity)
	pos := 0
	maxPos := 0
	return &memoryWriter{
		buffer: &buf,
		pos:    &pos,
		maxPos: &maxPos,
		writer: writer,
	}
}

func (mw memoryWriter) Write(p []byte) (n int, err error) {
	// Get the length of the incoming data
	lenCopyBytes := len(p)

	// Check if we need to grow the buffer
	requiredSize := *mw.pos + lenCopyBytes
	if requiredSize > len(*mw.buffer) {
		return 0, io.ErrShortWrite
	}

	// Copy the data into the buffer at the current position
	numCopiedBytes := copy((*mw.buffer)[*mw.pos:], p)

	// Update the position
	*mw.pos += numCopiedBytes
	if *mw.pos >= *mw.maxPos {
		*mw.maxPos = *mw.pos
	}

	if numCopiedBytes != lenCopyBytes {
		return numCopiedBytes, errors.New("could not copy all data into buffer")
	}

	return numCopiedBytes, nil
}

func (mw memoryWriter) Flush() error {
	buf := *mw.buffer
	_, err := mw.writer.Write(buf[:*mw.maxPos])
	if err != nil {
		return err
	}

	return nil
}

func (mw memoryWriter) Reset(writer io.Writer) {}

func (mw memoryWriter) ResetWritePositionToZero() {
	*mw.pos = 0
}

func (mw memoryWriter) ResetWritePositionToMax() {
	*mw.pos = *mw.maxPos
}

// NewCompactor from left (older) and right (newer) seeker. See [Compactor] for
// an explanation of what goes on under the hood, and why the input
// requirements are the way they are.
func NewCompactor(w io.WriteSeeker,
	left, right *SegmentCursor, level uint16,
	scratchSpacePath string, cleanupDeletions bool,
	enableChecksumValidation bool, maxNewFileSize int64,
) *Compactor {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  "roaringset",
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	var writer compactorWriter
	var mw *memoryWriter
	if maxNewFileSize < 4096 {
		mw = newMemoryWriterWrapper(maxNewFileSize, w)
		writer = mw
	} else {
		writer = bufio.NewWriterSize(w, 256*1024)
	}
	return &Compactor{
		left:                     left,
		right:                    right,
		w:                        diskio.NewMeteredWriter(w, writeCB),
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupDeletions:         cleanupDeletions,
		scratchSpacePath:         scratchSpacePath,
		enableChecksumValidation: enableChecksumValidation,
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
	if err := c.writeHeader(segmentFile, c.currentLevel,
		version, 0, dataEnd); err != nil {
		return fmt.Errorf("write header: %w", err)
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
	}
	_, err := f.WriteIndexes(indexes)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *Compactor) writeHeader(f *segmentindex.SegmentFile,
	level, version, secondaryIndices uint16, startOfIndex uint64,
) error {
	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategyRoaringSet,
		IndexStart:       startOfIndex,
	}

	if c.mw == nil {
		if _, err := c.w.Seek(0, io.SeekStart); err != nil {
			return errors.Wrap(err, "seek to beginning to write header")
		}

		// We have to write directly to compactor writer,
		// since it has seeked back to start. The following
		// call to f.WriteHeader will not write again.
		if _, err := h.WriteTo(c.w); err != nil {
			return err
		}

	} else {
		c.mw.ResetWritePositionToZero()
		if _, err := h.WriteTo(c.bufw); err != nil {
			return err
		}
	}

	if _, err := f.WriteHeader(h); err != nil {
		return err
	}

	// We need to seek back to the end so we can write a checksum
	if c.mw == nil {
		if _, err := c.w.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("seek to end after writing header: %w", err)
		}
	} else {
		c.mw.ResetWritePositionToMax()
	}

	c.bufw.Reset(c.w)

	return nil
}
