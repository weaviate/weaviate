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

package lsmkv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type compactorSet struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1 *segmentCursorCollection
	c2 *segmentCursorCollection

	// the level matching those of the cursors
	currentLevel        uint16
	secondaryIndexCount uint16
	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones bool

	w    io.WriteSeeker
	bufw *bufio.Writer

	scratchSpacePath string
}

func newCompactorSetCollection(w io.WriteSeeker,
	c1, c2 *segmentCursorCollection, level, secondaryIndexCount uint16,
	scratchSpacePath string, cleanupTombstones bool,
) *compactorSet {
	return &compactorSet{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 256*1024),
		currentLevel:        level,
		cleanupTombstones:   cleanupTombstones,
		secondaryIndexCount: secondaryIndexCount,
		scratchSpacePath:    scratchSpacePath,
	}
}

func (c *compactorSet) do() error {
	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
	)

	kis, err := c.writeKeys(segmentFile)
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	if err := c.writeIndexes(segmentFile, kis); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return errors.Wrap(err, "flush buffered")
	}

	var dataEnd uint64 = segmentindex.HeaderSize
	if len(kis) > 0 {
		dataEnd = uint64(kis[len(kis)-1].ValueEnd)
	}

	if err := c.writeHeader(segmentFile, c.currentLevel,
		segmentindex.SegmentV1, c.secondaryIndexCount, dataEnd); err != nil {
		return errors.Wrap(err, "write header")
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorSet segment checksum: %w", err)
	}

	return nil
}

func (c *compactorSet) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

func (c *compactorSet) writeKeys(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize

	var kis []segmentindex.Key

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			values := append(value1, value2...)
			valuesMerged := newSetDecoder().DoPartial(values)
			if values, skip := c.cleanupValues(valuesMerged); !skip {
				ki, err := c.writeIndividualNode(f, offset, key2, values)
				if err != nil {
					return nil, errors.Wrap(err, "write individual node (equal keys)")
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			// advance both!
			key1, value1, _ = c.c1.next()
			key2, value2, _ = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			if values, skip := c.cleanupValues(value1); !skip {
				ki, err := c.writeIndividualNode(f, offset, key1, values)
				if err != nil {
					return nil, errors.Wrap(err, "write individual node (key1 smaller)")
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			if values, skip := c.cleanupValues(value2); !skip {
				ki, err := c.writeIndividualNode(f, offset, key2, values)
				if err != nil {
					return nil, errors.Wrap(err, "write individual node (key2 smaller)")
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key2, value2, _ = c.c2.next()
		}
	}

	return kis, nil
}

func (c *compactorSet) writeIndividualNode(f *segmentindex.SegmentFile,
	offset int, key []byte, values []value,
) (segmentindex.Key, error) {
	return (&segmentCollectionNode{
		values:     values,
		primaryKey: key,
		offset:     offset,
	}).KeyIndexAndWriteTo(f.BodyWriter())
}

func (c *compactorSet) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
	}
	_, err := f.WriteIndexes(indexes)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorSet) writeHeader(f *segmentindex.SegmentFile,
	level, version, secondaryIndices uint16, startOfIndex uint64,
) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategySetCollection,
		IndexStart:       startOfIndex,
	}
	// We have to write directly to compactor writer,
	// since it has seeked back to start. The following
	// call to f.WriteHeader will not write again.
	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	if _, err := f.WriteHeader(h); err != nil {
		return err
	}

	// We need to seek back to the end so we can write a checksum
	if _, err := c.w.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end after writing header: %w", err)
	}

	c.bufw.Reset(c.w)

	return nil
}

// Removes values with tombstone set from input slice. Output slice may be smaller than input one.
// Returned skip of true means there are no values left (key can be omitted in segment)
// WARN: method can alter input slice by swapping its elements and reducing length (not capacity)
func (c *compactorSet) cleanupValues(values []value) (vals []value, skip bool) {
	if !c.cleanupTombstones {
		return values, false
	}

	// Reuse input slice not to allocate new memory
	// Rearrange slice in a way that tombstoned values are moved to the end
	// and reduce slice's length.
	last := 0
	for i := 0; i < len(values); i++ {
		if !values[i].tombstone {
			// Swap both elements instead overwritting `last` by `i`.
			// Overwrite would result in `values[last].value` pointing to the same slice
			// as `values[i].value`.
			// If `values` slice is reused by multiple nodes (as it happens for map cursors
			// `segmentCursorCollectionReusable` using `segmentCollectionNode` as buffer)
			// populating values[i].value would overwrite values[last].value
			// Swaps makes sure values[i].value and values[last].value point to different slices
			values[last], values[i] = values[i], values[last]
			last++
		}
	}

	if last == 0 {
		return nil, true
	}
	return values[:last], false
}
