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

package lsmkv

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type compactorSet struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1, c2 *segmentCursorCollectionReusable

	// the level matching those of the cursors
	currentLevel        uint16
	secondaryIndexCount uint16
	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones bool

	w    io.WriteSeeker
	bufw compactor.Writer
	mw   *compactor.MemoryWriter

	maxNewFileSize int64
	allocChecker   memwatch.AllocChecker

	scratchSpacePath string

	enableChecksumValidation bool

	shouldSkipKeyFunc func(key []byte, ctx context.Context) (bool, error)

	// reusable buffers to reduce allocations during compaction
	mergedValues []value
	setDecoder   setDecoder

	writeBuf [9]byte // reused by writeIndividualNode to avoid per-key allocation
}

func newCompactorSetCollection(w io.WriteSeeker,
	c1, c2 *segmentCursorCollectionReusable, level, secondaryIndexCount uint16,
	scratchSpacePath string, cleanupTombstones bool,
	enableChecksumValidation bool, maxNewFileSize int64, allocChecker memwatch.AllocChecker,
	shouldSkipKeyFunc func(key []byte, ctx context.Context) (bool, error),
) *compactorSet {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  StrategySetCollection,
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(w, writeCB)
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	return &compactorSet{
		c1:                       c1,
		c2:                       c2,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupTombstones:        cleanupTombstones,
		secondaryIndexCount:      secondaryIndexCount,
		scratchSpacePath:         scratchSpacePath,
		enableChecksumValidation: enableChecksumValidation,
		allocChecker:             allocChecker,
		maxNewFileSize:           maxNewFileSize,
		shouldSkipKeyFunc:        shouldSkipKeyFunc,
	}
}

func (c *compactorSet) do() error {
	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)

	kis, err := c.writeKeys(segmentFile)
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	if err := c.writeIndexes(segmentFile, kis); err != nil {
		return errors.Wrap(err, "write index")
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
		c.secondaryIndexCount, dataEnd, segmentindex.StrategySetCollection); err != nil {
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

func (c *compactorSet) writeKeys(f *segmentindex.SegmentFile) ([]segmentindex.KeyRedux, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize
	var kis []segmentindex.KeyRedux

	for {
		if key1 == nil && key2 == nil {
			break
		}

		if bytes.Equal(key1, key2) {
			if err := c.processKeyPair(f, &offset, &kis, key1, key2, value1, value2); err != nil {
				return nil, err
			}
			// advance both!
			key1, value1, _ = c.c1.next()
			key2, value2, _ = c.c2.next()
		} else if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			if err := c.processKey(f, &offset, &kis, key1, value1); err != nil {
				return nil, err
			}
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			if err := c.processKey(f, &offset, &kis, key2, value2); err != nil {
				return nil, err
			}
			key2, value2, _ = c.c2.next()
		}
	}

	return kis, nil
}

func (c *compactorSet) processKeyPair(f *segmentindex.SegmentFile, offset *int,
	kis *[]segmentindex.KeyRedux, key1, key2 []byte, value1, value2 []value,
) error {
	skip, err := c.shouldSkipKey(key1, context.Background())
	if err != nil || skip {
		return err
	}

	needed := len(value1) + len(value2)
	if cap(c.mergedValues) < needed {
		c.mergedValues = make([]value, needed, int(float64(needed)*1.25))
	} else {
		c.mergedValues = c.mergedValues[:needed]
	}
	copy(c.mergedValues, value1)
	copy(c.mergedValues[len(value1):], value2)

	valuesMerged := c.setDecoder.DoPartial(c.mergedValues)
	if vals, skip := c.cleanupValues(valuesMerged); !skip {
		ki, err := c.writeIndividualNode(f, *offset, key2, vals)
		if err != nil {
			return errors.Wrap(err, "write individual node")
		}
		*offset = ki.ValueEnd
		*kis = append(*kis, ki)
	}
	return nil
}

func (c *compactorSet) processKey(f *segmentindex.SegmentFile, offset *int,
	kis *[]segmentindex.KeyRedux, key []byte, value []value,
) error {
	skip, err := c.shouldSkipKey(key, context.Background())
	if err != nil || skip {
		return err
	}

	if vals, skip := c.cleanupValues(value); !skip {
		ki, err := c.writeIndividualNode(f, *offset, key, vals)
		if err != nil {
			return errors.Wrap(err, "write individual node")
		}
		*offset = ki.ValueEnd
		*kis = append(*kis, ki)
	}
	return nil
}

func (c *compactorSet) writeIndividualNode(f *segmentindex.SegmentFile,
	offset int, key []byte, values []value,
) (segmentindex.KeyRedux, error) {
	// With reusable cursors, the key buffer is shared across iterations.
	// We must copy it before writing, as KeyIndexAndWriteTo may store
	// a reference. See: https://github.com/weaviate/weaviate/issues/3517
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	return (&segmentCollectionNode{
		values:     values,
		primaryKey: keyCopy,
		offset:     offset,
	}).KeyIndexAndWriteToRedux(f.BodyWriter(), c.writeBuf[:])
}

func (c *compactorSet) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.KeyRedux,
) error {
	_, err := segmentindex.MarshalSortedKeys(f.BodyWriter(), keys)
	return err
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

func (c *compactorSet) shouldSkipKey(key []byte, ctx context.Context) (bool, error) {
	if c.shouldSkipKeyFunc == nil {
		return false, nil
	}
	skip, err := c.shouldSkipKeyFunc(key, ctx)
	if err != nil {
		return false, errors.Wrap(err, "should skip key")
	}
	return skip, nil
}
