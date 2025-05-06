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
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type compactorMap struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1 *segmentCursorCollectionReusable
	c2 *segmentCursorCollectionReusable

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

	scratchSpacePath string

	// for backward-compatibility with states where the disk state for maps was
	// not guaranteed to be sorted yet
	requiresSorting bool

	enableChecksumValidation bool
}

func newCompactorMapCollection(w io.WriteSeeker,
	c1, c2 *segmentCursorCollectionReusable, level, secondaryIndexCount uint16,
	scratchSpacePath string, requiresSorting bool, cleanupTombstones bool,
	enableChecksumValidation bool, maxNewFileSize int64,
) *compactorMap {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  StrategyMapCollection,
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(w, writeCB)
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	return &compactorMap{
		c1:                       c1,
		c2:                       c2,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupTombstones:        cleanupTombstones,
		secondaryIndexCount:      secondaryIndexCount,
		scratchSpacePath:         scratchSpacePath,
		requiresSorting:          requiresSorting,
		enableChecksumValidation: enableChecksumValidation,
	}
}

func (c *compactorMap) do() error {
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
		c.secondaryIndexCount, dataEnd, segmentindex.StrategyMapCollection); err != nil {
		return errors.Wrap(err, "write header")
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorMap segment checksum: %w", err)
	}

	return nil
}

func (c *compactorMap) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

func (c *compactorMap) writeKeys(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize

	var kis []segmentindex.Key
	pairs := newReusableMapPairs()
	me := newMapEncoder()
	ssm := newSortedMapMerger()

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			pairs.ResizeLeft(len(value1))
			pairs.ResizeRight(len(value2))

			for i, v := range value1 {
				if err := pairs.left[i].FromBytes(v.value, false); err != nil {
					return nil, err
				}
				pairs.left[i].Tombstone = v.tombstone
			}

			for i, v := range value2 {
				if err := pairs.right[i].FromBytes(v.value, false); err != nil {
					return nil, err
				}
				pairs.right[i].Tombstone = v.tombstone
			}

			if c.requiresSorting {
				sort.Slice(pairs.left, func(a, b int) bool {
					return bytes.Compare(pairs.left[a].Key, pairs.left[b].Key) < 0
				})
				sort.Slice(pairs.right, func(a, b int) bool {
					return bytes.Compare(pairs.right[a].Key, pairs.right[b].Key) < 0
				})
			}

			ssm.reset([][]MapPair{pairs.left, pairs.right})
			mergedPairs, err := ssm.
				doKeepTombstonesReusable()
			if err != nil {
				return nil, err
			}

			mergedEncoded, err := me.DoMultiReusable(mergedPairs)
			if err != nil {
				return nil, err
			}

			if values, skip := c.cleanupValues(mergedEncoded); !skip {
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

func (c *compactorMap) writeIndividualNode(f *segmentindex.SegmentFile,
	offset int, key []byte, values []value,
) (segmentindex.Key, error) {
	// NOTE: There are no guarantees in the cursor logic that any memory is valid
	// for more than a single iteration. Every time you call next() to advance
	// the cursor, any memory might be reused.
	//
	// This includes the key buffer which was the cause of
	// https://github.com/weaviate/weaviate/issues/3517
	//
	// A previous logic created a new assignment in each iteration, but thatwas
	// not an explicit guarantee. A change in v1.21 (for pread/mmap) added a
	// reusable buffer for the key which surfaced this bug.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	return segmentCollectionNode{
		values:     values,
		primaryKey: keyCopy,
		offset:     offset,
	}.KeyIndexAndWriteTo(f.BodyWriter())
}

func (c *compactorMap) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  StrategyMapCollection,
			"operation": "writeIndices",
		}),
	}
	_, err := f.WriteIndexes(indexes)
	return err
}

// Removes values with tombstone set from input slice. Output slice may be smaller than input one.
// Returned skip of true means there are no values left (key can be omitted in segment)
// WARN: method can alter input slice by swapping its elements and reducing length (not capacity)
func (c *compactorMap) cleanupValues(values []value) (vals []value, skip bool) {
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
			// populating slice `values[i].value` would overwrite slice `values[last].value`.
			// Swaps makes sure `values[i].value` and `values[last].value` point to different slices.
			values[last], values[i] = values[i], values[last]
			last++
		}
	}

	if last == 0 {
		return nil, true
	}
	return values[:last], false
}
