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
	"errors"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type compactorReplace struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1, c2 *segmentCursorReplaceReusable

	// the level matching those of the cursors
	currentLevel uint16
	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones   bool
	secondaryIndexCount uint16

	w    io.WriteSeeker
	bufw compactor.Writer
	mw   *compactor.MemoryWriter

	allocChecker   memwatch.AllocChecker
	maxNewFileSize int64

	enableChecksumValidation bool

	// arena holds stable copies of key bytes so that ki.Key / ki.SecondaryKeys
	// stored in the kis slice remain valid after the reusable cursor advances.
	arena keyArena

	writeBuf [9]byte // reused by writeIndividualNode to avoid per-key allocation

	// primaryIndexSize and secIndexSizes are accumulated during writeKeys so
	// that writeDirectly does not need a second O(N) scan over the keys.
	primaryIndexSize int64
	secIndexSizes    []int64
}

func newCompactorReplace(w io.WriteSeeker,
	c1, c2 *segmentCursorReplaceReusable, level, secondaryIndexCount uint16,
	cleanupTombstones bool,
	enableChecksumValidation bool, maxNewFileSize int64, allocChecker memwatch.AllocChecker,
) *compactorReplace {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  StrategyReplace,
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(w, writeCB)
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	var secIndexSizes []int64
	if secondaryIndexCount > 0 {
		secIndexSizes = make([]int64, secondaryIndexCount)
	}

	return &compactorReplace{
		c1:                       c1,
		c2:                       c2,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupTombstones:        cleanupTombstones,
		secondaryIndexCount:      secondaryIndexCount,
		enableChecksumValidation: enableChecksumValidation,
		allocChecker:             allocChecker,
		maxNewFileSize:           maxNewFileSize,
		secIndexSizes:            secIndexSizes,
	}
}

func (c *compactorReplace) do() error {
	if err := c.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)

	kis, err := c.writeKeys(segmentFile)
	if err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	if err := c.writeIndexes(segmentFile, kis); err != nil {
		return fmt.Errorf("write indices: %w", err)
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
		c.secondaryIndexCount, dataEnd, segmentindex.StrategyReplace); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorReplace segment checksum: %w", err)
	}

	return nil
}

func (c *compactorReplace) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return fmt.Errorf("write empty header: %w", err)
	}

	return nil
}

func (c *compactorReplace) writeKeys(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	res1, err1 := c.c1.first()
	res2, err2 := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize

	kis := make([]segmentindex.Key, 0, c.c1.keyCount()+c.c2.keyCount())

	for {
		var key1, key2 []byte
		if res1 != nil {
			key1 = res1.primaryKey
		}
		if res2 != nil {
			key2 = res2.primaryKey
		}

		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			if !(c.cleanupTombstones && errors.Is(err2, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res2.primaryKey, res2.value,
					res2.secondaryKeys, errors.Is(err2, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (equal keys): %w", err)
				}

				offset = ki.ValueEnd
				c.accumulateIndexSizes(ki)
				kis = append(kis, ki)
			}
			// advance both!
			res1, err1 = c.c1.next()
			res2, err2 = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			if !(c.cleanupTombstones && errors.Is(err1, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res1.primaryKey, res1.value,
					res1.secondaryKeys, errors.Is(err1, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (res1.primaryKey smaller): %w", err)
				}

				offset = ki.ValueEnd
				c.accumulateIndexSizes(ki)
				kis = append(kis, ki)
			}
			res1, err1 = c.c1.next()
		} else {
			// key 2 is smaller
			if !(c.cleanupTombstones && errors.Is(err2, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res2.primaryKey, res2.value,
					res2.secondaryKeys, errors.Is(err2, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (res2.primaryKey smaller): %w", err)
				}

				offset = ki.ValueEnd
				c.accumulateIndexSizes(ki)
				kis = append(kis, ki)
			}
			res2, err2 = c.c2.next()
		}
	}

	return kis, nil
}

// TREE_KEY_STORE_OVERHEAD + len(key) is the overhead of storing a key in the index:
//   - 4 bytes for the length of the key
//   - len(key) bytes for the key itself
//   - 8 bytes for the node start pos (points to matching file offset in the Strategy specific data structure)
//   - 8 bytes for the node end pos (points to matching file offset in the Strategy specific data structure)
//   - 8 bytes for the left offset (offset of the left node)
//   - 8 bytes for the right offset (offset of the right node)
func (c *compactorReplace) accumulateIndexSizes(ki segmentindex.Key) {
	c.primaryIndexSize += int64(segmentindex.TREE_KEY_STORE_OVERHEAD + len(ki.Key))
	for pos, sk := range ki.SecondaryKeys {
		c.secIndexSizes[pos] += int64(segmentindex.TREE_KEY_STORE_OVERHEAD + len(sk))
	}
}

func (c *compactorReplace) writeIndividualNode(f *segmentindex.SegmentFile,
	offset int, key, value []byte, secondaryKeys [][]byte, tombstone bool,
) (segmentindex.Key, error) {
	// Copy key bytes into stable arena memory. The reusable cursor reuses its
	// internal buffers on every next() call, so ki.Key / ki.SecondaryKeys stored
	// in the kis slice would otherwise be corrupted on the next iteration.
	keyCopy := c.arena.CopyKey(key)

	var secKeysCopy [][]byte
	if len(secondaryKeys) > 0 {
		secKeysCopy = make([][]byte, len(secondaryKeys))
		for i, sk := range secondaryKeys {
			secKeysCopy[i] = c.arena.CopyKey(sk)
		}
	}

	// value is NOT arena-copied because it is written synchronously to the
	// buffered writer by KeyIndexAndWriteToWithBuf. The bufio.Writer.Write
	// call copies value bytes into its internal buffer before returning,
	// so the reusable cursor's buffer can safely be reused on the next
	// iteration.
	segNode := segmentReplaceNode{
		offset:              offset,
		tombstone:           tombstone,
		value:               value,
		primaryKey:          keyCopy,
		secondaryIndexCount: c.secondaryIndexCount,
		secondaryKeys:       secKeysCopy,
	}
	return segNode.KeyIndexAndWriteToWithBuf(f.BodyWriter(), c.writeBuf[:])
}

func (c *compactorReplace) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                           keys,
		SecondaryIndexCount:            c.secondaryIndexCount,
		AllocChecker:                   c.allocChecker,
		SizesPrecomputed:               true,
		PrecomputedPrimaryIndexSize:    c.primaryIndexSize,
		PrecomputedSecondaryIndexSizes: c.secIndexSizes,
	}
	_, err := f.WriteIndexes(indexes)
	return err
}
