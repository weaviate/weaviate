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
	"errors"
	"fmt"
	"io"

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
	c1 *segmentCursorReplace
	c2 *segmentCursorReplace

	// the level matching those of the cursors
	currentLevel uint16
	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones   bool
	secondaryIndexCount uint16

	w                io.WriteSeeker
	bufw             compactor.Writer
	mw               *compactor.MemoryWriter
	scratchSpacePath string

	enableChecksumValidation bool
}

func newCompactorReplace(w io.WriteSeeker,
	c1, c2 *segmentCursorReplace, level, secondaryIndexCount uint16,
	scratchSpacePath string, cleanupTombstones bool,
	enableChecksumValidation bool, maxNewFileSize int64,
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

	return &compactorReplace{
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
	res1, err1 := c.c1.firstWithAllKeys()
	res2, err2 := c.c2.firstWithAllKeys()

	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize

	var kis []segmentindex.Key

	for {
		if res1.primaryKey == nil && res2.primaryKey == nil {
			break
		}
		if bytes.Equal(res1.primaryKey, res2.primaryKey) {
			if !(c.cleanupTombstones && errors.Is(err2, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res2.primaryKey, res2.value,
					res2.secondaryKeys, errors.Is(err2, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (equal keys): %w", err)
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			// advance both!
			res1, err1 = c.c1.nextWithAllKeys()
			res2, err2 = c.c2.nextWithAllKeys()
			continue
		}

		if (res1.primaryKey != nil && bytes.Compare(res1.primaryKey, res2.primaryKey) == -1) || res2.primaryKey == nil {
			// key 1 is smaller
			if !(c.cleanupTombstones && errors.Is(err1, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res1.primaryKey, res1.value,
					res1.secondaryKeys, errors.Is(err1, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (res1.primaryKey smaller)")
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			res1, err1 = c.c1.nextWithAllKeys()
		} else {
			// key 2 is smaller
			if !(c.cleanupTombstones && errors.Is(err2, lsmkv.Deleted)) {
				ki, err := c.writeIndividualNode(f, offset, res2.primaryKey, res2.value,
					res2.secondaryKeys, errors.Is(err2, lsmkv.Deleted))
				if err != nil {
					return nil, fmt.Errorf("write individual node (res2.primaryKey smaller): %w", err)
				}

				offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			res2, err2 = c.c2.nextWithAllKeys()
		}
	}

	return kis, nil
}

func (c *compactorReplace) writeIndividualNode(f *segmentindex.SegmentFile,
	offset int, key, value []byte, secondaryKeys [][]byte, tombstone bool,
) (segmentindex.Key, error) {
	segNode := segmentReplaceNode{
		offset:              offset,
		tombstone:           tombstone,
		value:               value,
		primaryKey:          key,
		secondaryIndexCount: c.secondaryIndexCount,
		secondaryKeys:       secondaryKeys,
	}
	return segNode.KeyIndexAndWriteTo(f.BodyWriter())
}

func (c *compactorReplace) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  StrategyReplace,
			"operation": "writeIndices",
		}),
	}
	_, err := f.WriteIndexes(indexes)
	return err
}
