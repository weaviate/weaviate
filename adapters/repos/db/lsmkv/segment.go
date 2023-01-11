//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bytes"
	"fmt"
	"os"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/willf/bloom"
)

type segment struct {
	path                  string
	level                 uint16
	secondaryIndexCount   uint16
	version               uint16
	segmentStartPos       uint64
	segmentEndPos         uint64
	dataStartPos          uint64
	dataEndPos            uint64
	contents              []byte
	bloomFilter           *bloom.BloomFilter
	secondaryBloomFilters []*bloom.BloomFilter
	strategy              segmentindex.Strategy
	index                 diskIndex
	secondaryIndices      []diskIndex
	logger                logrus.FieldLogger
	metrics               *Metrics
	bloomFilterMetrics    *bloomFilterMetrics

	// the net addition this segment adds with respect to all previous segments
	countNetAdditions int
}

type diskIndex interface {
	// Get return segmentindex.NotFound in case no node can be found
	Get(key []byte) (segmentindex.Node, error)

	// Seek returns segmentindex.NotFound in case the seek value is larger than
	// the highest value in the collection, otherwise it returns the next highest
	// value (or the exact value if present)
	Seek(key []byte) (segmentindex.Node, error)

	// AllKeys in no specific order, e.g. for building a bloom filter
	AllKeys() ([][]byte, error)

	// Size of the index in bytes
	Size() int
}

func newSegment(path string, logger logrus.FieldLogger, metrics *Metrics,
	existsLower existsOnLowerSegmentsFn,
) (*segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}

	file_info, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file")
	}

	content, err := syscall.Mmap(int(file.Fd()), 0, int(file_info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file")
	}

	header, err := segmentindex.ParseHeader(bytes.NewReader(content[:segmentindex.HeaderSize]))
	if err != nil {
		return nil, errors.Wrap(err, "parse header")
	}

	switch header.Strategy {
	case segmentindex.StrategyReplace, segmentindex.StrategySetCollection,
		segmentindex.StrategyMapCollection, segmentindex.StrategyRoaringSet:
	default:
		return nil, errors.Errorf("unsupported strategy in segment")
	}

	primaryIndex, err := header.PrimaryIndex(content)
	if err != nil {
		return nil, errors.Wrap(err, "extract primary index position")
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	ind := &segment{
		level:               header.Level,
		path:                path,
		contents:            content,
		version:             header.Version,
		secondaryIndexCount: header.SecondaryIndices,
		segmentStartPos:     header.IndexStart,
		segmentEndPos:       uint64(len(content)),
		strategy:            header.Strategy,
		dataStartPos:        segmentindex.HeaderSize, // fixed value that's the same for all strategies
		dataEndPos:          header.IndexStart,
		index:               primaryDiskIndex,
		logger:              logger,
		metrics:             metrics,
		bloomFilterMetrics:  newBloomFilterMetrics(metrics),
	}

	if ind.secondaryIndexCount > 0 {
		ind.secondaryIndices = make([]diskIndex, ind.secondaryIndexCount)
		ind.secondaryBloomFilters = make([]*bloom.BloomFilter, ind.secondaryIndexCount)
		for i := range ind.secondaryIndices {
			secondary, err := header.SecondaryIndex(content, uint16(i))
			if err != nil {
				return nil, errors.Wrapf(err, "get position for secondary index at %d", i)
			}

			ind.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
			if err := ind.initSecondaryBloomFilter(i); err != nil {
				return nil, errors.Wrapf(err, "init bloom filter for secondary index at %d", i)
			}
		}
	}

	if err := ind.initBloomFilter(); err != nil {
		return nil, err
	}

	if err := ind.initCountNetAdditions(existsLower); err != nil {
		return nil, err
	}

	return ind, nil
}

func (ind *segment) close() error {
	return syscall.Munmap(ind.contents)
}

func (ind *segment) drop() error {
	// support for persisting bloom filters and cnas was added in v1.17,
	// therefore the files may not be present on segments created with previous
	// versions. By using RemoveAll, which does not error on NotExists, these
	// drop calls are backward-compatible:
	if err := os.RemoveAll(ind.bloomFilterPath()); err != nil {
		return fmt.Errorf("drop bloom filter: %w", err)
	}

	for i := 0; i < int(ind.secondaryIndexCount); i++ {
		if err := os.RemoveAll(ind.bloomFilterSecondaryPath(i)); err != nil {
			return fmt.Errorf("drop bloom filter: %w", err)
		}
	}

	if err := os.RemoveAll(ind.countNetPath()); err != nil {
		return fmt.Errorf("drop count net additions file: %w", err)
	}

	// for the segment itself, we're not using RemoveAll, but Remove. If there
	// was a NotExists error here, something would be seriously wrong and we
	// don't want to ignore it.
	if err := os.Remove(ind.path); err != nil {
		return fmt.Errorf("drop segment: %w", err)
	}

	return nil
}

// Size returns the total size of the segment in bytes, including the header
// and index
func (ind *segment) Size() int {
	return len(ind.contents)
}

// Payload Size is only the payload of the index, excluding the index
func (ind *segment) PayloadSize() int {
	return int(ind.dataEndPos)
}
