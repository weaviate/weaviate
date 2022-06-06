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

package lsmkv

import (
	"bytes"
	"os"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/sirupsen/logrus"
	"github.com/willf/bloom"
)

var (
	NotFound = errors.Errorf("not found")
	Deleted  = errors.Errorf("deleted")
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
	strategy              SegmentStrategy
	index                 diskIndex
	secondaryIndices      []diskIndex
	logger                logrus.FieldLogger
	metrics               *Metrics

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
}

func newSegment(path string, logger logrus.FieldLogger, metrics *Metrics,
	existsLower existsOnLowerSegmentsFn) (*segment, error) {
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

	header, err := parseSegmentHeader(bytes.NewReader(content[:SegmentHeaderSize]))
	if err != nil {
		return nil, errors.Wrap(err, "parse header")
	}

	switch header.strategy {
	case SegmentStrategyReplace, SegmentStrategySetCollection,
		SegmentStrategyMapCollection:
	default:
		return nil, errors.Errorf("unsupported strategy in segment")
	}

	primaryIndex, err := header.PrimaryIndex(content)
	if err != nil {
		return nil, errors.Wrap(err, "extract primary index position")
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	ind := &segment{
		level:               header.level,
		path:                path,
		contents:            content,
		version:             header.version,
		secondaryIndexCount: header.secondaryIndices,
		segmentStartPos:     header.indexStart,
		segmentEndPos:       uint64(len(content)),
		strategy:            header.strategy,
		dataStartPos:        SegmentHeaderSize, // fixed value that's the same for all strategies
		dataEndPos:          header.indexStart,
		index:               primaryDiskIndex,
		logger:              logger,
		metrics:             metrics,
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

// existOnLowerSegments is a simple function that can be passed at segment
// initialization time to check if any of the keys are truly new or previously
// seen. This can in turn be used to build up the net count additions. The
// reason this is abstrate:
type existsOnLowerSegmentsFn func(key []byte) (bool, error)

func (ind *segment) initCountNetAdditions(exists existsOnLowerSegmentsFn) error {
	if ind.strategy != SegmentStrategyReplace {
		// replace is the only strategy that supports counting
		return nil
	}

	// before := time.Now()
	// defer func() {
	// 	fmt.Printf("init count net additions took %s\n", time.Since(before))
	// }()

	var lastErr error
	netCount := 0
	cb := func(key []byte, tombstone bool) {
		existedOnPrior, err := exists(key)
		if err != nil {
			lastErr = err
		}

		if tombstone && existedOnPrior {
			netCount--
		}

		if !tombstone && !existedOnPrior {
			netCount++
		}
	}

	extr := newBufferedKeyAndTombstoneExtractor(ind.contents, ind.dataStartPos,
		ind.dataEndPos, 10e6, ind.secondaryIndexCount, cb)

	extr.do()

	ind.countNetAdditions = netCount

	return lastErr
}

func (ind *segment) initBloomFilter() error {
	before := time.Now()
	keys, err := ind.index.AllKeys()
	if err != nil {
		return err
	}

	ind.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.bloomFilter.Add(key)
	}

	took := time.Since(before)
	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_primary").
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) initSecondaryBloomFilter(pos int) error {
	before := time.Now()
	keys, err := ind.secondaryIndices[pos].AllKeys()
	if err != nil {
		return err
	}

	ind.secondaryBloomFilters[pos] = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.secondaryBloomFilters[pos].Add(key)
	}
	took := time.Since(before)

	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) close() error {
	return syscall.Munmap(ind.contents)
}

func (ind *segment) drop() error {
	return os.Remove(ind.path)
}
