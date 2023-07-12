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
	contentFile           *os.File
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
	// Get return lsmkv.NotFound in case no node can be found
	Get(key []byte) (segmentindex.Node, error)

	// Seek returns lsmkv.NotFound in case the seek value is larger than
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
		return nil, fmt.Errorf("open file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	contents := make([]byte, fileInfo.Size())
	n, err := file.Read(contents)
	if err != nil {
		return nil, fmt.Errorf("read segment file %q: %w", fileInfo.Name(), err)
	}
	if int64(n) != fileInfo.Size() {
		logger.WithField("action", "read segment file").
			Warnf("only read %d out of %d segment bytes", n, fileInfo.Size())
	}

	header, err := segmentindex.ParseHeader(bytes.NewReader(contents[:segmentindex.HeaderSize]))
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	switch header.Strategy {
	case segmentindex.StrategyReplace, segmentindex.StrategySetCollection,
		segmentindex.StrategyMapCollection, segmentindex.StrategyRoaringSet:
	default:
		return nil, fmt.Errorf("unsupported strategy in segment")
	}

	primaryIndex, err := header.PrimaryIndex(contents)
	if err != nil {
		return nil, fmt.Errorf("extract primary index position: %w", err)
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	ind := &segment{
		level:               header.Level,
		path:                path,
		contentFile:         file,
		version:             header.Version,
		secondaryIndexCount: header.SecondaryIndices,
		segmentStartPos:     header.IndexStart,
		segmentEndPos:       uint64(len(contents)),
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
			secondary, err := header.SecondaryIndex(contents, uint16(i))
			if err != nil {
				return nil, fmt.Errorf("get position for secondary index at %d: %w", i, err)
			}

			ind.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
			if err := ind.initSecondaryBloomFilter(i); err != nil {
				return nil, fmt.Errorf("init bloom filter for secondary index at %d: %w", i, err)
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

func (s *segment) close() error {
	if s.contentFile != nil {
		if err := s.contentFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *segment) drop() error {
	// support for persisting bloom filters and cnas was added in v1.17,
	// therefore the files may not be present on segments created with previous
	// versions. By using RemoveAll, which does not error on NotExists, these
	// drop calls are backward-compatible:
	if err := os.RemoveAll(s.bloomFilterPath()); err != nil {
		return fmt.Errorf("drop bloom filter: %w", err)
	}

	for i := 0; i < int(s.secondaryIndexCount); i++ {
		if err := os.RemoveAll(s.bloomFilterSecondaryPath(i)); err != nil {
			return fmt.Errorf("drop bloom filter: %w", err)
		}
	}

	if err := os.RemoveAll(s.countNetPath()); err != nil {
		return fmt.Errorf("drop count net additions file: %w", err)
	}

	// for the segment itself, we're not using RemoveAll, but Remove. If there
	// was a NotExists error here, something would be seriously wrong and we
	// don't want to ignore it.
	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("drop segment: %w", err)
	}

	return nil
}

// Size returns the total size of the segment in bytes, including the header
// and index
func (s *segment) Size() int {
	stat, err := s.contentFile.Stat()
	if err != nil {
		s.logger.WithField("action", "stat segment file").
			Error(err.Error())
		return 0
	}
	return int(stat.Size())
}

// Payload Size is only the payload of the index, excluding the index
func (s *segment) PayloadSize() int {
	return int(s.dataEndPos)
}

func (s *segment) pread(buf []byte, start, end uint64) error {
	n, err := s.contentFile.ReadAt(buf, int64(start))
	if n != int(end-start) {
		s.logger.WithField("action", "pread").
			Warnf("only read %d out of %d segment bytes", n, end-start)
	}
	return err
}
