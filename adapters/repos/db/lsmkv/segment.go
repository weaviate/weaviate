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
	"os"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/contentReader"

	"github.com/edsrzf/mmap-go"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/willf/bloom"
)

type segment struct {
	path                string
	level               uint16
	secondaryIndexCount uint16
	version             uint16
	segmentStartPos     uint64
	segmentEndPos       uint64
	dataStartPos        uint64
	dataEndPos          uint64
	strategy            segmentindex.Strategy
	index               diskIndex
	secondaryIndices    []diskIndex
	logger              logrus.FieldLogger
	metrics             *Metrics
	size                int64

	useBloomFilter        bool // see bucket for more datails
	bloomFilter           *bloom.BloomFilter
	secondaryBloomFilters []*bloom.BloomFilter
	bloomFilterMetrics    *bloomFilterMetrics

	// the net addition this segment adds with respect to all previous segments
	calcCountNetAdditions bool // see bucket for more datails
	countNetAdditions     int

	contentReader contentReader.ContentReader
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
	existsLower existsOnLowerSegmentsFn, mmapContents bool,
	useBloomFilter bool, calcCountNetAdditions bool, overwriteDerived bool,
) (*segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	var contReader contentReader.ContentReader
	if mmapContents {
		contents, err := mmap.MapRegion(file, int(fileInfo.Size()), mmap.RDONLY, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("mmap file: %w", err)
		}
		contReader = contentReader.NewMMap(contents)
	} else {
		contReader = contentReader.NewPread(file, uint64(fileInfo.Size()))
	}

	headerByte, _ := contReader.ReadRange(0, segmentindex.HeaderSize, nil)
	header, err := segmentindex.ParseHeader(bytes.NewReader(headerByte))
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	switch header.Strategy {
	case segmentindex.StrategyReplace, segmentindex.StrategySetCollection,
		segmentindex.StrategyMapCollection, segmentindex.StrategyRoaringSet:
	default:
		return nil, fmt.Errorf("unsupported strategy in segment")
	}

	primaryIndex, err := header.PrimaryIndex(contReader)
	if err != nil {
		return nil, fmt.Errorf("extract primary index position: %w", err)
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	seg := &segment{
		level:                 header.Level,
		path:                  path,
		version:               header.Version,
		secondaryIndexCount:   header.SecondaryIndices,
		segmentStartPos:       header.IndexStart,
		segmentEndPos:         uint64(fileInfo.Size()),
		strategy:              header.Strategy,
		dataStartPos:          segmentindex.HeaderSize, // fixed value that's the same for all strategies
		dataEndPos:            header.IndexStart,
		index:                 primaryDiskIndex,
		logger:                logger,
		metrics:               metrics,
		size:                  fileInfo.Size(),
		useBloomFilter:        useBloomFilter,
		calcCountNetAdditions: calcCountNetAdditions,
		contentReader:         contReader,
	}

	// Using pread strategy requires file to remain open for segment lifetime
	if mmapContents {
		defer file.Close()
	}

	if seg.secondaryIndexCount > 0 {
		seg.secondaryIndices = make([]diskIndex, seg.secondaryIndexCount)
		for i := range seg.secondaryIndices {
			secondary, err := header.SecondaryIndex(contReader, uint16(i))
			if err != nil {
				return nil, fmt.Errorf("get position for secondary index at %d: %w", i, err)
			}
			seg.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
		}
	}

	if seg.useBloomFilter {
		if err := seg.initBloomFilters(metrics, overwriteDerived); err != nil {
			return nil, err
		}
	}
	if seg.calcCountNetAdditions {
		if err := seg.initCountNetAdditions(existsLower, overwriteDerived); err != nil {
			return nil, err
		}
	}

	return seg, nil
}

func (s *segment) close() error {
	return s.contentReader.Close()
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
	// was a NotExists error here, something would be seriously wrong, and we
	// don't want to ignore it.
	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("drop segment: %w", err)
	}

	return nil
}

// Size returns the total size of the segment in bytes, including the header
// and index
func (s *segment) Size() int {
	return int(s.size)
}

// PayloadSize is only the payload of the index, excluding the index
func (s *segment) PayloadSize() int {
	return int(s.dataEndPos)
}

type nodeReader struct {
	r io.Reader
}

func (n *nodeReader) Read(b []byte) (int, error) {
	return n.r.Read(b)
}

type nodeOffset struct {
	start, end uint64
}

func (s *segment) newNodeReader(offset nodeOffset) (*nodeReader, error) {
	return &nodeReader{r: s.contentReader.ReaderFromOffset(offset.start, offset.end)}, nil
}

func (s *segment) copyNode(b []byte, offset nodeOffset) error {
	_, _ = s.contentReader.ReadRange(offset.start, offset.end-offset.start, b)
	return nil
}
