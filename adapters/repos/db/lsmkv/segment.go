//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/willf/bloom"
)

var (
	NotFound = errors.Errorf("not found")
	Deleted  = errors.Errorf("deleted")
)

type segment struct {
	segmentStartPos uint64
	segmentEndPos   uint64
	dataStartPos    uint64
	dataEndPos      uint64
	contents        []byte
	bloomFilter     *bloom.BloomFilter
	strategy        SegmentStrategy
	index           diskIndex
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

func newSegment(path string) (*segment, error) {
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

	var strategy SegmentStrategy
	if err := binary.Read(bytes.NewReader(content[2:4]), binary.LittleEndian,
		&strategy); err != nil {
		return nil, err
	}

	switch strategy {
	case SegmentStrategyReplace, SegmentStrategySetCollection,
		SegmentStrategyMapCollection:
	default:
		return nil, errors.Errorf("unsupported strategy in segment")
	}

	var indexStartPos uint64
	if err := binary.Read(bytes.NewReader(content[4:12]), binary.LittleEndian,
		&indexStartPos); err != nil {
		return nil, err
	}

	diskIndex := segmentindex.NewDiskTree(content[indexStartPos:])

	ind := &segment{
		contents:        content,
		segmentStartPos: indexStartPos,
		segmentEndPos:   uint64(len(content)),
		strategy:        strategy,
		dataStartPos:    SegmentHeaderSize, // fixed value that's the same for all strategies
		dataEndPos:      indexStartPos,
		index:           diskIndex,
	}

	if err := ind.initBloomFilter(); err != nil {
		return nil, err
	}

	return ind, nil
}

func (ind *segment) initBloomFilter() error {
	before := time.Now()
	keys, err := ind.index.AllKeys()
	if err != nil {
		return err
	}

	fmt.Printf("new segment has %d keys\n", len(keys))

	ind.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.bloomFilter.Add(key)
	}
	took := time.Since(before)

	fmt.Printf("building bloom filter took %s\n", took)
	return nil
}
