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
	Get(key []byte) (segmentindex.Node, error)
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
	// elementCount := len(segmentBytes) / 32

	ind := &segment{
		contents:        content,
		segmentStartPos: indexStartPos,
		segmentEndPos:   uint64(len(content)),
		strategy:        strategy,
		dataStartPos:    8,
		dataEndPos:      indexStartPos,
		index:           diskIndex,
	}

	if err := ind.initBloomFilter(); err != nil {
		return nil, err
	}

	return ind, nil
}

func (i *segment) get(key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if !i.bloomFilter.Test(key) {
		return nil, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		return nil, err
	}

	return i.parseReplaceData(i.contents[node.Start:node.End])
}

func (i *segment) parseReplaceData(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		return nil, Deleted
	}

	return in[1:], nil
}

func (i *segment) getCollection(key []byte) ([]value, error) {
	if i.strategy != SegmentStrategySetCollection &&
		i.strategy != SegmentStrategyMapCollection {
		return nil, errors.Errorf("get only possible for strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	if !i.bloomFilter.Test(key) {
		return nil, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		return nil, err
	}

	return i.parseCollectionData(i.contents[node.Start:node.End])
}

func (i *segment) parseCollectionData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	r := bytes.NewReader(in)

	readSoFar := 0

	var valuesLen uint64
	if err := binary.Read(r, binary.LittleEndian, &valuesLen); err != nil {
		return nil, errors.Wrap(err, "read values len")
	}
	readSoFar += 8

	values := make([]value, valuesLen)
	for i := range values {
		if err := binary.Read(r, binary.LittleEndian, &values[i].tombstone); err != nil {
			return nil, errors.Wrap(err, "read value tombstone")
		}
		readSoFar += 1

		var valueLen uint64
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return nil, errors.Wrap(err, "read value len")
		}
		readSoFar += 8

		values[i].value = make([]byte, valueLen)
		n, err := r.Read(values[i].value)
		if err != nil {
			return nil, errors.Wrap(err, "read value")
		}
		readSoFar += n
	}

	if len(in) != readSoFar {
		return nil, errors.Errorf("corrupt collection segment: read %d bytes out of %d",
			readSoFar, len(in))
	}

	return values, nil
}

var (
	NotFound = errors.Errorf("not found")
	Deleted  = errors.Errorf("deleted")
)

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

	fmt.Printf("building bloom filter took %s\n", took)
	return nil
}
