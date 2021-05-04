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
	// elementCount := len(segmentBytes) / 32

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

	return i.replaceStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) replaceStratParseData(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		return nil, Deleted
	}

	r := bytes.NewReader(in[1:])
	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return nil, errors.Wrap(err, "read value length encoding")
	}

	data := make([]byte, valueLength)
	if _, err := r.Read(data); err != nil {
		return nil, errors.Wrap(err, "read value")
	}

	return data, nil
}

type segmentParseResult struct {
	key   []byte
	value []byte
	read  int // so that the cursor and calculate its offset for the next round
}

func (i *segment) replaceStratParseDataWithKey(in []byte) (segmentParseResult, error) {
	out := segmentParseResult{}

	if len(in) == 0 {
		return out, NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		return out, Deleted
	}
	out.read += 1

	r := bytes.NewReader(in[1:])
	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return out, errors.Wrap(err, "read value length encoding")
	}
	out.read += 8

	out.value = make([]byte, valueLength)
	if n, err := r.Read(out.value); err != nil {
		return out, errors.Wrap(err, "read value")
	} else {
		out.read += n
	}

	var keyLength uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLength); err != nil {
		return out, errors.Wrap(err, "read key length encoding")
	}
	out.read += 4

	out.key = make([]byte, keyLength)
	if n, err := r.Read(out.key); err != nil {
		return out, errors.Wrap(err, "read key")
	} else {
		out.read += n
	}

	return out, nil
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

	return i.collectionStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) collectionStratParseData(in []byte) ([]value, error) {
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
