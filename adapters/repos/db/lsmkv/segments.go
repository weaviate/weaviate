package lsmkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"github.com/willf/bloom"
)

type segment struct {
	segmentStartPos uint64
	segmentEndPos   uint64
	segmentCount    uint64
	dataStartPos    uint64
	dataEndPos      uint64
	contents        []byte // in mem for now, mmapped later
	bloomFilter     *bloom.BloomFilter
	strategy        SegmentStrategy
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
	case SegmentStrategyReplace, SegmentStrategySetCollection:
	default:
		return nil, errors.Errorf("unsupported strategy in segment")
	}

	var pos uint64
	if err := binary.Read(bytes.NewReader(content[4:12]), binary.LittleEndian,
		&pos); err != nil {
		return nil, err
	}
	segmentBytes := content[pos:]
	if len(segmentBytes)%32 != 0 {
		return nil, errors.Errorf("corrupt segment with len %d", len(segmentBytes))
	}

	elementCount := len(segmentBytes) / 32

	ind := &segment{
		contents:        content,
		segmentStartPos: pos,
		segmentEndPos:   uint64(len(content)),
		segmentCount:    uint64(elementCount),
		strategy:        strategy,
		dataStartPos:    8,
		dataEndPos:      pos,
	}

	ind.initBloomFilter()

	return ind, nil
}

func (i *segment) get(key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	hasher := murmur3.New128()
	hasher.Write(key)
	keyHash := hasher.Sum(nil)

	if !i.bloomFilter.Test(keyHash) {
		return nil, NotFound
	}

	start, end, err := i.segmentBinarySearch(keyHash)
	if err != nil {
		return nil, err
	}

	return i.parseReplaceData(i.contents[start:end])
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
	if i.strategy != SegmentStrategySetCollection {
		return nil, errors.Errorf("get only possible for strategy %q",
			StrategySetCollection)
	}

	hasher := murmur3.New128()
	hasher.Write(key)
	keyHash := hasher.Sum(nil)

	if !i.bloomFilter.Test(keyHash) {
		return nil, NotFound
	}

	start, end, err := i.segmentBinarySearch(keyHash)
	if err != nil {
		return nil, err
	}

	return i.parseCollectionData(i.contents[start:end])
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

		if values[i].tombstone {
			continue
		}

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

func (i *segment) segmentBinarySearch(needle []byte) (uint64, uint64, error) {
	low := uint64(0)
	high := i.segmentCount - 1

	for {
		mid := (high-low)/2 + low
		start := i.segmentStartPos + (mid * 32)
		end := start + 16
		hay := i.contents[start:end]

		res := bytes.Compare(hay, needle)
		if res == 0 {
			return i.decodeStartEnd(i.contents[end : end+16])
		}

		if high == low {
			return 0, 0, NotFound
		}

		if res > 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
}

func (i *segment) decodeStartEnd(in []byte) (uint64, uint64, error) {
	tuple := make([]uint64, 2)
	err := binary.Read(bytes.NewReader(in), binary.LittleEndian, &tuple)
	if err != nil {
		return 0, 0, err
	}

	return tuple[0], tuple[1], nil
}

func (ind *segment) initBloomFilter() {
	before := time.Now()
	ind.bloomFilter = bloom.NewWithEstimates(uint(ind.segmentCount), 0.001)
	for i := uint64(0); i < ind.segmentCount; i++ {
		start := ind.segmentStartPos + (i * 32)
		end := start + 16
		ind.bloomFilter.Add(ind.contents[start:end])
	}
	took := time.Since(before)

	fmt.Printf("building bloom filter took %s\n", took)
}
