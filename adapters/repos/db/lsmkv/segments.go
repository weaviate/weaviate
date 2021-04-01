package lsmkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"github.com/willf/bloom"
)

type SegmentGroup struct {
	segments []*segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
}

func newSegmentGroup(dir string) (*SegmentGroup, error) {
	list, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	out := &SegmentGroup{
		segments: make([]*segment, len(list)),
	}

	for i, fileInfo := range list {
		segment, err := newSegment(filepath.Join(dir, fileInfo.Name()))
		if err != nil {
			return nil, errors.Wrapf(err, "init segment %s", fileInfo.Name())
		}

		out.segments[i] = segment
	}

	return out, nil
}

func (ig *SegmentGroup) add(path string) error {
	ig.maintenanceLock.Lock()
	defer ig.maintenanceLock.Unlock()

	segment, err := newSegment(path)
	if err != nil {
		return errors.Wrapf(err, "init segment %s", path)
	}

	ig.segments = append(ig.segments, segment)
	return nil
}

func (ig *SegmentGroup) get(key []byte) ([]byte, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	// TODO: instead of returning first match, return latest
	for _, segment := range ig.segments {
		v, err := segment.get(key)
		if err != nil {
			if err == NotFound {
				continue
			}
		}

		return v, nil
	}

	return nil, nil
}

type segment struct {
	segmentStartPos uint64
	segmentEndPos   uint64
	segmentCount    uint64
	dataStartPos    uint64
	dataEndPos      uint64
	contents        []byte // in mem for now, mmapped later
	bloomFilter     *bloom.BloomFilter
}

func (i *segment) get(key []byte) ([]byte, error) {
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

	return i.contents[start:end], nil
}

var NotFound = errors.Errorf("not found")

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

	var pos uint64
	binary.Read(bytes.NewReader(content[2:10]), binary.LittleEndian, &pos)
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
		dataStartPos:    8,
		dataEndPos:      pos,
	}

	ind.initBloomFilter()

	return ind, nil
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
