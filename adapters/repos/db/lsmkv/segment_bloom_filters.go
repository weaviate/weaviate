//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (s *segment) buildPath(template string) string {
	isTmpFile := filepath.Ext(s.path) == ".tmp"

	extless := strings.TrimSuffix(s.path, filepath.Ext(s.path))
	if isTmpFile { // remove second extension
		extless = strings.TrimSuffix(extless, filepath.Ext(extless))
	}

	path := fmt.Sprintf(template, extless)
	if isTmpFile {
		path = fmt.Sprintf("%s.tmp", path)
	}
	return path
}

func (s *segment) bloomFilterPath() string {
	return s.buildPath("%s.bloom")
}

func (s *segment) bloomFilterSecondaryPath(pos int) string {
	posTemplate := fmt.Sprintf(".%d.bloom", pos)
	return s.buildPath("%s.secondary" + posTemplate)
}

func (s *segment) initBloomFilters(metrics *Metrics, overwrite bool, existingFilesList map[string]int64) error {
	if err := s.initBloomFilter(overwrite, existingFilesList); err != nil {
		return fmt.Errorf("init bloom filter for primary index: %w", err)
	}
	if s.secondaryIndexCount > 0 {
		s.secondaryBloomFilters = make([]*bloom.BloomFilter, s.secondaryIndexCount)
		for i := range s.secondaryBloomFilters {
			if err := s.initSecondaryBloomFilter(i, overwrite, existingFilesList); err != nil {
				return fmt.Errorf("init bloom filter for secondary index at %d: %w", i, err)
			}
		}
	}
	return nil
}

func (s *segment) initBloomFilter(overwrite bool, existingFilesList map[string]int64) error {
	path := s.bloomFilterPath()
	s.metaPaths = append(s.metaPaths, path)

	loadFromDisk, err := fileExistsInList(existingFilesList, filepath.Base(path))
	if err != nil {
		return err
	}
	if loadFromDisk {
		if overwrite {
			err := os.Remove(path)
			if err != nil {
				return fmt.Errorf("delete existing bloom filter %s: %w", path, err)
			}
		} else {
			err = s.loadBloomFilterFromDisk()
			if err == nil {
				return nil
			}

			if !canRecomputeSidecar(err) {
				return err
			}

			// now continue re-calculating
		}
	}

	before := time.Now()

	if err := s.computeAndStoreBloomFilter(path); err != nil {
		return err
	}

	took := time.Since(before)

	s.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_primary").
		WithField("path", s.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)

	return nil
}

func (s *segment) computeAndStoreBloomFilter(path string) error {
	count := s.index.KeyCount()
	s.bloomFilter = bloom.NewWithEstimates(uint(count), 0.001)
	s.index.ForEachKey(func(key []byte) {
		s.bloomFilter.Add(key)
	})

	if err := s.storeBloomFilterOnDisk(path); err != nil {
		return fmt.Errorf("store bloom filter on disk: %w", err)
	}

	return nil
}

func (s *segment) storeBloomFilterOnDisk(path string) error {
	bfSize := getBloomFilterSize(s.bloomFilter)

	rw := byteops.NewReadWriter(make([]byte, bfSize+byteops.Uint32Len))
	rw.MoveBufferPositionForward(byteops.Uint32Len) // leave space for checksum
	_, err := s.bloomFilter.WriteTo(&rw)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(rw, path, s.observeMetaWrite)
}

func (s *segment) loadBloomFilterFromDisk() error {
	data, err := loadWithChecksum(s.bloomFilterPath(), -1, s.metrics.ReadObserver("loadBloomfilter"))
	if err != nil {
		return err
	}

	s.bloomFilter = new(bloom.BloomFilter)
	_, err = s.bloomFilter.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("read bloom filter from disk: %w", err)
	}

	return nil
}

// getBloomFilter returns nil if the segment has none. Shared with every
// concurrent reader, so callers must not mutate it.
func (s *segment) getBloomFilter() *bloom.BloomFilter {
	return s.bloomFilter
}

// getKeysSorted returns the primary index's keys ascending. They alias the
// segment's data, so they are valid only while it is pinned.
func (s *segment) getKeysSorted() [][]byte {
	keys := make([][]byte, 0, s.index.KeyCount())
	s.index.ForEachKey(func(key []byte) {
		keys = append(keys, key)
	})
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

// combineBloomFilters returns an unsaturated, mutable copy of the
// largest-estimating filter it can build from the pinned segments: a union per
// (m, k) geometry — Merge rejects mismatched ones — or a single segment's
// filter. Nil if no segment carries a filter; segments without one contribute
// their keys to exact instead.
func combineBloomFilters(segments []Segment, exact *exactKeys) *bloom.BloomFilter {
	type geometry struct{ m, k uint }
	var (
		unions           map[geometry]*bloom.BloomFilter
		largestSingle    *bloom.BloomFilter
		largestSingleEst uint32
	)
	for _, seg := range segments {
		bf := seg.getBloomFilter()
		if bf == nil {
			exact.add(seg.getKeysSorted())
			continue
		}
		if bloomSaturated(bf) {
			// sized for its own key count, so this should not happen; skip
			// rather than poison a union
			continue
		}
		if est := bf.ApproximatedSize(); largestSingle == nil || est > largestSingleEst {
			largestSingle, largestSingleEst = bf, est
		}
		g := geometry{m: bf.Cap(), k: bf.K()}
		if u, ok := unions[g]; ok {
			_ = u.Merge(bf) // equal geometry, cannot fail
		} else {
			if unions == nil {
				unions = map[geometry]*bloom.BloomFilter{}
			}
			unions[g] = bf.Copy()
		}
	}

	var (
		best    *bloom.BloomFilter
		bestEst uint32
	)
	for _, u := range unions {
		if bloomSaturated(u) {
			continue
		}
		if est := u.ApproximatedSize(); best == nil || est > bestEst {
			best, bestEst = u, est
		}
	}
	// a union bounds only its own geometry, so a surviving small union must not
	// displace a larger single filter whose union saturated
	if largestSingle != nil && (best == nil || largestSingleEst > bestEst) {
		return largestSingle.Copy()
	}
	return best
}

func (s *segment) initSecondaryBloomFilter(pos int, overwrite bool, existingFilesList map[string]int64) error {
	before := time.Now()

	path := s.bloomFilterSecondaryPath(pos)
	s.metaPaths = append(s.metaPaths, path)

	loadFromDisk, err := fileExistsInList(existingFilesList, filepath.Base(path))
	if err != nil {
		return err
	}
	if loadFromDisk {
		if overwrite {
			err := os.Remove(path)
			if err != nil {
				return fmt.Errorf("deleting existing secondary bloom filter %s: %w", path, err)
			}
		} else {
			err = s.loadBloomFilterSecondaryFromDisk(pos)
			if err == nil {
				return nil
			}

			if !canRecomputeSidecar(err) {
				return err
			}

			// now continue re-calculating
		}
	}

	if err := s.computeAndStoreSecondaryBloomFilter(path, pos); err != nil {
		return err
	}

	took := time.Since(before)

	s.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", s.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)

	return nil
}

func (s *segment) computeAndStoreSecondaryBloomFilter(path string, pos int) error {
	count := s.secondaryIndices[pos].KeyCount()
	s.secondaryBloomFilters[pos] = bloom.NewWithEstimates(uint(count), 0.001)
	s.secondaryIndices[pos].ForEachKey(func(key []byte) {
		s.secondaryBloomFilters[pos].Add(key)
	})

	if err := s.storeBloomFilterSecondaryOnDisk(path, pos); err != nil {
		return fmt.Errorf("store secondary bloom filter on disk: %w", err)
	}

	return nil
}

func (s *segment) storeBloomFilterSecondaryOnDisk(path string, pos int) error {
	bfSize := getBloomFilterSize(s.bloomFilter)

	rw := byteops.NewReadWriter(make([]byte, bfSize+byteops.Uint32Len))
	rw.MoveBufferPositionForward(byteops.Uint32Len) // leave space for checksum
	_, err := s.secondaryBloomFilters[pos].WriteTo(&rw)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(rw, path, s.observeMetaWrite)
}

func (s *segment) loadBloomFilterSecondaryFromDisk(pos int) error {
	data, err := loadWithChecksum(s.bloomFilterSecondaryPath(pos), -1, s.metrics.ReadObserver("loadSecondaryBloomFilter"))
	if err != nil {
		return err
	}

	s.secondaryBloomFilters[pos] = new(bloom.BloomFilter)
	_, err = s.secondaryBloomFilters[pos].ReadFrom(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("read bloom filter from disk: %w", err)
	}

	return nil
}

func fileExistsInList(nameList map[string]int64, filePath string) (bool, error) {
	if nameList != nil {
		_, ok := nameList[filePath]
		return ok, nil
	} else {
		return fileExists(filePath)
	}
}

// writeWithChecksum expects the data in the buffer to start at position byteops.Uint32Len so the
// checksum can be added into the same buffer at its start and everything can be written to the file
// in one go
func writeWithChecksum(bufWriter byteops.ReadWriter, path string, observeFileWriter diskio.MeteredWriterCallback) error {
	// checksum needs to be at the start of the file
	chksm := crc32.ChecksumIEEE(bufWriter.Buffer[byteops.Uint32Len:])
	bufWriter.MoveBufferToAbsolutePosition(0)
	bufWriter.WriteUint32(chksm)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}

	meteredW := diskio.NewMeteredWriter(f, observeFileWriter)

	if _, err := meteredW.Write(bufWriter.Buffer); err != nil {
		// ignoring f.Close() error here, as we don't care about whether the file
		// was flushed, the call is mainly intended to prevent a file descriptor
		// leak.  We still want to return the original error below.
		f.Close()
		return fmt.Errorf("write bloom filter to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close bloom filter file: %w", err)
	}

	return nil
}

// use negative length check to indicate that no length check should be
// performed
func loadWithChecksum(path string, lengthCheck int, observeFileReader BytesReadObserver) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	meteredF := diskio.NewMeteredReader(f, diskio.MeteredReaderCallback(observeFileReader))

	data, err := io.ReadAll(meteredF)
	if err != nil {
		return nil, err
	}
	if lengthCheck > 0 && len(data) != lengthCheck {
		return nil, ErrInvalidChecksum
	}

	if len(data) < 4 {
		// the file does not even contain the full checksum, we must consider it corrupt
		return nil, ErrInvalidChecksum
	}

	chcksm := binary.LittleEndian.Uint32(data[:4])
	actual := crc32.ChecksumIEEE(data[4:])
	if chcksm != actual {
		return nil, ErrInvalidChecksum
	}

	return data[4:], nil
}

func getBloomFilterSize(bf *bloom.BloomFilter) int {
	// size of the bloom filter is size of the underlying bitSet and two uint64 parameters
	bs := bf.BitSet()
	bsSize := bs.BinaryStorageSize()
	return bsSize + 2*byteops.Uint64Len
}
