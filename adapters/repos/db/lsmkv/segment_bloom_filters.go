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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
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
	s.bloomFilterMetrics = newBloomFilterMetrics(metrics)
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

			if !errors.Is(err, ErrInvalidChecksum) {
				// not a recoverable error
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
	keys, err := s.index.AllKeys()
	if err != nil {
		return err
	}

	s.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		s.bloomFilter.Add(key)
	}

	if err := s.storeBloomFilterOnDisk(path); err != nil {
		return fmt.Errorf("store bloom filter on disk: %w", err)
	}

	return nil
}

func (s *segment) precomputeBloomFilters() ([]string, error) {
	out := []string{}

	if err := s.precomputeBloomFilter(); err != nil {
		return nil, fmt.Errorf("precompute bloom filter for primary index: %w", err)
	}
	out = append(out, fmt.Sprintf("%s.tmp", s.bloomFilterPath()))

	if s.secondaryIndexCount > 0 {
		s.secondaryBloomFilters = make([]*bloom.BloomFilter, s.secondaryIndexCount)
		for i := range s.secondaryBloomFilters {
			if err := s.precomputeSecondaryBloomFilter(i); err != nil {
				return nil, fmt.Errorf("precompute bloom filter for secondary index at %d: %w", i, err)
			}
			out = append(out, fmt.Sprintf("%s.tmp", s.bloomFilterSecondaryPath(i)))
		}
	}
	return out, nil
}

func (s *segment) precomputeBloomFilter() error {
	before := time.Now()

	path := fmt.Sprintf("%s.tmp", s.bloomFilterPath())
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		s.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_primary").
			WithField("path", path).
			Debugf("temp bloom filter already exists - deleting")
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("delete existing primary temp bloom filter %s: %w", path, err)
		}
	}

	if err := s.computeAndStoreBloomFilter(path); err != nil {
		return err
	}

	took := time.Since(before)
	s.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_primary").
		WithField("path", s.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)

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

			if !errors.Is(err, ErrInvalidChecksum) {
				// not a recoverable error
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
	keys, err := s.secondaryIndices[pos].AllKeys()
	if err != nil {
		return err
	}

	s.secondaryBloomFilters[pos] = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		s.secondaryBloomFilters[pos].Add(key)
	}

	if err := s.storeBloomFilterSecondaryOnDisk(path, pos); err != nil {
		return fmt.Errorf("store secondary bloom filter on disk: %w", err)
	}

	return nil
}

func (s *segment) precomputeSecondaryBloomFilter(pos int) error {
	before := time.Now()
	path := fmt.Sprintf("%s.tmp", s.bloomFilterSecondaryPath(pos))

	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		s.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_secondary").
			WithField("secondary_index_position", pos).
			WithField("path", path).
			Debugf("temp bloom filter already exists - deleting")
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("delete existing secondary temp bloom filter %s: %w", path, err)
		}
	}

	if err := s.computeAndStoreSecondaryBloomFilter(path, pos); err != nil {
		return err
	}

	took := time.Since(before)
	s.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", s.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
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
