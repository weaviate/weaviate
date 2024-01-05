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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/willf/bloom"
)

func (s *segment) bloomFilterPath() string {
	extless := strings.TrimSuffix(s.path, filepath.Ext(s.path))
	return fmt.Sprintf("%s.bloom", extless)
}

func (s *segment) bloomFilterSecondaryPath(pos int) string {
	extless := strings.TrimSuffix(s.path, filepath.Ext(s.path))
	return fmt.Sprintf("%s.secondary.%d.bloom", extless, pos)
}

func (s *segment) initBloomFilters(metrics *Metrics) error {
	if err := s.initBloomFilter(); err != nil {
		return fmt.Errorf("init bloom filter for primary index: %w", err)
	}
	if s.secondaryIndexCount > 0 {
		s.secondaryBloomFilters = make([]*bloom.BloomFilter, s.secondaryIndexCount)
		for i := range s.secondaryBloomFilters {
			if err := s.initSecondaryBloomFilter(i); err != nil {
				return fmt.Errorf("init bloom filter for secondary index at %d: %w", i, err)
			}
		}
	}
	s.bloomFilterMetrics = newBloomFilterMetrics(metrics)
	return nil
}

func (s *segment) initBloomFilter() error {
	path := s.bloomFilterPath()
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		err = s.loadBloomFilterFromDisk()
		if err == nil {
			return nil
		}

		if err != ErrInvalidChecksum {
			// not a recoverable error
			return err
		}

		// now continue re-calculating
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
		return fmt.Errorf("a bloom filter already exists with path %s", path)
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
	buf := new(bytes.Buffer)

	_, err := s.bloomFilter.WriteTo(buf)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (s *segment) loadBloomFilterFromDisk() error {
	data, err := loadWithChecksum(s.bloomFilterPath(), -1)
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

func (s *segment) initSecondaryBloomFilter(pos int) error {
	before := time.Now()

	path := s.bloomFilterSecondaryPath(pos)
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		err = s.loadBloomFilterSecondaryFromDisk(pos)
		if err == nil {
			return nil
		}

		if err != ErrInvalidChecksum {
			// not a recoverable error
			return err
		}

		// now continue re-calculating
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
		return fmt.Errorf("a secondary bloom filter already exists with path %s", path)
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
	buf := new(bytes.Buffer)
	_, err := s.secondaryBloomFilters[pos].WriteTo(buf)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (s *segment) loadBloomFilterSecondaryFromDisk(pos int) error {
	data, err := loadWithChecksum(s.bloomFilterSecondaryPath(pos), -1)
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

func writeWithChecksum(data []byte, path string) error {
	chksm := crc32.ChecksumIEEE(data)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}

	if err := binary.Write(f, binary.LittleEndian, chksm); err != nil {
		// ignoring f.Close() error here, as we don't care about whether the file
		// was flushed, the call is mainly intended to prevent a file descriptor
		// leak.  We still want to return the original error below.
		f.Close()
		return fmt.Errorf("write checkusm to file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
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
func loadWithChecksum(path string, lengthCheck int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file for reading: %w", err)
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(f); err != nil {
		// ignoring f.Close() error here, as we don't care about whether the file
		// was flushed, the call is mainly intended to prevent a file descriptor
		// leak.  We still want to return the original error below.
		f.Close()
		return nil, fmt.Errorf("read bloom filter data from file: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close bloom filter file: %w", err)
	}

	data := buf.Bytes()
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
