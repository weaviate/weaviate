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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/willf/bloom"
)

func (ind *segment) bloomFilterPath() string {
	extless := strings.TrimSuffix(ind.path, filepath.Ext(ind.path))
	return fmt.Sprintf("%s.bloom", extless)
}

func (ind *segment) bloomFilterSecondaryPath(pos int) string {
	extless := strings.TrimSuffix(ind.path, filepath.Ext(ind.path))
	return fmt.Sprintf("%s.secondary.%d.bloom", extless, pos)
}

func (ind *segment) initBloomFilter() error {
	path := ind.bloomFilterPath()
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		err = ind.loadBloomFilterFromDisk()
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
	if err := ind.computeAndStoreBloomFilter(path); err != nil {
		return err
	}

	took := time.Since(before)
	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_primary").
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) computeAndStoreBloomFilter(path string) error {
	keys, err := ind.index.AllKeys()
	if err != nil {
		return err
	}

	ind.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.bloomFilter.Add(key)
	}

	if err := ind.storeBloomFilterOnDisk(path); err != nil {
		return fmt.Errorf("store bloom filter on disk: %w", err)
	}

	return nil
}

func (ind *segment) precomputeBloomFilter() error {
	before := time.Now()

	path := fmt.Sprintf("%s.tmp", ind.bloomFilterPath())
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		return fmt.Errorf("a bloom filter already exists with path %s", path)
	}

	if err := ind.computeAndStoreBloomFilter(path); err != nil {
		return err
	}

	took := time.Since(before)
	ind.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_primary").
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)

	return nil
}

func (ind *segment) storeBloomFilterOnDisk(path string) error {
	buf := new(bytes.Buffer)

	_, err := ind.bloomFilter.WriteTo(buf)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (ind *segment) loadBloomFilterFromDisk() error {
	data, err := loadWithChecksum(ind.bloomFilterPath(), -1)
	if err != nil {
		return err
	}

	ind.bloomFilter = new(bloom.BloomFilter)
	_, err = ind.bloomFilter.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("read bloom filter from disk: %w", err)
	}

	return nil
}

func (ind *segment) initSecondaryBloomFilter(pos int) error {
	before := time.Now()

	path := ind.bloomFilterSecondaryPath(pos)
	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		err = ind.loadBloomFilterSecondaryFromDisk(pos)
		if err == nil {
			return nil
		}

		if err != ErrInvalidChecksum {
			// not a recoverable error
			return err
		}

		// now continue re-calculating
	}

	if err := ind.computeAndStoreSecondaryBloomFilter(path, pos); err != nil {
		return err
	}

	took := time.Since(before)

	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) computeAndStoreSecondaryBloomFilter(path string, pos int) error {
	keys, err := ind.secondaryIndices[pos].AllKeys()
	if err != nil {
		return err
	}

	ind.secondaryBloomFilters[pos] = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.secondaryBloomFilters[pos].Add(key)
	}

	if err := ind.storeBloomFilterSecondaryOnDisk(path, pos); err != nil {
		return fmt.Errorf("store secondary bloom filter on disk: %w", err)
	}

	return nil
}

func (ind *segment) precomputeSecondaryBloomFilter(pos int) error {
	before := time.Now()
	path := fmt.Sprintf("%s.tmp", ind.bloomFilterSecondaryPath(pos))

	ok, err := fileExists(path)
	if err != nil {
		return err
	}

	if ok {
		return fmt.Errorf("a secondary bloom filter already exists with path %s", path)
	}

	if err := ind.computeAndStoreSecondaryBloomFilter(path, pos); err != nil {
		return err
	}

	took := time.Since(before)
	ind.logger.WithField("action", "lsm_precompute_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) storeBloomFilterSecondaryOnDisk(path string, pos int) error {
	buf := new(bytes.Buffer)
	_, err := ind.secondaryBloomFilters[pos].WriteTo(buf)
	if err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (ind *segment) loadBloomFilterSecondaryFromDisk(pos int) error {
	data, err := loadWithChecksum(ind.bloomFilterSecondaryPath(pos), -1)
	if err != nil {
		return err
	}

	ind.secondaryBloomFilters[pos] = new(bloom.BloomFilter)
	_, err = ind.secondaryBloomFilters[pos].ReadFrom(bytes.NewReader(data))
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
	chcksm := binary.LittleEndian.Uint32(data[:4])
	actual := crc32.ChecksumIEEE(data[4:])
	if chcksm != actual {
		return nil, ErrInvalidChecksum
	}

	return data[4:], nil
}
