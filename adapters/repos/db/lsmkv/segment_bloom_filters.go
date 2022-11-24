package lsmkv

import (
	"fmt"
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

func (ind *segment) initBloomFilter() error {
	ok, err := fileExists(ind.bloomFilterPath())
	if err != nil {
		return err
	}

	if ok {
		return ind.loadBloomFilterFromDisk()
	}

	before := time.Now()
	keys, err := ind.index.AllKeys()
	if err != nil {
		return err
	}

	ind.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.bloomFilter.Add(key)
	}

	if err := ind.storeBloomFilterOnDisk(); err != nil {
		return err
	}

	took := time.Since(before)
	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_primary").
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}

func (ind *segment) storeBloomFilterOnDisk() error {
	f, err := os.Create(ind.bloomFilterPath())
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}

	_, err = ind.bloomFilter.WriteTo(f)
	if err != nil {
		return fmt.Errorf("write bloom filter to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close bloom filter file: %w", err)
	}

	return nil
}

func (ind *segment) loadBloomFilterFromDisk() error {
	f, err := os.Open(ind.bloomFilterPath())
	if err != nil {
		return fmt.Errorf("open file for reading: %w", err)
	}

	ind.bloomFilter = new(bloom.BloomFilter)
	_, err = ind.bloomFilter.ReadFrom(f)
	if err != nil {
		return fmt.Errorf("read bloom filter from disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close bloom filter file: %w", err)
	}

	return nil
}

func (ind *segment) initSecondaryBloomFilter(pos int) error {
	before := time.Now()
	keys, err := ind.secondaryIndices[pos].AllKeys()
	if err != nil {
		return err
	}

	ind.secondaryBloomFilters[pos] = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		ind.secondaryBloomFilters[pos].Add(key)
	}
	took := time.Since(before)

	ind.logger.WithField("action", "lsm_init_disk_segment_build_bloom_filter_secondary").
		WithField("secondary_index_position", pos).
		WithField("path", ind.path).
		WithField("took", took).
		Debugf("building bloom filter took %s\n", took)
	return nil
}
