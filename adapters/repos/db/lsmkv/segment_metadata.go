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
	"errors"
	"fmt"
	"path/filepath"

	"github.com/weaviate/weaviate/entities/diskio"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const MetadataVersion = 0

func (s *segment) metadataPath() string {
	return s.buildPath("%s.metadata")
}

func (s *segment) initMetadata(metrics *Metrics, overwrite bool, exists existsOnLowerSegmentsFn, precomputedCNAValue *int, existingFilesList map[string]int64, writeMetadata bool) (bool, error) {
	if !s.useBloomFilter && !s.calcCountNetAdditions {
		return false, nil
	}
	s.bloomFilterMetrics = newBloomFilterMetrics(metrics)

	path := s.metadataPath()

	loadFromDisk, err := fileExistsInList(existingFilesList, filepath.Base(path))
	if err != nil {
		return false, err
	}
	if loadFromDisk {
		if overwrite {
			err := diskio.Remove(path, "metadata")
			if err != nil {
				return false, fmt.Errorf("delete metadata %s: %w", path, err)
			}
		} else {
			err := s.loadMetaFromDisk(path)
			if err == nil {
				return true, nil // successfully loaded
			}
			if !errors.Is(err, ErrInvalidChecksum) {
				// not a recoverable error
				return false, err
			}

			// now continue re-calculating
		}
	}

	if !writeMetadata {
		return false, nil
	}
	// don't write metadata file if other metadata files exist
	bloomFilterFileExists, err := fileExistsInList(existingFilesList, filepath.Base(s.bloomFilterPath()))
	if err != nil || bloomFilterFileExists {
		return false, nil
	}
	cnaExists, err := fileExistsInList(existingFilesList, filepath.Base(s.countNetPath()))
	if err != nil || cnaExists {
		return false, nil
	}
	for i := 0; i < int(s.secondaryIndexCount); i++ {
		secondaryBloomFilterFileExists, err := fileExistsInList(existingFilesList, filepath.Base(s.bloomFilterSecondaryPath(i)))
		if err != nil || secondaryBloomFilterFileExists {
			return false, nil
		}
	}

	s.metaPaths = append(s.metaPaths, path)

	primaryBloom, secondaryBloom, err := s.recalculateBloomFilters()
	if err != nil {
		return false, err
	}

	netAdditions, err := s.recalcCountNetAdditions(exists, precomputedCNAValue)
	if err != nil {
		return false, err
	}

	return true, s.writeMetadataToDisk(path, primaryBloom, secondaryBloom, netAdditions)
}

func (s *segment) writeMetadataToDisk(path string, primaryBloom []byte, secondaryBloom [][]byte, netAdditions []byte) error {
	// Uint32 for checksum
	// byte for version
	// Uint32 for lengths - primary bloom filters, N secondary bloom filters and CNA
	sizeHeader := byteops.Uint32Len + 1 + (2+s.secondaryIndexCount)*byteops.Uint32Len
	sizeData := len(primaryBloom) + len(netAdditions)
	for _, b := range secondaryBloom {
		sizeData += len(b)
	}
	rw := byteops.NewReadWriter(make([]byte, int(sizeHeader)+sizeData))
	rw.MoveBufferPositionForward(byteops.Uint32Len) // leave space for checksum

	rw.WriteByte(MetadataVersion)

	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(primaryBloom); err != nil {
		return err
	}

	if err := rw.CopyBytesToBufferWithUint32LengthIndicator(netAdditions); err != nil {
		return err
	}
	for _, b := range secondaryBloom {
		if err := rw.CopyBytesToBufferWithUint32LengthIndicator(b); err != nil {
			return err
		}
	}
	return writeWithChecksum(rw, path, s.observeMetaWrite)
}

func (s *segment) recalculateBloomFilters() ([]byte, [][]byte, error) {
	if !s.useBloomFilter {
		return nil, nil, nil
	}
	primaryBloom, err := s.recalculatePrimaryBloomFilter()
	if err != nil {
		return nil, nil, err
	}

	secondaryBlooms, err := s.recalculateSecondaryBloomFilter()
	if err != nil {
		return nil, nil, err
	}

	return primaryBloom, secondaryBlooms, nil
}

func (s *segment) recalculatePrimaryBloomFilter() ([]byte, error) {
	keys, err := s.index.AllKeys()
	if err != nil {
		return nil, err
	}

	s.bloomFilter = bloom.NewWithEstimates(uint(len(keys)), 0.001)
	for _, key := range keys {
		s.bloomFilter.Add(key)
	}

	bfSize := getBloomFilterSize(s.bloomFilter)

	rw := byteops.NewReadWriter(make([]byte, bfSize))

	if _, err := s.bloomFilter.WriteTo(&rw); err != nil {
		return nil, err
	}

	return rw.Buffer, nil
}

func (s *segment) recalculateSecondaryBloomFilter() ([][]byte, error) {
	if s.secondaryIndexCount == 0 {
		return nil, nil
	}

	s.secondaryBloomFilters = make([]*bloom.BloomFilter, s.secondaryIndexCount)
	out := make([][]byte, s.secondaryIndexCount)
	for i := range s.secondaryBloomFilters {
		keys, err := s.secondaryIndices[i].AllKeys()
		if err != nil {
			return nil, err
		}

		s.secondaryBloomFilters[i] = bloom.NewWithEstimates(uint(len(keys)), 0.001)
		for _, key := range keys {
			s.secondaryBloomFilters[i].Add(key)
		}
		bfSize := getBloomFilterSize(s.secondaryBloomFilters[i])

		rw := byteops.NewReadWriter(make([]byte, bfSize))
		if _, err := s.secondaryBloomFilters[i].WriteTo(&rw); err != nil {
			return nil, err
		}

		out[i] = rw.Buffer
	}
	return out, nil
}

func (s *segment) loadMetaFromDisk(path string) error {
	data, err := loadWithChecksum(path, -1, s.metrics.ReadObserver("loadMetadata"))
	if err != nil {
		return err
	}

	rw := byteops.NewReadWriter(data)
	version := rw.ReadUint8()
	if version != MetadataVersion {
		return fmt.Errorf("invalid metadata version: %d", version)
	}
	primaryBloom := rw.ReadBytesFromBufferWithUint32LengthIndicator()
	netAdditions := rw.ReadBytesFromBufferWithUint32LengthIndicator()
	secondaryBloom := make([][]byte, s.secondaryIndexCount)
	for i := range secondaryBloom {
		secondaryBloom[i] = rw.ReadBytesFromBufferWithUint32LengthIndicator()
	}

	if err := s.initBloomFiltersFromData(primaryBloom, secondaryBloom); err != nil {
		return err
	}

	if err := s.initCNAFromData(netAdditions); err != nil {
		return err
	}

	return nil
}

func (s *segment) initBloomFiltersFromData(primary []byte, secondary [][]byte) error {
	if !s.useBloomFilter {
		return nil
	}

	s.bloomFilter = new(bloom.BloomFilter)
	_, err := s.bloomFilter.ReadFrom(bytes.NewReader(primary))
	if err != nil {
		return fmt.Errorf("read bloom filter: %w", err)
	}

	s.secondaryBloomFilters = make([]*bloom.BloomFilter, s.secondaryIndexCount)
	for i := range s.secondaryBloomFilters {
		s.secondaryBloomFilters[i] = new(bloom.BloomFilter)
		_, err := s.secondaryBloomFilters[i].ReadFrom(bytes.NewReader(secondary[i]))
		if err != nil {
			return fmt.Errorf("read bloom filter: %w", err)
		}

	}
	return nil
}

func (s *segment) initCNAFromData(netAdditions []byte) error {
	if !s.calcCountNetAdditions || s.strategy != segmentindex.StrategyReplace {
		return nil
	}

	s.countNetAdditions = int(binary.LittleEndian.Uint64(netAdditions))

	return nil
}

func (s *segment) recalcCountNetAdditions(exists existsOnLowerSegmentsFn, precomputedCNAValue *int) ([]byte, error) {
	if !s.calcCountNetAdditions || s.strategy != segmentindex.StrategyReplace {
		return nil, nil
	}

	if precomputedCNAValue != nil {
		s.countNetAdditions = *precomputedCNAValue
	} else {
		var lastErr error
		countNet := 0
		cb := func(key []byte, tombstone bool) {
			existedOnPrior, err := exists(key)
			if err != nil {
				lastErr = err
			}

			if tombstone && existedOnPrior {
				countNet--
			}

			if !tombstone && !existedOnPrior {
				countNet++
			}
		}

		extr := newBufferedKeyAndTombstoneExtractor(s.contents, s.dataStartPos,
			s.dataEndPos, 10e6, s.secondaryIndexCount, cb)

		extr.do()

		if lastErr != nil {
			return nil, lastErr
		}

		s.countNetAdditions = countNet
	}

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(s.countNetAdditions))
	return data, nil
}
