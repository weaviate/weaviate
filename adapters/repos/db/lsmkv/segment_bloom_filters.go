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

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

const MetadataVersion = 0

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

func (s *segment) metadataPath() string {
	return s.buildPath("%s.metadata")
}

func (s *segment) initMetadata(metrics *Metrics, overwrite bool, exists existsOnLowerSegmentsFn, precomputedCNAValue *int, existingFilesList map[string]int64) error {
	if !s.useBloomFilter && !s.calcCountNetAdditions {
		return nil
	}
	s.bloomFilterMetrics = newBloomFilterMetrics(metrics)

	path := s.metadataPath()
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
			err := s.loadMetaFromDisk(path)
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

	primaryBloom, secondaryBloom, err := s.recalculateBloomFilters()
	if err != nil {
		return err
	}

	netAdditions, err := s.recalcCountNetAdditions(exists, precomputedCNAValue)
	if err != nil {
		return err
	}

	return s.writeMetadataToDisk(path, primaryBloom, secondaryBloom, netAdditions)
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
