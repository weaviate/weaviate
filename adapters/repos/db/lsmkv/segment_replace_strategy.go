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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) get(key []byte) ([]byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, fmt.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return nil, lsmkv.NotFound
		} else {
			return nil, err
		}
	}

	// We need to copy the data we read from the segment exactly once in this
	// place. This means that future processing can share this memory as much as
	// it wants to, as it can now be considered immutable. If we didn't copy in
	// this place it would only be safe to hold this data while still under the
	// protection of the segmentGroup.maintenanceLock. This lock makes sure that
	// no compaction is started during an ongoing read. However, once read,
	// further processing is no longer protected by lock.
	// If a compaction completes and the old segment is removed, we would be accessing
	// invalid memory without the copy, thus leading to a SEGFAULT.
	// Similar approach was used to fix SEGFAULT in collection strategy
	// https://github.com/weaviate/weaviate/issues/1837
	contentsCopy := make([]byte, node.End-node.Start)
	if err = s.copyNode(contentsCopy, nodeOffset{node.Start, node.End}); err != nil {
		return nil, err
	}

	_, v, err := s.replaceStratParseData(contentsCopy)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (s *segment) getBySecondary(pos int, key []byte, buffer []byte) ([]byte, []byte, []byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, nil, nil, fmt.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos >= len(s.secondaryIndices) || s.secondaryIndices[pos] == nil {
		return nil, nil, nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	if s.useBloomFilter && !s.secondaryBloomFilters[pos].Test(key) {
		return nil, nil, nil, lsmkv.NotFound
	}

	node, err := s.secondaryIndices[pos].Get(key)
	if err != nil {
		return nil, nil, nil, err
	}

	// We need to copy the data we read from the segment exactly once in this
	// place. This means that future processing can share this memory as much as
	// it wants to, as it can now be considered immutable. If we didn't copy in
	// this place it would only be safe to hold this data while still under the
	// protection of the segmentGroup.maintenanceLock. This lock makes sure that
	// no compaction is started during an ongoing read. However, once read,
	// further processing is no longer protected by lock.
	// If a compaction completes and the old segment is removed, we would be accessing
	// invalid memory without the copy, thus leading to a SEGFAULT.
	// Similar approach was used to fix SEGFAULT in collection strategy
	// https://github.com/weaviate/weaviate/issues/1837
	var contentsCopy []byte
	if uint64(cap(buffer)) >= node.End-node.Start {
		contentsCopy = buffer[:node.End-node.Start]
	} else {
		contentsCopy = make([]byte, node.End-node.Start)
	}
	if err = s.copyNode(contentsCopy, nodeOffset{node.Start, node.End}); err != nil {
		return nil, nil, nil, err
	}

	primaryKey, currContent, err := s.replaceStratParseData(contentsCopy)
	if err != nil {
		return nil, nil, nil, err
	}

	return primaryKey, currContent, contentsCopy, err
}

func (s *segment) replaceStratParseData(in []byte) ([]byte, []byte, error) {
	return parseReplaceNodeData(in)
}

// parseReplaceNodeData parses a raw secondary-index node byte slice and
// returns (primaryKey, value, error). It is a package-level function so that
// the batch secondary-lookup path can reuse it without a segment receiver.
//
// Binary layout:
//
//	byte 0:       tombstone flag (0x01 = deleted)
//	bytes 1-8:    value length (little-endian uint64)
//	bytes 9-...:  value bytes
//	next 4 bytes: primary-key length (little-endian uint32)
//	remaining:    primary-key bytes
func parseReplaceNodeData(in []byte) ([]byte, []byte, error) {
	if len(in) == 0 {
		return nil, nil, lsmkv.NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		if len(in) < 9 {
			return nil, nil, lsmkv.Deleted
		}

		valueLength := binary.LittleEndian.Uint64(in[1:9])

		return nil, nil, errorFromTombstonedValue(in[9 : 9+valueLength])
	}

	valueLength := binary.LittleEndian.Uint64(in[1:9])

	pkLength := binary.LittleEndian.Uint32(in[9+valueLength:])

	return in[9+valueLength+4 : 9+valueLength+4+uint64(pkLength)], in[9 : 9+valueLength], nil
}

func (s *segment) existsKey(key []byte) (bool, error) {
	if err := segmentindex.CheckExpectedStrategy(s.strategy, segmentindex.StrategyReplace); err != nil {
		return false, fmt.Errorf("segment::existsKey: %w", err)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false, nil
	}

	_, err := s.index.Get(key)

	if err == nil {
		return true, nil
	}
	if errors.Is(err, lsmkv.NotFound) {
		return false, nil
	}
	return false, err
}

// secondaryNodePos describes the location of a single secondary-index node.
//
// For mmap-backed or in-memory segments the data has already been copied into
// inMemoryData; Fd/Offset/Length are zero.
//
// For pread-backed segments inMemoryData is nil and Fd/Offset/Length describe
// the byte range to read from the segment file. The file descriptor remains
// valid as long as the bucket's flushLock read-lock is held.
//
// A zeroed secondaryNodePos with no inMemoryData and Fd==0 means "not found".
// deleted==true means the entry exists but carries a tombstone.
type secondaryNodePos struct {
	inMemoryData []byte
	fd           int
	offset       int64
	length       int64
	deleted      bool
}

// getSecondaryNodePos performs only the in-memory index lookup for a secondary
// key and returns the location of the corresponding node data. Unlike
// getBySecondary it does NOT perform the disk read for pread-backed segments;
// instead it returns the file position so that the caller can batch multiple
// reads together (e.g. via io_uring).
//
// For mmap-backed segments the node data is copied eagerly (same safety
// guarantee as getBySecondary: the copy is taken while the segment list lock
// is held, before any compaction can remove the segment).
func (s *segment) getSecondaryNodePos(pos int, key []byte) (secondaryNodePos, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return secondaryNodePos{}, fmt.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos >= len(s.secondaryIndices) || s.secondaryIndices[pos] == nil {
		return secondaryNodePos{}, fmt.Errorf("no secondary index at pos %d", pos)
	}

	if s.useBloomFilter && !s.secondaryBloomFilters[pos].Test(key) {
		return secondaryNodePos{}, lsmkv.NotFound
	}

	node, err := s.secondaryIndices[pos].Get(key)
	if err != nil {
		return secondaryNodePos{}, err
	}

	sz := node.End - node.Start

	if s.readFromMemory {
		// Data is already in memory (mmap or preloaded). Copy it now while
		// the maintenance lock is held so that a concurrent compaction cannot
		// free the segment and leave us with a dangling pointer.
		data := make([]byte, sz)
		copy(data, s.contents[node.Start:node.End])
		return secondaryNodePos{inMemoryData: data}, nil
	}

	// Pread-backed segment: return the position for later batch reading.
	// The file descriptor is stable for as long as the bucket flushLock
	// read-lock is held by the caller.
	if s.contentFile == nil {
		return secondaryNodePos{}, fmt.Errorf("nil contentFile for segment at %s", s.path)
	}
	return secondaryNodePos{
		fd:     int(s.contentFile.Fd()),
		offset: int64(node.Start),
		length: int64(sz),
	}, nil
}

// exists checks if a key exists and is not deleted, without reading the full value.
// Returns nil if the key exists and is not deleted, lsmkv.NotFound if not found,
// or lsmkv.Deleted (with deletion time) if the key was tombstoned.
//
// This method is optimized for the HNSW rescoring use case where we need to verify
// that objects still exist before rescoring their vectors. During rescoring, we may
// check thousands of objects, and reading the full object payload (which can be large)
// just to verify existence would be wasteful. Instead, this method reads only the
// tombstone header (up to 18 bytes) to determine if the key exists and is not deleted.
//
// Performance: O(1) index lookup + minimal I/O (18 bytes max vs potentially KB/MB for full objects).
func (s *segment) exists(key []byte) error {
	if s.strategy != segmentindex.StrategyReplace {
		return fmt.Errorf("exists only possible for strategy %q", StrategyReplace)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return lsmkv.NotFound
		}
		return err
	}

	// Read only the tombstone header instead of the full payload.
	// Format: byte 0 = tombstone flag, bytes 1-8 = value length, bytes 9+ = value
	// For tombstones, the value is 9 bytes (1 version + 8 timestamp).
	const (
		tombstoneFlagSize   = 1
		valueLengthSize     = 8
		maxTombstoneValSize = 9 // 1 version + 8 timestamp
		maxHeaderSize       = tombstoneFlagSize + valueLengthSize + maxTombstoneValSize
	)
	nodeSize := node.End - node.Start
	headerSize := uint64(maxHeaderSize)
	if nodeSize < headerSize {
		headerSize = nodeSize
	}

	// Use stack-allocated buffer to avoid heap allocation on every call
	var headerBuf [maxHeaderSize]byte
	header := headerBuf[:headerSize]
	if err = s.copyNode(header, nodeOffset{node.Start, node.Start + headerSize}); err != nil {
		return err
	}

	if len(header) == 0 {
		return lsmkv.NotFound
	}

	// Check tombstone flag
	if header[0] != 0x01 {
		// Not a tombstone, key exists
		return nil
	}

	// It's a tombstone - extract deletion time if available
	if len(header) < 9 {
		return lsmkv.Deleted
	}

	valueLength := binary.LittleEndian.Uint64(header[1:9])
	if valueLength == 0 || len(header) < 9+int(valueLength) {
		return lsmkv.Deleted
	}

	return errorFromTombstonedValue(header[9 : 9+valueLength])
}
