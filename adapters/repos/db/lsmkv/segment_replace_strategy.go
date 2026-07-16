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

// getBySecondaryIndexNode performs only the secondary-index descent for key at
// pos: bloom-test then DiskTree.Get. It returns the index node (whose Start/End
// delimit the value's byte range in the segment) on a confirmed hit, WITHOUT
// reading the value bytes. Returns lsmkv.NotFound if the bloom filter or the
// on-disk index has no entry for key.
//
// This is the index half of getBySecondary, split out for the batched resolver:
// phase 1 collects confirmed index hits (newest-wins elimination keys on a
// confirmed hit, never a bloom pass) and phase 2 reads the value bytes later via
// readSecondaryValueAtNode. Whether the node is a live value or a tombstone is
// only knowable after the value read, but the newest confirmed index hit is
// authoritative for newest-wins regardless.
func (s *segment) getBySecondaryIndexNode(pos int, key []byte) (segmentindex.Node, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return segmentindex.Node{}, fmt.Errorf("getBySecondary only possible for strategy %q", StrategyReplace)
	}

	if pos >= len(s.secondaryIndices) || s.secondaryIndices[pos] == nil {
		return segmentindex.Node{}, fmt.Errorf("no secondary index at pos %d", pos)
	}

	if s.useBloomFilter && !s.secondaryBloomFilters[pos].Test(key) {
		return segmentindex.Node{}, lsmkv.NotFound
	}

	return s.secondaryIndices[pos].Get(key)
}

// readSecondaryValueAtNode reads and parses the value bytes for a node located by
// getBySecondaryIndexNode. It copies the bytes out of the (possibly mmap'd)
// segment exactly once here (copy-under-refcount, see the getBySecondary comment
// and https://github.com/weaviate/weaviate/issues/1837) so the returned
// primaryKey and value stay valid after the consistent view is released. Returns
// lsmkv.Deleted (or an errorFromTombstonedValue wrapping it) for a tombstone
// node. The returned allocBuf is the backing array of primaryKey and value; the
// caller may reuse it as the buffer for the next read only AFTER copying
// primaryKey/value out.
func (s *segment) readSecondaryValueAtNode(node segmentindex.Node, buffer []byte) ([]byte, []byte, []byte, error) {
	var contentsCopy []byte
	if uint64(cap(buffer)) >= node.End-node.Start {
		contentsCopy = buffer[:node.End-node.Start]
	} else {
		contentsCopy = make([]byte, node.End-node.Start)
	}
	if err := s.copyNode(contentsCopy, nodeOffset{node.Start, node.End}); err != nil {
		return nil, nil, nil, err
	}

	primaryKey, currContent, err := s.replaceStratParseData(contentsCopy)
	if err != nil {
		return nil, nil, nil, err
	}

	return primaryKey, currContent, contentsCopy, nil
}

func (s *segment) replaceStratParseData(in []byte) ([]byte, []byte, error) {
	if len(in) == 0 {
		return nil, nil, lsmkv.NotFound
	}

	// byte         meaning
	// 0         is tombstone
	// 1-8       data length as Little Endian uint64
	// 9-length  data

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
