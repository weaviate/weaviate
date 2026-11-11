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
	"fmt"
	"slices"

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

	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		if lsmkv.IsNotFound(err) {
			return nil, lsmkv.NotFound
		}
		return nil, s.reportIndexErr(err)
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
	contentsCopy := make([]byte, end-start)
	if err = s.copyNode(contentsCopy, nodeOffset{start, end}); err != nil {
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

	start, end, err := s.secondaryIndices[pos].GetOffsets(key)
	if err != nil {
		// a segment holds one primary index plus one per secondary, each separately
		// corruptible, and the path alone does not say which
		return nil, nil, nil, s.reportIndexErr(fmt.Errorf("secondary index %d: %w", pos, err))
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
	//
	// The copy's capacity stops at the node, so a length read out of the payload
	// cannot slice into what a reused buffer holds of a larger node. buffer is
	// returned instead, keeping the capacity the caller grew.
	nodeSize := end - start
	if uint64(cap(buffer)) < nodeSize {
		buffer = make([]byte, nodeSize)
	}
	contentsCopy := buffer[:nodeSize:nodeSize]
	if err = s.copyNode(contentsCopy, nodeOffset{start, end}); err != nil {
		return nil, nil, nil, err
	}

	primaryKey, currContent, err := s.replaceStratParseData(contentsCopy)
	if err != nil {
		return nil, nil, nil, err
	}

	return primaryKey, currContent, buffer, err
}

// replaceNodePrefixLen is the tombstone byte and the value length that
// open a replace node (see replaceStratParseData).
const replaceNodePrefixLen = 1 + 8

const rangeNodeOp = "copyNodeRange"

// getBySecondaryRange is getBySecondary narrowed to a range of the value,
// for Bucket.GetBySecondaryRange. The node's prefix and the value's head
// come in one positioned read, the range in a second, and the node's tail,
// the primary key and the secondary keys, in a third (the caller's recheck
// in newer segments needs the primary key). The node is read whole only when
// the head covers the whole value, or for a tombstone. An in-memory segment slices its contents. The lookup is
// the same as in getBySecondary. The range is copied into buffer, as
// getBySecondary copies the node, because the segment may be compacted away
// once the read returns.
func (s *segment) getBySecondaryRange(pos int, key []byte, head uint64, pick ValueRangePicker, buffer []byte) ([]byte, []byte, []byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, nil, nil, fmt.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos >= len(s.secondaryIndices) || s.secondaryIndices[pos] == nil {
		return nil, nil, nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	if s.useBloomFilter && !s.secondaryBloomFilters[pos].Test(key) {
		return nil, nil, nil, lsmkv.NotFound
	}

	start, end, err := s.secondaryIndices[pos].GetOffsets(key)
	if err != nil {
		return nil, nil, nil, s.reportIndexErr(fmt.Errorf("secondary index %d: %w", pos, err))
	}

	if s.readFromMemory {
		node := s.contents[start:end]
		if len(node) == 0 || node[0] == 0x01 {
			if err := checkTombstoneLength(node, start); err != nil {
				return nil, nil, nil, err
			}
			_, _, err := s.replaceStratParseData(node)
			return nil, nil, nil, err
		}
		if uint64(len(node)) < replaceNodePrefixLen {
			return nil, nil, nil, fmt.Errorf("replace node of %d bytes at %d: shorter than its prefix", len(node), start)
		}
		valueLength := binary.LittleEndian.Uint64(node[1:replaceNodePrefixLen])
		if err := checkValueLength(uint64(len(node)), valueLength, start); err != nil {
			return nil, nil, nil, err
		}
		tailStart := replaceNodePrefixLen + valueLength
		primaryKey, err := s.primaryKeyOfNodeTail(node[tailStart:], start)
		if err != nil {
			return nil, nil, nil, err
		}
		out, buffer, err := rangeOfValue(node[replaceNodePrefixLen:tailStart], head, pick, buffer)
		if err != nil {
			return nil, nil, nil, err
		}
		return slices.Clone(primaryKey), out, buffer, nil
	}

	nodeSize := end - start
	prefixLen := replaceNodePrefixLen + head
	if nodeSize < prefixLen {
		prefixLen = nodeSize
	}
	prefix := make([]byte, prefixLen)
	if err := s.preadInto(prefix, start, rangeNodeOp); err != nil {
		return nil, nil, nil, err
	}
	if len(prefix) == 0 {
		return nil, nil, nil, fmt.Errorf("empty replace node at %d", start)
	}
	if prefix[0] == 0x01 {
		// a tombstone: a small node, parsed whole as getBySecondary parses it
		node := make([]byte, nodeSize)
		if err := s.preadInto(node, start, rangeNodeOp); err != nil {
			return nil, nil, nil, err
		}
		if err := checkTombstoneLength(node, start); err != nil {
			return nil, nil, nil, err
		}
		_, _, err := s.replaceStratParseData(node)
		return nil, nil, nil, err
	}
	if len(prefix) < replaceNodePrefixLen {
		return nil, nil, nil, fmt.Errorf("replace node of %d bytes at %d: shorter than its prefix", nodeSize, start)
	}
	valueLength := binary.LittleEndian.Uint64(prefix[1:replaceNodePrefixLen])
	if err := checkValueLength(nodeSize, valueLength, start); err != nil {
		return nil, nil, nil, err
	}

	readHead := func(n uint64) ([]byte, error) {
		if n <= uint64(len(prefix))-replaceNodePrefixLen {
			return prefix[replaceNodePrefixLen : replaceNodePrefixLen+n], nil
		}
		h := make([]byte, n)
		return h, s.preadInto(h, start+replaceNodePrefixLen, rangeNodeOp)
	}
	offset, length, err := pickRange(valueLength, head, pick, readHead)
	if err != nil {
		return nil, nil, nil, err
	}

	if buffer == nil || uint64(cap(buffer)) < length { // nil means absent
		buffer = make([]byte, length)
	}
	out := buffer[:length:length]
	if length > 0 {
		if err := s.preadInto(out, start+replaceNodePrefixLen+offset, rangeNodeOp); err != nil {
			return nil, nil, nil, err
		}
	}

	tail := make([]byte, nodeSize-replaceNodePrefixLen-valueLength)
	if err := s.preadInto(tail, start+replaceNodePrefixLen+valueLength, rangeNodeOp); err != nil {
		return nil, nil, nil, err
	}
	primaryKey, err := s.primaryKeyOfNodeTail(tail, start)
	if err != nil {
		return nil, nil, nil, err
	}
	return primaryKey, out, buffer, nil
}

// checkValueLength checks a node's declared value length against the node's
// size, by subtraction: a corrupt length near 2^64 would wrap a sum. The
// primary key's 4-byte length follows the value.
func checkValueLength(nodeSize, valueLength, start uint64) error {
	if nodeSize < replaceNodePrefixLen+4 || valueLength > nodeSize-replaceNodePrefixLen-4 {
		return fmt.Errorf("replace node of %d bytes at %d declares a value of %d bytes", nodeSize, start, valueLength)
	}
	return nil
}

// checkTombstoneLength checks a tombstone's declared value length against the
// node's size, so that a corrupt one is an error here rather than a slice
// bounds panic in replaceStratParseData, which the other readers keep.
func checkTombstoneLength(node []byte, start uint64) error {
	if uint64(len(node)) < replaceNodePrefixLen {
		return nil // parsed as a short tombstone
	}
	if valueLength := binary.LittleEndian.Uint64(node[1:replaceNodePrefixLen]); valueLength > uint64(len(node))-replaceNodePrefixLen {
		return fmt.Errorf("tombstone node of %d bytes at %d declares a value of %d bytes", len(node), start, valueLength)
	}
	return nil
}

// primaryKeyOfNodeTail returns the primary key from what follows a node's
// value: the primary key and the secondary keys, each with a 4-byte length.
// These must fill the tail exactly, so a value length that is wrong but fits
// the node is an error too.
func (s *segment) primaryKeyOfNodeTail(tail []byte, start uint64) ([]byte, error) {
	var primaryKey []byte
	rest := tail
	for i := range 1 + int(s.secondaryIndexCount) {
		if len(rest) < 4 {
			return nil, fmt.Errorf("replace node at %d: key %d has no length", start, i)
		}
		n := uint64(binary.LittleEndian.Uint32(rest))
		rest = rest[4:]
		if n > uint64(len(rest)) {
			return nil, fmt.Errorf("replace node at %d declares a key of %d bytes, %d are left", start, n, len(rest))
		}
		if i == 0 {
			primaryKey = rest[:n]
		}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("replace node at %d has %d bytes after its keys", start, len(rest))
	}
	return primaryKey, nil
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

// indexContainsKey answers from the bloom filter and index alone, so a
// tombstoned entry counts as present: the index carries every key the segment
// holds, whether its node is a value or a tombstone. That is what supersession
// callers need — a tombstone in a newer segment must hide the older row.
func (s *segment) indexContainsKey(key []byte) (bool, error) {
	// compared directly rather than through the variadic CheckExpectedStrategy,
	// whose argument slice escapes and so allocates on every call: a targeted scan
	// probes this once per row per newer segment
	if s.strategy != segmentindex.StrategyReplace {
		return false, fmt.Errorf("segment::indexContainsKey: %w",
			segmentindex.CheckExpectedStrategy(s.strategy, segmentindex.StrategyReplace))
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false, nil
	}

	contains, err := s.index.Contains(key)
	if err != nil {
		return false, s.reportIndexErr(err)
	}
	return contains, nil
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

	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		if lsmkv.IsNotFound(err) {
			return lsmkv.NotFound
		}
		return s.reportIndexErr(err)
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
	nodeSize := end - start
	headerSize := uint64(maxHeaderSize)
	if nodeSize < headerSize {
		headerSize = nodeSize
	}

	// Use stack-allocated buffer to avoid heap allocation on every call
	var headerBuf [maxHeaderSize]byte
	header := headerBuf[:headerSize]
	if err = s.copyNode(header, nodeOffset{start, start + headerSize}); err != nil {
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
