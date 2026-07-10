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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) getCollection(key []byte) ([]value, error) {
	if s.strategy != segmentindex.StrategySetCollection &&
		s.strategy != segmentindex.StrategyMapCollection &&
		s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("get only possible for strategies %q, %q and %q, got %q",
			StrategySetCollection, StrategyMapCollection, StrategyInverted, s.strategy)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	// We need to copy the data we read from the segment exactly once in this
	// place. This means that future processing can share this memory as much as
	// it wants to, as it can now be considered immutable. If we didn't copy in
	// this place it would only be safe to hold this data while still under the
	// protection of the segmentGroup.maintenanceLock. This lock makes sure that
	// no compaction is started during an ongoing read. However, as we could show
	// as part of https://github.com/weaviate/weaviate/issues/1837
	// further processing, such as map-decoding and eventually map-merging would
	// happen inside the bucket.MapList() method. This scope has its own lock,
	// but that lock can only protecting against flushing (i.e. changing the
	// active/flushing memtable), not against removing the disk segment. If a
	// compaction completes and the old segment is removed, we would be accessing
	// invalid memory without the copy, thus leading to a SEGFAULT.
	contentsCopy := make([]byte, node.End-node.Start)
	if err = s.copyNode(contentsCopy, nodeOffset{node.Start, node.End}); err != nil {
		return nil, err
	}
	if s.strategy == segmentindex.StrategyInverted {
		return s.collectionStratParseDataInverted(contentsCopy)
	}

	return s.collectionStratParseData(contentsCopy)
}

func (s *segment) getCollectionBytes(key []byte) ([][]byte, error) {
	if s.strategy != segmentindex.StrategySetCollection {
		return nil, fmt.Errorf("getCollectionBytes only possible for strategy %q, got %q",
			StrategySetCollection, s.strategy)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	// We need to copy the data we read from the segment exactly once in this
	// place. This means that future processing can share this memory as much as
	// it wants to, as it can now be considered immutable. If we didn't copy in
	// this place it would only be safe to hold this data while still under the
	// protection of the segmentGroup.maintenanceLock. This lock makes sure that
	// no compaction is started during an ongoing read. However, as we could show
	// as part of https://github.com/weaviate/weaviate/issues/1837
	// further processing, such as map-decoding and eventually map-merging would
	// happen inside the bucket.MapList() method. This scope has its own lock,
	// but that lock can only protecting against flushing (i.e. changing the
	// active/flushing memtable), not against removing the disk segment. If a
	// compaction completes and the old segment is removed, we would be accessing
	// invalid memory without the copy, thus leading to a SEGFAULT.
	contentsCopy := make([]byte, node.End-node.Start)
	if err = s.copyNode(contentsCopy, nodeOffset{node.Start, node.End}); err != nil {
		return nil, err
	}
	return s.collectionStratParseDataBytes(contentsCopy)
}

// getCollectionBytesNoCopy behaves like getCollectionBytes but, when the
// segment serves reads from its in-memory/mmap contents (s.readFromMemory),
// parses directly over that memory and returns the values as sub-slices of it
// WITHOUT copying.
//
// WARNING: the returned slices alias the segment's contents. They are valid
// ONLY while the caller holds a consistent view that refcounts this segment
// (the view keeps the mmap mapped / the segment un-dropped). Retaining or
// mutating them after the view is released is a use-after-free against memory a
// completed compaction may have unmapped. Callers that must outlive the view
// have to use the copying getCollectionBytes instead.
//
// Segments on the pread strategy (readFromMemory == false: no stable in-memory
// slice to alias, reads go through the file descriptor) transparently fall back
// to the copying getCollectionBytes. Correctness over purity.
func (s *segment) getCollectionBytesNoCopy(key []byte) ([][]byte, error) {
	if !s.readFromMemory {
		// pread segment: no usable in-memory contents to alias.
		return s.getCollectionBytes(key)
	}

	if s.strategy != segmentindex.StrategySetCollection {
		return nil, fmt.Errorf("getCollectionBytesNoCopy only possible for strategy %q, got %q",
			StrategySetCollection, s.strategy)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	// Zero-copy: parse directly over the segment's in-memory/mmap contents.
	// collectionStratParseDataBytes returns sub-slices of its input, so the
	// returned values alias s.contents (valid only while a view holds the
	// segment). This is the same parse the copying path runs over its
	// contentsCopy; the only difference is that we skip the copy.
	return s.collectionStratParseDataBytes(s.contents[node.Start:node.End])
}

func (s *segment) collectionStratParseDataBytes(in []byte) ([][]byte, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}

	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	offset += 8

	values := make([][]byte, valuesLen)
	valueIndex := 0
	for valueIndex < int(valuesLen) {
		if in[offset] == 0x01 {
			// skip tombstone
			offset += 9
			valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
			offset += 8 + int(valueLen)
			continue
		}
		offset += 1

		valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
		offset += 8

		values[valueIndex] = in[offset : offset+int(valueLen)]
		offset += int(valueLen)

		valueIndex++
	}

	return values, nil
}

func (s *segment) collectionStratParseData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}

	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	offset += 8

	values := make([]value, valuesLen)
	valueIndex := 0
	for valueIndex < int(valuesLen) {
		values[valueIndex].tombstone = in[offset] == 0x01
		offset += 1

		valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
		offset += 8

		values[valueIndex].value = in[offset : offset+int(valueLen)]
		offset += int(valueLen)

		valueIndex++
	}

	return values, nil
}

func (s *segment) collectionStratParseDataInverted(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}

	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	// offset += 8

	values := make([]value, valuesLen)

	nodes, _ := decodeAndConvertFromBlocks(in)

	valueIndex := 0
	for _, node := range nodes {
		buf := make([]byte, 16)
		copy(buf, node.Key)
		copy(buf[8:], node.Value)
		values[valueIndex].tombstone = node.Tombstone
		values[valueIndex].value = buf

		valueIndex++

	}

	return values, nil
}
