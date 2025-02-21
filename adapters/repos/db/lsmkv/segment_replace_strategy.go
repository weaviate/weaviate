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
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) get(key []byte) ([]byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, fmt.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	before := time.Now()

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		s.bloomFilterMetrics.trueNegative(before)
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			if s.useBloomFilter {
				s.bloomFilterMetrics.falsePositive(before)
			}
			return nil, lsmkv.NotFound
		} else {
			return nil, err
		}
	}

	defer func() {
		if s.useBloomFilter {
			s.bloomFilterMetrics.truePositive(before)
		}
	}()

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

func (s *segment) getBySecondaryIntoMemory(pos int, key []byte, buffer []byte) ([]byte, []byte, []byte, error) {
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

func (s *segment) exists(key []byte) (bool, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return false, fmt.Errorf("exists only possible for strategy %q", StrategyReplace)
	}

	before := time.Now()

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		s.bloomFilterMetrics.trueNegative(before)
		return false, nil
	}

	_, err := s.index.Get(key)

	if err == nil {
		if s.useBloomFilter {
			s.bloomFilterMetrics.truePositive(before)
		}
		return true, nil
	}
	if errors.Is(err, lsmkv.NotFound) {
		if s.useBloomFilter {
			s.bloomFilterMetrics.falsePositive(before)
		}
		return false, nil
	}
	return false, err
}
