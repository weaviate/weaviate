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
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) get(key []byte) ([]byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	before := time.Now()

	if !s.bloomFilter.Test(key) {
		s.bloomFilterMetrics.trueNegative(before)
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		if err == lsmkv.NotFound {
			s.bloomFilterMetrics.falsePositive(before)
			return nil, lsmkv.NotFound
		} else {
			return nil, err
		}
	}

	defer s.bloomFilterMetrics.truePositive(before)

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
	copy(contentsCopy, s.contents[node.Start:node.End])

	return s.replaceStratParseData(contentsCopy)
}

func (s *segment) getBySecondary(pos int, key []byte) ([]byte, error) {
	if s.strategy != segmentindex.StrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos > len(s.secondaryIndices) || s.secondaryIndices[pos] == nil {
		return nil, errors.Errorf("no secondary index at pos %d", pos)
	}

	if !s.secondaryBloomFilters[pos].Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.secondaryIndices[pos].Get(key)
	if err != nil {
		return nil, err
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
	copy(contentsCopy, s.contents[node.Start:node.End])

	return s.replaceStratParseData(contentsCopy)
}

func (s *segment) replaceStratParseData(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}

	// byte         meaning
	// 0         is tombstone
	// 1-8       data length as Little Endian uint64
	// 9-length  data

	// check the tombstone byte
	if in[0] == 0x01 {
		return nil, lsmkv.Deleted
	}

	valueLength := binary.LittleEndian.Uint64(in[1:9])

	return in[9 : 9+valueLength], nil
}

func (s *segment) replaceStratParseDataWithKey(in []byte) (segmentReplaceNode, error) {
	if len(in) == 0 {
		return segmentReplaceNode{}, lsmkv.NotFound
	}

	r := bytes.NewReader(in)

	out, err := ParseReplaceNode(r, s.secondaryIndexCount)
	if err != nil {
		return out, err
	}

	if out.tombstone {
		return out, lsmkv.Deleted
	}

	return out, nil
}

func (s *segment) replaceStratParseDataWithKeyInto(in []byte,
	node *segmentReplaceNode,
) error {
	if len(in) == 0 {
		return lsmkv.NotFound
	}

	r := bytes.NewReader(in)

	err := ParseReplaceNodeInto(r, s.secondaryIndexCount, node)
	if err != nil {
		return err
	}

	if node.tombstone {
		return lsmkv.Deleted
	}

	return nil
}
