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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) getCollection(key []byte) ([]value, error) {
	if s.strategy != segmentindex.StrategySetCollection &&
		s.strategy != segmentindex.StrategyMapCollection {
		return nil, fmt.Errorf("get only possible for strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
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

	return s.collectionStratParseData(contentsCopy)
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
