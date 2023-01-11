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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/entities"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) getCollection(key []byte) ([]value, error) {
	if i.strategy != SegmentStrategySetCollection &&
		i.strategy != SegmentStrategyMapCollection {
		return nil, errors.Errorf("get only possible for strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	if !i.bloomFilter.Test(key) {
		return nil, entities.NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, entities.NotFound
		} else {
			return nil, err
		}
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
	copy(contentsCopy, i.contents[node.Start:node.End])

	return i.collectionStratParseData(contentsCopy)
}

func (i *segment) collectionStratParseData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, entities.NotFound
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

func (i *segment) collectionStratParseDataWithKey(in []byte) (segmentCollectionNode, error) {
	r := bytes.NewReader(in)

	if len(in) == 0 {
		return segmentCollectionNode{}, entities.NotFound
	}

	return ParseCollectionNode(r)
}

func (i *segment) collectionStratParseDataWithKeyInto(in []byte, node *segmentCollectionNode) error {
	if len(in) == 0 {
		return entities.NotFound
	}

	return ParseCollectionNodeInto(in, node)
}
