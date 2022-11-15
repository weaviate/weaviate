//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) get(key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	before := time.Now()

	if !i.bloomFilter.Test(key) {
		i.bloomFilterMetrics.trueNegative(before)
		return nil, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			i.bloomFilterMetrics.falsePositive(before)
			return nil, NotFound
		} else {
			return nil, err
		}
	}

	defer i.bloomFilterMetrics.truePositive(before)

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
	// https://github.com/semi-technologies/weaviate/issues/1837
	contentsCopy := make([]byte, node.End-node.Start)
	copy(contentsCopy, i.contents[node.Start:node.End])

	return i.replaceStratParseData(contentsCopy)
}

func (i *segment) getBySecondary(pos int, key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos > len(i.secondaryIndices) || i.secondaryIndices[pos] == nil {
		return nil, errors.Errorf("no secondary index at pos %d", pos)
	}

	if !i.secondaryBloomFilters[pos].Test(key) {
		return nil, NotFound
	}

	node, err := i.secondaryIndices[pos].Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, NotFound
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
	// https://github.com/semi-technologies/weaviate/issues/1837
	contentsCopy := make([]byte, node.End-node.Start)
	copy(contentsCopy, i.contents[node.Start:node.End])

	return i.replaceStratParseData(contentsCopy)
}

func (i *segment) replaceStratParseData(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	// byte         meaning
	// 0         is tombstone
	// 1-8       data length as Little Endian uint64
	// 9-length  data

	// check the tombstone byte
	if in[0] == 0x01 {
		return nil, Deleted
	}

	valueLength := binary.LittleEndian.Uint64(in[1:9])

	return in[9 : 9+valueLength], nil
}

func (i *segment) replaceStratParseDataWithKey(in []byte) (segmentReplaceNode, error) {
	if len(in) == 0 {
		return segmentReplaceNode{}, NotFound
	}

	r := bytes.NewReader(in)

	out, err := ParseReplaceNode(r, i.secondaryIndexCount)
	if err != nil {
		return out, err
	}

	if out.tombstone {
		return out, Deleted
	}

	return out, nil
}

func (i *segment) replaceStratParseDataWithKeyInto(in []byte,
	node *segmentReplaceNode,
) error {
	if len(in) == 0 {
		return NotFound
	}

	r := bytes.NewReader(in)

	err := ParseReplaceNodeInto(r, i.secondaryIndexCount, node)
	if err != nil {
		return err
	}

	if node.tombstone {
		return Deleted
	}

	return nil
}
