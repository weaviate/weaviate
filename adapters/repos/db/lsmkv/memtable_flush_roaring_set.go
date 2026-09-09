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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func (m *Memtable) flushDataRoaringSet(f *segmentindex.SegmentFile) ([]segmentindex.KeyRedux, error) {
	// The header below pins SecondaryIndices to 0, so a non-zero count would only
	// add an offset table the reader does not skip, leaving the primary index
	// unparseable.
	if m.secondaryIndices != 0 {
		return nil, fmt.Errorf("roaring set flush cannot write %d secondary indexes",
			m.secondaryIndices)
	}

	// FlattenInOrder reads every node's bitmaps, which the roaringSet* writers
	// mutate under m.Lock().
	m.RLock()
	defer m.RUnlock()

	flat := m.roaringSet.FlattenInOrder()

	totalDataLength := totalPayloadSizeRoaringSet(flat)
	header := &segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSet,
	}

	n, err := f.WriteHeader(header)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	// flush() marshals the index from segmentindex.HeaderSize, so a short header
	// write would lay the nodes at an offset no index entry names.
	if headerSize != segmentindex.HeaderSize {
		return nil, fmt.Errorf("header write returned %d bytes, want %d",
			headerSize, segmentindex.HeaderSize)
	}

	keys := make([]segmentindex.KeyRedux, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		sn, err := roaringset.NewSegmentNode(node.Key, node.Value.Additions,
			node.Value.Deletions)
		if err != nil {
			return nil, fmt.Errorf("create segment node: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(f.BodyWriter(), totalWritten)
		if err != nil {
			return nil, fmt.Errorf("write node %d: %w", i, err)
		}

		// ki.Key is a subslice of the node's serialization, so keeping it would
		// hold the whole segment body until flush() writes the index. The tree's
		// key has the same bytes and outlives the flush.
		keys[i] = segmentindex.KeyRedux{Key: node.Key, ValueEnd: ki.ValueEnd}
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func totalPayloadSizeRoaringSet(in []*roaringset.BinarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += 8 // uint64 to segment length
		sum += 8 // uint64 to indicate length of additions bitmap
		sum += len(n.Value.Additions.ToBuffer())
		sum += 8 // uint64 to indicate length of deletions bitmap
		sum += len(n.Value.Deletions.ToBuffer())
		sum += 4 // uint32 to indicate key size
		sum += len(n.Key)
	}

	return sum
}
