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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

func (m *Memtable) flushDataRoaringSetRange(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	nodes := m.roaringSetRange.Nodes()

	totalDataLength := totalPayloadSizeRoaringSetRange(nodes)
	header := &segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSetRange,
	}

	_, err := f.WriteHeader(header)
	if err != nil {
		return nil, err
	}

	for i, node := range nodes {
		sn, err := roaringsetrange.NewSegmentNode(node.Key, node.Additions, node.Deletions)
		if err != nil {
			return nil, fmt.Errorf("create segment node: %w", err)
		}

		_, err = f.BodyWriter().Write(sn.ToBuffer())
		if err != nil {
			return nil, fmt.Errorf("write segment node %d: %w", i, err)
		}
	}

	return make([]segmentindex.Key, 0), nil
}

func totalPayloadSizeRoaringSetRange(nodes []*roaringsetrange.MemtableNode) int {
	var sum int
	for _, node := range nodes {
		sum += 8 // uint64 to segment length
		sum += 1 // key (fixed size)
		sum += 8 // uint64 to indicate length of additions bitmap
		sum += len(node.Additions.ToBuffer())

		if node.Key == 0 {
			sum += 8 // uint64 to indicate length of deletions bitmap
			sum += len(node.Deletions.ToBuffer())
		}
	}

	return sum
}
