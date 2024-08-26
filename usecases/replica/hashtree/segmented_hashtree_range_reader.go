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

package hashtree

type SegmentedHashTreeDiffReader struct {
	ht          *SegmentedHashTree
	rangeReader AggregatedHashTreeRangeReader
}

func (ht *SegmentedHashTree) NewRangeReader(discriminant *Bitset) AggregatedHashTreeRangeReader {
	return &SegmentedHashTreeDiffReader{
		ht:          ht,
		rangeReader: ht.hashtree.NewRangeReader(discriminant),
	}
}

func (r *SegmentedHashTreeDiffReader) Next() (uint64, uint64, error) {
	mappedLeaf0, mappedLeaf1, err := r.rangeReader.Next()
	if err != nil {
		return 0, 0, err
	}

	return r.ht.unmapLeaf(mappedLeaf0), r.ht.unmapLeaf(mappedLeaf1), nil
}
