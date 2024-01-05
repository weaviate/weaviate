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

package hashtree

type MultiSegmentHashTreeDiffReader struct {
	ht         *MultiSegmentHashTree
	diffReader AggregatedHashTreeDiffReader
}

func (ht *MultiSegmentHashTree) NewDiffReader(discriminant *Bitset) AggregatedHashTreeDiffReader {
	return &MultiSegmentHashTreeDiffReader{
		ht:         ht,
		diffReader: ht.hashtree.NewDiffReader(discriminant),
	}
}

func (r *MultiSegmentHashTreeDiffReader) Next() (uint64, uint64, error) {
	mappedLeaf0, mappedLeaf1, err := r.diffReader.Next()
	if err != nil {
		return 0, 0, err
	}

	return r.ht.unmapLeaf(mappedLeaf0), r.ht.unmapLeaf(mappedLeaf1), nil
}
