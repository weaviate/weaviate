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

type CompactHashTreeDiffReader struct {
	ht         *CompactHashTree
	diffReader AggregatedHashTreeDiffReader
}

func (ht *CompactHashTree) NewDiffReader(discriminant *Bitset) AggregatedHashTreeDiffReader {
	return &CompactHashTreeDiffReader{
		ht:         ht,
		diffReader: ht.hashtree.NewDiffReader(discriminant),
	}
}

func (r *CompactHashTreeDiffReader) Next() (uint64, uint64, error) {
	mappedLeaf0, mappedLeaf1, err := r.diffReader.Next()
	if err != nil {
		return 0, 0, err
	}

	var groupSize uint64

	if mappedLeaf1 < uint64(r.ht.extendedGroupsCount) {
		groupSize = r.ht.extendedGroupSize
	} else {
		groupSize = r.ht.groupSize
	}

	return r.ht.unmapLeaf(mappedLeaf0), r.ht.unmapLeaf(mappedLeaf1) + groupSize - 1, nil
}
