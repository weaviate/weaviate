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

func (ht *SegmentedHashTree) Diff(ht2 AggregatedHashTree) (discriminant *Bitset, err error) {
	cht2, isSegmentedHashTree := ht2.(*SegmentedHashTree)

	if ht2 == nil || !isSegmentedHashTree {
		return nil, ErrIllegalArguments
	}

	return ht.hashtree.Diff(cht2.hashtree)
}

func (ht *SegmentedHashTree) DiffUsing(ht2 AggregatedHashTree, discriminant *Bitset, digests1, digests2 []Digest) error {
	cht2, isSegmentedHashTree := ht2.(*SegmentedHashTree)

	if ht2 == nil || !isSegmentedHashTree {
		return ErrIllegalArguments
	}

	return ht.hashtree.DiffUsing(cht2.hashtree, discriminant, digests1, digests2)
}
