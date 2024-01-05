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

func (ht *MultiSegmentHashTree) Diff(ht2 AggregatedHashTree) (discriminant *Bitset, err error) {
	msht2, isMultiSegmentHashTree := ht2.(*MultiSegmentHashTree)

	if ht2 == nil || !isMultiSegmentHashTree {
		return nil, ErrIllegalArguments
	}

	return ht.hashtree.Diff(msht2.hashtree)
}

func (ht *MultiSegmentHashTree) DiffUsing(ht2 AggregatedHashTree, discriminant *Bitset, digests1, digests2 []Digest) error {
	msht2, isMultiSegmentHashTree := ht2.(*MultiSegmentHashTree)

	if ht2 == nil || !isMultiSegmentHashTree {
		return ErrIllegalArguments
	}

	return ht.hashtree.DiffUsing(msht2.hashtree, discriminant, digests1, digests2)
}
