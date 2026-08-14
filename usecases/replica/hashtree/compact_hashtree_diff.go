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

package hashtree

func (ht *CompactHashTree) Diff(ht2 AggregatedHashTree) (*Bitset, error) {
	cht2, isCompactHashTree := ht2.(*CompactHashTree)
	if ht2 == nil || !isCompactHashTree {
		return nil, ErrIllegalArguments
	}

	return ht.hashtree.Diff(cht2.hashtree)
}
