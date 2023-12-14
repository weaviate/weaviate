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

func CompactHashTreeDiff(ht1, ht2 *CompactHashTree) (diffReader *CompactHashTreeDiffReader, err error) {
	if ht1 == nil || ht2 == nil {
		return nil, ErrIllegalArguments
	}

	r, err := HashTreeDiff(ht1.hashtree, ht2.hashtree)
	if err != nil {
		return nil, err
	}

	return ht1.NewDiffReader(r), nil
}

func CompactHashTreeDiffWith(ht1, ht2 *CompactHashTree, diff *Bitset, digests1, digests2 []Digest) (diffReader *CompactHashTreeDiffReader, err error) {
	if ht1 == nil || ht2 == nil {
		return nil, ErrIllegalArguments
	}

	r, err := HashTreeDiffWith(ht1.hashtree, ht2.hashtree, diff, digests1, digests2)
	if err != nil {
		return nil, err
	}

	return ht1.NewDiffReader(r), nil
}
