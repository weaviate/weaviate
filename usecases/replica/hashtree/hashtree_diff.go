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

import "fmt"

func LevelDiff(l int, discriminant *Bitset, digests1, digests2 []Digest) {
	var offset int

	if l > 0 {
		offset = NodesCount(l)
	}

	for j := 0; j < nodesAtLevel(l); j++ {
		node := offset + j

		if !discriminant.IsSet(node) {
			continue
		}

		if digests1[j] == digests2[j] {
			discriminant.Unset(node)
			continue
		}

		leftChild := 2*node + 1
		rightChild := 2*node + 2

		if discriminant.Size() <= rightChild {
			// node is a leaf
			continue
		}

		discriminant.Set(leftChild)
		discriminant.Set(rightChild)
	}
}

func HashTreeDiff(ht1, ht2 *HashTree) (*HashTreeDiffReader, error) {
	if ht1 == nil || ht2 == nil {
		return nil, ErrIllegalArguments
	}

	if ht1.Height() != ht2.Height() {
		return nil, fmt.Errorf("%w: hash trees of different heights are non-comparable", ErrIllegalArguments)
	}

	// init for comparison
	diff := NewBitset(NodesCount(ht1.Height()))

	leavesCount := LeavesCount(ht1.Height())
	digests1 := make([]Digest, leavesCount)
	digests2 := make([]Digest, leavesCount)

	return HashTreeDiffWith(ht1, ht2, diff, digests1, digests2)
}

func HashTreeDiffWith(ht1, ht2 *HashTree, diff *Bitset, digests1, digests2 []Digest) (*HashTreeDiffReader, error) {
	if ht1 == nil || ht2 == nil || diff == nil {
		return nil, ErrIllegalArguments
	}

	if ht1.Height() != ht2.Height() {
		return nil, fmt.Errorf("%w: hash trees of different heights are non-comparable", ErrIllegalArguments)
	}

	if diff.Size() != NodesCount(ht1.Height()) {
		return nil, fmt.Errorf("%w: diff bitset size should mismatch", ErrIllegalArguments)
	}

	diff.Reset().Set(0) // init comparison at root level

	for l := 0; l < ht1.Height(); l++ {
		_, err := ht1.Level(l, diff, digests1)
		if err != nil {
			return nil, err
		}

		_, err = ht2.Level(l, diff, digests2)
		if err != nil {
			return nil, err
		}

		LevelDiff(l, diff, digests1, digests2)

		if diff.SetCount() == 0 {
			// no difference found
			break
		}
	}

	return ht1.NewDiffReader(diff), nil
}
