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

import "fmt"

func (ht *HashTree) Diff(ht2 AggregatedHashTree) (discriminant *Bitset, err error) {
	_, isHashTree := ht2.(*HashTree)

	if ht2 == nil || !isHashTree {
		return nil, ErrIllegalArguments
	}

	if ht.Height() != ht2.Height() {
		return nil, fmt.Errorf("%w: hash trees of different heights are non-comparable", ErrIllegalArguments)
	}

	// init for comparison
	discriminant = NewBitset(NodesCount(ht.Height()))

	leavesCount := LeavesCount(ht.Height())
	digests1 := make([]Digest, leavesCount)
	digests2 := make([]Digest, leavesCount)

	err = ht.DiffUsing(ht2, discriminant, digests1, digests2)
	if err != nil {
		return nil, err
	}

	return discriminant, nil
}

func (ht *HashTree) DiffUsing(ht2 AggregatedHashTree, discriminant *Bitset, digests1, digests2 []Digest) error {
	_, isHashTree := ht2.(*HashTree)

	if ht2 == nil || !isHashTree {
		return ErrIllegalArguments
	}

	if discriminant == nil {
		return ErrIllegalArguments
	}

	if ht.Height() != ht2.Height() {
		return fmt.Errorf("%w: hash trees of different heights are non-comparable", ErrIllegalArguments)
	}

	if discriminant.Size() != NodesCount(ht.Height()) {
		return fmt.Errorf("%w: diff bitset size should mismatch", ErrIllegalArguments)
	}

	discriminant.Reset().Set(0) // init comparison at root level

	for l := 0; l <= ht.Height(); l++ {
		_, err := ht.Level(l, discriminant, digests1)
		if err != nil {
			return err
		}

		_, err = ht2.Level(l, discriminant, digests2)
		if err != nil {
			return err
		}

		LevelDiff(l, discriminant, digests1, digests2)

		if discriminant.SetCount() == 0 {
			// no difference found
			break
		}
	}

	return nil
}
