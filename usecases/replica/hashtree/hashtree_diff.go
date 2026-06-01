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

import "fmt"

// Diff returns a leaf-level discriminant; bit i is set when leaf i differs.
func (ht *HashTree) Diff(ht2 AggregatedHashTree) (*Bitset, error) {
	_, isHashTree := ht2.(*HashTree)
	if ht2 == nil || !isHashTree {
		return nil, ErrIllegalArguments
	}

	height := ht.Height()
	if height != ht2.Height() {
		return nil, fmt.Errorf("%w: hash trees of different heights are non-comparable", ErrIllegalArguments)
	}

	leavesCount := LeavesCount(height)
	digests1 := make([]Digest, leavesCount)
	digests2 := make([]Digest, leavesCount)

	walk := NewBitset(1)
	walk.Set(0) // root

	for l := 0; l <= height; l++ {
		if _, err := ht.Level(l, walk, digests1); err != nil {
			return nil, err
		}
		if _, err := ht2.Level(l, walk, digests2); err != nil {
			return nil, err
		}

		nextWalk, levelDiffCount, err := LevelDiff(l, height, walk, digests1, digests2)
		if err != nil {
			return nil, err
		}

		if l == height {
			return walk, nil
		}
		if levelDiffCount == 0 {
			return NewBitset(leavesCount), nil
		}
		walk = nextWalk
	}

	return NewBitset(leavesCount), nil
}
