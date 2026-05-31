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

import (
	"fmt"
	"io"
)

type AggregatedHashTree interface {
	Height() int
	AggregateLeafWith(i uint64, val []byte) error
	Sync()
	Root() Digest
	Level(level int, discriminant *Bitset, digests []Digest) (n int, err error)
	Reset()
	Clone() AggregatedHashTree

	Diff(ht AggregatedHashTree) (discriminant *Bitset, err error)

	NewRangeReader(discriminant *Bitset) (AggregatedHashTreeRangeReader, error)

	Serialize(w io.Writer) (n int64, err error)
}

type AggregatedHashTreeRangeReader interface {
	Next() (uint64, uint64, error)
}

// LevelDiff compares level-l digests1 and digests2, clearing matched bits in
// discriminant. For l < height it returns a level-(l+1) discriminant with
// the children of mismatched nodes set; at l == height it returns nil.
func LevelDiff(l, height int, discriminant *Bitset, digests1, digests2 []Digest) (nextDiscriminant *Bitset, levelDiffCount int, err error) {
	if l < 0 {
		return nil, 0, fmt.Errorf("%w: invalid level(%d)", ErrIllegalArguments, l)
	}
	if l > height {
		return nil, 0, fmt.Errorf("%w: level(%d) is too high for height(%d)", ErrIllegalState, l, height)
	}
	if discriminant == nil {
		return nil, 0, fmt.Errorf("%w: nil discriminant provided", ErrIllegalArguments)
	}

	expected := nodesAtLevel(l)
	if discriminant.Size() != expected {
		return nil, 0, fmt.Errorf("%w: discriminant size %d, expected %d for level %d",
			ErrIllegalArguments, discriminant.Size(), expected, l)
	}

	// digests1/digests2 hold one entry per set bit, not per Size().
	setCount := discriminant.SetCount()
	if len(digests1) < setCount || len(digests2) < setCount {
		return nil, 0, fmt.Errorf("%w: digests slice too short for level %d (have %d/%d, need >= %d)",
			ErrIllegalArguments, l, len(digests1), len(digests2), setCount)
	}

	if l < height {
		nextDiscriminant = NewBitset(nodesAtLevel(l + 1))
	}

	n := 0
	for j := 0; j < discriminant.Size(); j++ {
		if !discriminant.IsSet(j) {
			continue
		}

		if digests1[n] == digests2[n] {
			discriminant.Unset(j)
			n++
			continue
		}
		levelDiffCount++
		n++

		if nextDiscriminant != nil {
			nextDiscriminant.Set(2 * j)
			nextDiscriminant.Set(2*j + 1)
		}
	}

	return nextDiscriminant, levelDiffCount, nil
}
