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
	"errors"
	"fmt"
)

var ErrNoMoreRanges = errors.New("no more ranges")

type HashTreeDiffReader struct {
	discriminant *Bitset
	pos          int
}

// NewRangeReader expects a leaf-level discriminant (Size() ==
// LeavesCount(ht.Height())); bit i indicates leaf i differs.
func (ht *HashTree) NewRangeReader(discriminant *Bitset) (AggregatedHashTreeRangeReader, error) {
	if discriminant == nil {
		return nil, fmt.Errorf("%w: nil discriminant provided", ErrIllegalArguments)
	}
	expected := LeavesCount(ht.Height())
	if discriminant.Size() != expected {
		return nil, fmt.Errorf("%w: discriminant size %d, expected %d (leaf-level)",
			ErrIllegalArguments, discriminant.Size(), expected)
	}

	return &HashTreeDiffReader{
		discriminant: discriminant,
		pos:          0,
	}, nil
}

func (r *HashTreeDiffReader) Next() (uint64, uint64, error) {
	for ; r.pos < r.discriminant.Size() && !r.discriminant.IsSet(r.pos); r.pos++ {
	}

	if r.pos == r.discriminant.Size() {
		return 0, 0, ErrNoMoreRanges
	}

	pos0 := r.pos

	for ; r.pos < r.discriminant.Size() && r.discriminant.IsSet(r.pos); r.pos++ {
	}

	return uint64(pos0), uint64(r.pos - 1), nil
}
