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

import "errors"

var ErrNoMoreDifferences = errors.New("no more differences")

type HashTreeDiffReader struct {
	discriminant *Bitset
	firstLeafPos int
	pos          int
}

func (ht *HashTree) NewDiffReader(discriminant *Bitset) AggregatedHashTreeDiffReader {
	if discriminant == nil || discriminant.Size() != NodesCount(ht.Height()) {
		panic("illegal discriminant")
	}

	firstLeafPos := NodesCount(ht.Height() - 1)

	return &HashTreeDiffReader{
		discriminant: discriminant,
		firstLeafPos: firstLeafPos,
		pos:          firstLeafPos,
	}
}

func (r *HashTreeDiffReader) Next() (uint64, uint64, error) {
	for ; r.pos < r.discriminant.Size() && !r.discriminant.IsSet(r.pos); r.pos++ {
	}

	if r.pos == r.discriminant.Size() {
		return 0, 0, ErrNoMoreDifferences
	}

	pos0 := r.pos

	for ; r.pos < r.discriminant.Size() && r.discriminant.IsSet(r.pos); r.pos++ {
	}

	return uint64(pos0 - r.firstLeafPos), uint64(r.pos - 1 - r.firstLeafPos), nil
}
