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
	diff         *Bitset
	firstLeafPos int
	pos          int
}

func (ht *HashTree) NewDiffReader(diff *Bitset) *HashTreeDiffReader {
	if diff == nil || diff.Size() != NodesCount(ht.Height()) {
		panic("illegal diff")
	}

	firstLeafPos := NodesCount(ht.Height() - 1)

	return &HashTreeDiffReader{
		diff:         diff,
		firstLeafPos: firstLeafPos,
		pos:          firstLeafPos,
	}
}

func (r *HashTreeDiffReader) Next() (int, int, error) {
	for ; r.pos < r.diff.Size() && !r.diff.IsSet(r.pos); r.pos++ {
	}

	if r.pos == r.diff.Size() {
		return 0, 0, ErrNoMoreDifferences
	}

	pos0 := r.pos

	for ; r.pos < r.diff.Size() && r.diff.IsSet(r.pos); r.pos++ {
	}

	return pos0 - r.firstLeafPos, r.pos - 1 - r.firstLeafPos, nil
}
