//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func mergeAndOptimized(children []*propValuePair,
	acceptDuplicates bool) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// Since the nested filter could have further children which are AND/OR
	// filters, we need to merge the innermost of them first

	// Part 1: Merge Children if any
	// -----------------------------
	// If the given operands are Value filters, merge will simply return the
	// respective values
	for i, child := range children {
		docIDs, err := child.mergeDocIDs(acceptDuplicates)
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
		}

		sets[i] = docIDs
	}

	// Potential early exit condition
	// ------------------------------
	if len(sets) == 1 || checksumsIdentical(sets) {
		// all children are identical, no need to merge, simply return the first
		// set
		return sets[0], nil
	}

	checksum := combineSetChecksums(sets, filters.OperatorAnd)

	// Part 2: Recursively intersect sets
	// ----------------------------------
	// The idea is that we pick the smallest list first and check against it, as
	// building a map is considerably more expensive than a map lookup. So we
	// build a small map first. Since the overall strategy is AND, we know that
	// we will never have more items than on the smallest list (since an id would
	// have to be present on all lists to make it through the merge)

	// Thus we must start by sorting the respective sets by their length in ASC
	// order

	sort.Slice(sets, func(a, b int) bool {
		return len(sets[a].docIDs) < len(sets[b].docIDs)
	})

	// Now we start a recursive merge where merge element 0 and element 1, then
	// remove both from the list. If there are elements left we merge the result
	// of the first round with the next smallest set (originally element 2, now
	// element 0) and so on until we are left with only a single element

	for len(sets) > 1 {
		merged := intersectAnd(sets[0], sets[1])
		sets[0] = nil // set to nil to avoid mem leak, as we are cutting from * slice
		sets[1] = nil // set to nil to avoid mem leak, as we are cutting from * slice
		sets = append([]*docPointers{merged}, sets[2:]...)
	}

	sets[0].checksum = checksum

	return sets[0], nil
}

func intersectAnd(smaller, larger *docPointers) *docPointers {
	lookup := make(map[uint64]struct{}, len(smaller.docIDs))
	eligibile := docPointers{
		docIDs: make([]docPointer, len(smaller.docIDs)),
	}
	for i := range smaller.docIDs {
		lookup[smaller.docIDs[i].id] = struct{}{}
	}

	matches := 0
	for i := range larger.docIDs {
		if _, ok := lookup[larger.docIDs[i].id]; ok {
			eligibile.docIDs[matches] = docPointer{id: larger.docIDs[i].id}
			matches++
		}
	}

	eligibile.docIDs = eligibile.docIDs[:matches]
	eligibile.count = uint64(matches)

	return &eligibile
}

func mergeOrAcceptDuplicates(in []*docPointers) (*docPointers, error) {
	size := 0
	for i := range in {
		size += len(in[i].docIDs)
	}

	out := docPointers{
		docIDs:   make([]docPointer, size),
		checksum: combineSetChecksums(in, filters.OperatorOr),
	}

	index := 0
	for i := range in {
		for j := range in[i].docIDs {
			out.docIDs[index] = in[i].docIDs[j]
			index++
		}
	}

	return &out, nil
}
