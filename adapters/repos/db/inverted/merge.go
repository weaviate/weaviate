package inverted

import (
	"sort"

	"github.com/pkg/errors"
)

func mergeAndOptimized(children []*propValuePair) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// retrieve child IDs
	for i, child := range children {
		docIDs, err := child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
		}

		sets[i] = docIDs
	}

	if len(sets) == 1 || checksumsIdentical(sets) {
		// all children are identical, no need to merge, simply return the first
		// set
		return sets[0], nil
	}

	// merge AND

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

	// TODO: implement
	if len(sets) > 2 {
		panic("support for more than 2 not implement yet")
	}

	lookup := make(map[uint64]struct{}, len(sets[0].docIDs))
	eligibile := docPointers{
		docIDs: make([]docPointer, len(sets[0].docIDs)),
	}
	for i := range sets[0].docIDs {
		lookup[sets[0].docIDs[i].id] = struct{}{}
	}

	matches := 0
	for i := range sets[1].docIDs {
		if _, ok := lookup[sets[1].docIDs[i].id]; ok {
			eligibile.docIDs[matches] = docPointer{id: sets[1].docIDs[i].id}
			matches++
		}
	}

	eligibile.docIDs = eligibile.docIDs[:matches]

	return &eligibile, nil
}
