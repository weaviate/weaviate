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

package sorter

type comparator struct {
	comparators []basicComparator
}

func newComparator(dataTypesHelper *dataTypesHelper, propNames []string, orders []string) *comparator {
	provider := &basicComparatorProvider{}
	comparators := make([]basicComparator, len(propNames))
	for level, propName := range propNames {
		dataType := dataTypesHelper.getType(propName)
		comparators[level] = provider.provide(dataType, orders[level])
	}
	return &comparator{comparators}
}

func (c *comparator) compare(a, b *comparable) int {
	for level, comparator := range c.comparators {
		if res := comparator.compare(a.values[level], b.values[level]); res != 0 {
			return res
		}
	}
	// Tie-breaker: use document ID for deterministic ordering, consistent with the
	// other cross-shard merges (see sortByDistances, sortByScores, sortByID). Without
	// it, objects that tie on every sort key but live on different shards get a
	// nondeterministic order in the cross-shard merge (an unstable sort over results
	// appended in goroutine-completion order), so a boundary object can be dropped from
	// or duplicated across paginated (offset/limit) requests.
	if a.docID != b.docID {
		if a.docID < b.docID {
			return -1
		}
		return 1
	}
	return 0
}
