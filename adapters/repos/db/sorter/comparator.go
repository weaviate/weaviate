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

import "github.com/weaviate/weaviate/entities/filters"

type comparator struct {
	comparators []basicComparator
}

// A level with an origin compares distances from it instead of values.
func newComparator(dataTypesHelper *dataTypesHelper, propNames []string, orders []string,
	origins []*filters.SortGeoOrigin,
) *comparator {
	provider := &basicComparatorProvider{}
	comparators := make([]basicComparator, len(propNames))
	for level, propName := range propNames {
		if origins[level] != nil {
			comparators[level] = newDistanceComparator(orders[level])
			continue
		}
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
	return 0
}
