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
	return 0
}
