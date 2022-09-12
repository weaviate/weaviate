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
