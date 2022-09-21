package sorter

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func extractPropNamesAndOrders(sort []filters.Sort) ([]string, []string, error) {
	propNames := make([]string, len(sort))
	orders := make([]string, len(sort))

	for i, srt := range sort {
		if len(srt.Path) == 0 {
			return nil, nil, errors.New("path parameter cannot be empty")
		}
		if len(srt.Path) > 1 {
			return nil, nil, errors.New("sorting by reference not supported, path must have exactly one argument")
		}
		propNames[i] = srt.Path[0]
		orders[i] = srt.Order
	}
	return propNames, orders, nil
}

func validateLimit(limit, elementsCount int) int {
	if limit > elementsCount {
		return elementsCount
	}
	if limit < 0 {
		return 0
	}
	return limit
}
