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

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/filters"
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
