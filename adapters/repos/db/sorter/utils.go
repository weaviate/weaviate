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

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
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

// extractDistanceOrigins returns, per sort level, the point a distance sort
// measures from, or nil for a sort by value.
func extractDistanceOrigins(sort []filters.Sort) []*filters.SortGeoOrigin {
	origins := make([]*filters.SortGeoOrigin, len(sort))
	for i, srt := range sort {
		origins[i] = srt.DistanceFrom
	}
	return origins
}

// validateDistanceOrigins errors when a distance sort is not on a
// geoCoordinates property: there is no distance to order by.
func validateDistanceOrigins(dataTypesHelper *dataTypesHelper, propNames []string,
	origins []*filters.SortGeoOrigin,
) error {
	for level, origin := range origins {
		if origin == nil {
			continue
		}
		if dt := dataTypesHelper.getType(propNames[level]); dt != schema.DataTypeGeoCoordinates {
			return errors.Errorf("sorting by distance needs a geoCoordinates property, %q is of type %q",
				propNames[level], dt)
		}
	}
	return nil
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
