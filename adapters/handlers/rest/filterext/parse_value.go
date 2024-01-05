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

package filterext

import (
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func parseValue(in *models.WhereFilter) (*filters.Value, error) {
	var value *filters.Value

	for _, extractor := range valueExtractors {
		foundValue, err := extractor(in)
		// Abort if we found a value, but it's for being passed a string to an int value.
		if err != nil {
			return nil, err
		}

		if foundValue != nil {
			if value != nil {
				return nil, fmt.Errorf("found more than one values the clause '%s'", jsonify(in))
			} else {
				value = foundValue
			}
		}
	}

	if value == nil {
		return nil, fmt.Errorf("got operator '%s', but no value<Type> field set",
			in.Operator)
	}

	return value, nil
}

type valueExtractorFunc func(*models.WhereFilter) (*filters.Value, error)

var valueExtractors = []valueExtractorFunc{
	// int
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueInt == nil {
			return nil, nil
		}

		return valueFilter(int(*in.ValueInt), schema.DataTypeInt), nil
	},
	// number
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueNumber == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueNumber, schema.DataTypeNumber), nil
	},
	// text
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueText == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueText, schema.DataTypeText), nil
	},
	// date (as string)
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueDate == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueDate, schema.DataTypeDate), nil
	},
	// boolean
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueBoolean == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueBoolean, schema.DataTypeBoolean), nil
	},

	// int array
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueIntArray) == 0 {
			return nil, nil
		}

		valueInts := make([]int, len(in.ValueIntArray))
		for i := range in.ValueIntArray {
			valueInts[i] = int(in.ValueIntArray[i])
		}
		return valueFilter(valueInts, schema.DataTypeInt), nil
	},
	// number array
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueNumberArray) == 0 {
			return nil, nil
		}

		return valueFilter(in.ValueNumberArray, schema.DataTypeNumber), nil
	},
	// text array
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueTextArray) == 0 {
			return nil, nil
		}

		return valueFilter(in.ValueTextArray, schema.DataTypeText), nil
	},
	// date (as string) array
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueDateArray) == 0 {
			return nil, nil
		}

		return valueFilter(in.ValueDateArray, schema.DataTypeDate), nil
	},
	// boolean
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueBooleanArray) == 0 {
			return nil, nil
		}

		return valueFilter(in.ValueBooleanArray, schema.DataTypeBoolean), nil
	},

	// geo range
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueGeoRange == nil {
			return nil, nil
		}

		if in.ValueGeoRange.Distance == nil {
			return nil, fmt.Errorf("valueGeoRange: field 'distance' must be set")
		}

		if in.ValueGeoRange.Distance.Max < 0 {
			return nil, fmt.Errorf("valueGeoRange: field 'distance.max' must be a positive number")
		}

		if in.ValueGeoRange.GeoCoordinates == nil {
			return nil, fmt.Errorf("valueGeoRange: field 'geoCoordinates' must be set")
		}

		return valueFilter(filters.GeoRange{
			Distance: float32(in.ValueGeoRange.Distance.Max),
			GeoCoordinates: &models.GeoCoordinates{
				Latitude:  in.ValueGeoRange.GeoCoordinates.Latitude,
				Longitude: in.ValueGeoRange.GeoCoordinates.Longitude,
			},
		}, schema.DataTypeGeoCoordinates), nil
	},
	// deprecated string
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueString == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueString, schema.DataTypeString), nil
	},
	// deprecated string array
	func(in *models.WhereFilter) (*filters.Value, error) {
		if len(in.ValueStringArray) == 0 {
			return nil, nil
		}

		return valueFilter(in.ValueStringArray, schema.DataTypeString), nil
	},
}

func valueFilter(value interface{}, dt schema.DataType) *filters.Value {
	return &filters.Value{
		Type:  dt,
		Value: value,
	}
}

// Small utility function used in printing error messages.
func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
