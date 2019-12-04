//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package filterext

import (
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
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
		return nil, fmt.Errorf("no value set in filter '%s'", jsonify(in))
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
	// string
	func(in *models.WhereFilter) (*filters.Value, error) {
		if in.ValueString == nil {
			return nil, nil
		}

		return valueFilter(*in.ValueString, schema.DataTypeString), nil
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
