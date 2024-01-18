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

package common_filters

import (
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// Extract the filters from the arguments of a Local->Get or Local->Meta query.
func ExtractFilters(args map[string]interface{}, rootClass string) (*filters.LocalFilter, error) {
	where, wherePresent := args["where"]
	if !wherePresent {
		// No filters; all is fine!
		return nil, nil
	} else {
		whereMap := where.(map[string]interface{}) // guaranteed by GraphQL to be a map.
		filter, err := filterMapToModel(whereMap)
		if err != nil {
			return nil, fmt.Errorf("failed to extract filters: %s", err)
		}

		return filterext.Parse(filter, rootClass)
	}
}

func filterMapToModel(m map[string]interface{}) (*models.WhereFilter, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %s", err)
	}

	var filter WhereFilter
	err = json.Unmarshal(b, &filter)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %s", err)
	}

	return newConverter().do(&filter)
}

type converter struct{}

func newConverter() *converter {
	return &converter{}
}

func (c *converter) do(in *WhereFilter) (*models.WhereFilter, error) {
	whereFilter := &models.WhereFilter{
		Operator: in.Operator,
		Path:     in.Path,
	}

	if in.ValueInt != nil {
		switch v := in.ValueInt.(type) {
		case float64:
			val := int64(v)
			whereFilter.ValueInt = &val
		case []interface{}:
			ints := make([]int64, len(v))
			for i := range v {
				ints[i] = int64(v[i].(float64))
			}
			whereFilter.ValueIntArray = ints
		default:
			return nil, fmt.Errorf("unsupported type: '%T'", in.ValueInt)
		}
	}
	if in.ValueNumber != nil {
		switch v := in.ValueNumber.(type) {
		case float64:
			whereFilter.ValueNumber = &v
		case []interface{}:
			numbers := make([]float64, len(v))
			for i := range v {
				numbers[i] = v[i].(float64)
			}
			whereFilter.ValueNumberArray = numbers
		default:
			return nil, fmt.Errorf("unsupported type: '%T'", in.ValueNumber)
		}
	}
	if in.ValueBoolean != nil {
		switch v := in.ValueBoolean.(type) {
		case bool:
			whereFilter.ValueBoolean = &v
		case []interface{}:
			bools := make([]bool, len(v))
			for i := range v {
				bools[i] = v[i].(bool)
			}
			whereFilter.ValueBooleanArray = bools
		default:
			return nil, fmt.Errorf("unsupported type: '%T'", in.ValueBoolean)
		}
	}
	if in.ValueString != nil {
		value, valueArray, err := c.parseString(in.ValueString)
		if err != nil {
			return nil, err
		}
		whereFilter.ValueString = value
		whereFilter.ValueStringArray = valueArray
	}
	if in.ValueText != nil {
		value, valueArray, err := c.parseString(in.ValueText)
		if err != nil {
			return nil, err
		}
		whereFilter.ValueText = value
		whereFilter.ValueTextArray = valueArray
	}
	if in.ValueDate != nil {
		value, valueArray, err := c.parseString(in.ValueDate)
		if err != nil {
			return nil, err
		}
		whereFilter.ValueDate = value
		whereFilter.ValueDateArray = valueArray
	}
	if in.ValueGeoRange != nil {
		whereFilter.ValueGeoRange = in.ValueGeoRange
	}

	// recursively build operands
	for i, op := range in.Operands {
		whereFilterOp, err := c.do(op)
		if err != nil {
			return nil, fmt.Errorf("operands[%v]: %w", i, err)
		}
		whereFilter.Operands = append(whereFilter.Operands, whereFilterOp)
	}

	return whereFilter, nil
}

func (c *converter) parseString(in interface{}) (value *string, valueArray []string, err error) {
	switch v := in.(type) {
	case string:
		value = &v
	case []interface{}:
		valueArray = make([]string, len(v))
		for i := range v {
			valueArray[i] = v[i].(string)
		}
	default:
		err = fmt.Errorf("unsupported type: '%T'", in)
	}
	return
}

type WhereFilter struct {
	Operands      []*WhereFilter              `json:"operands"`
	Operator      string                      `json:"operator,omitempty"`
	Path          []string                    `json:"path"`
	ValueBoolean  interface{}                 `json:"valueBoolean,omitempty"`
	ValueDate     interface{}                 `json:"valueDate,omitempty"`
	ValueInt      interface{}                 `json:"valueInt,omitempty"`
	ValueNumber   interface{}                 `json:"valueNumber,omitempty"`
	ValueString   interface{}                 `json:"valueString,omitempty"`
	ValueText     interface{}                 `json:"valueText,omitempty"`
	ValueGeoRange *models.WhereFilterGeoRange `json:"valueGeoRange,omitempty"`
}
