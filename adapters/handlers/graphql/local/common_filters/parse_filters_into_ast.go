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
			return nil, fmt.Errorf("failed to extract filters: %w", err)
		}

		// GraphQL is disabled on namespace-enabled clusters, so the
		// namespacesEnabled flag is hard-wired to false here and the
		// principal isn't consulted by Parse for path qualification.
		return filterext.Parse(filter, rootClass, false, nil)
	}
}

func filterMapToModel(m map[string]interface{}) (*models.WhereFilter, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %w", err)
	}

	var filter WhereFilter
	err = json.Unmarshal(b, &filter)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %w", err)
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
		case int:
			val := int64(v)
			whereFilter.ValueInt = &val
		case int64:
			whereFilter.ValueInt = &v
		case []int:
			ints := make([]int64, len(v))
			for i, n := range v {
				ints[i] = int64(n)
			}
			whereFilter.ValueIntArray = ints
		case []int64:
			whereFilter.ValueIntArray = v
		case []interface{}:
			ints := make([]int64, len(v))
			for i := range v {
				switch elem := v[i].(type) {
				case float64:
					ints[i] = int64(elem)
				case int:
					ints[i] = int64(elem)
				case int64:
					ints[i] = elem
				default:
					return nil, fmt.Errorf("unsupported type in ValueInt array: '%T'", v[i])
				}
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
		case []float64:
			whereFilter.ValueNumberArray = v
		case []interface{}:
			numbers := make([]float64, len(v))
			for i := range v {
				switch elem := v[i].(type) {
				case float64:
					numbers[i] = elem
				case int:
					numbers[i] = float64(elem)
				default:
					return nil, fmt.Errorf("unsupported type in ValueNumber array: '%T'", v[i])
				}
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
		case []bool:
			whereFilter.ValueBooleanArray = v
		case []interface{}:
			bools := make([]bool, len(v))
			for i := range v {
				b, ok := v[i].(bool)
				if !ok {
					return nil, fmt.Errorf("unsupported type in ValueBoolean array: '%T'", v[i])
				}
				bools[i] = b
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
	case []string:
		valueArray = v
	case []interface{}:
		valueArray = make([]string, len(v))
		for i := range v {
			if s, ok := v[i].(string); ok {
				valueArray[i] = s
			} else {
				return nil, nil, fmt.Errorf("unsupported element type in string array: '%T'", v[i])
			}
		}
	default:
		err = fmt.Errorf("unsupported type: '%T'", in)
	}
	return value, valueArray, err
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
