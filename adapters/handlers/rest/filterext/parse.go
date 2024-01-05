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
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// Parse Filter from REST construct to entities filter
func Parse(in *models.WhereFilter, rootClass string) (*filters.LocalFilter, error) {
	if in == nil {
		return nil, nil
	}

	operator, err := parseOperator(in.Operator)
	if err != nil {
		return nil, err
	}

	if operator.OnValue() {
		filter, err := parseValueFilter(in, operator, rootClass)
		if err != nil {
			return nil, fmt.Errorf("invalid where filter: %v", err)
		}
		return filter, nil
	}

	filter, err := parseNestedFilter(in, operator, rootClass)
	if err != nil {
		return nil, fmt.Errorf("invalid where filter: %v", err)
	}
	return filter, nil
}

func parseValueFilter(in *models.WhereFilter,
	operator filters.Operator, rootClass string,
) (*filters.LocalFilter, error) {
	value, err := parseValue(in)
	if err != nil {
		return nil, err
	}

	path, err := parsePath(in.Path, rootClass)
	if err != nil {
		return nil, err
	}

	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			Value:    value,
			On:       path,
		},
	}, nil
}

func parseNestedFilter(in *models.WhereFilter,
	operator filters.Operator, rootClass string,
) (*filters.LocalFilter, error) {
	if in.Path != nil {
		return nil, fmt.Errorf(
			"operator '%s' not compatible with field 'path', remove 'path' "+
				"or switch to compare operator (eg. Equal, NotEqual, etc.)",
			operator.Name())
	}

	if !allValuesNil(in) {
		return nil, fmt.Errorf(
			"operator '%s' not compatible with field 'value<Type>', "+
				"remove value field or switch to compare operator "+
				"(eg. Equal, NotEqual, etc.)",
			operator.Name())
	}

	if in.Operands == nil || len(in.Operands) == 0 {
		return nil, fmt.Errorf(
			"operator '%s', but no operands set - add at least one operand",
			operator.Name())
	}

	operands, err := parseOperands(in.Operands, rootClass)
	if err != nil {
		return nil, err
	}

	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			Operands: operands,
		},
	}, nil
}

func parseOperands(ops []*models.WhereFilter, rootClass string) ([]filters.Clause, error) {
	out := make([]filters.Clause, len(ops))
	for i, operand := range ops {
		res, err := Parse(operand, rootClass)
		if err != nil {
			return nil, fmt.Errorf("operand %d: %v", i, err)
		}

		out[i] = *res.Root
	}

	return out, nil
}

func parseOperator(in string) (filters.Operator, error) {
	switch in {
	case models.WhereFilterOperatorEqual:
		return filters.OperatorEqual, nil
	case models.WhereFilterOperatorLike:
		return filters.OperatorLike, nil
	case models.WhereFilterOperatorLessThan:
		return filters.OperatorLessThan, nil
	case models.WhereFilterOperatorLessThanEqual:
		return filters.OperatorLessThanEqual, nil
	case models.WhereFilterOperatorGreaterThan:
		return filters.OperatorGreaterThan, nil
	case models.WhereFilterOperatorGreaterThanEqual:
		return filters.OperatorGreaterThanEqual, nil
	case models.WhereFilterOperatorNotEqual:
		return filters.OperatorNotEqual, nil
	case models.WhereFilterOperatorWithinGeoRange:
		return filters.OperatorWithinGeoRange, nil
	case models.WhereFilterOperatorAnd:
		return filters.OperatorAnd, nil
	case models.WhereFilterOperatorOr:
		return filters.OperatorOr, nil
	case models.WhereFilterOperatorIsNull:
		return filters.OperatorIsNull, nil
	case models.WhereFilterOperatorContainsAny:
		return filters.ContainsAny, nil
	case models.WhereFilterOperatorContainsAll:
		return filters.ContainsAll, nil
	default:
		return -1, fmt.Errorf("unrecognized operator: %s", in)
	}
}

func parsePath(in []string, rootClass string) (*filters.Path, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("field 'path': must have at least one element")
	}

	pathElements := make([]interface{}, len(in))
	for i, elem := range in {
		pathElements[i] = elem
	}

	return filters.ParsePath(pathElements, rootClass)
}

func allValuesNil(in *models.WhereFilter) bool {
	return in.ValueBoolean == nil &&
		in.ValueDate == nil &&
		in.ValueString == nil &&
		in.ValueText == nil &&
		in.ValueInt == nil &&
		in.ValueNumber == nil &&
		in.ValueGeoRange == nil &&
		len(in.ValueBooleanArray) == 0 &&
		len(in.ValueDateArray) == 0 &&
		len(in.ValueStringArray) == 0 &&
		len(in.ValueTextArray) == 0 &&
		len(in.ValueIntArray) == 0 &&
		len(in.ValueNumberArray) == 0
}
