package filterext

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
)

func Parse(in *models.WhereFilter) (*filters.LocalFilter, error) {

	// TODO: validate

	operator, err := parseOperator(in.Operator)
	if err != nil {
		return nil, err
	}

	if operator.OnValue() {
		return parseValueFilter(in, operator)
	} else {
		return nil, fmt.Errorf("nested filters not supported yet")
	}
}

func parseValueFilter(in *models.WhereFilter, operator filters.Operator) (*filters.LocalFilter, error) {
	value, err := parseValue(in)
	if err != nil {
		return nil, err
	}

	path, err := parsePath(in.Path)
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

func parseOperator(in string) (filters.Operator, error) {
	switch in {
	case models.WhereFilterOperatorEqual:
		return filters.OperatorEqual, nil

	default:
		return -1, fmt.Errorf("unrecognized operator: %s", in)
	}
}

func parsePath(in []string) (*filters.Path, error) {
	asInterface := make([]interface{}, len(in), len(in))
	for i, elem := range in {
		asInterface[i] = elem
	}

	return filters.ParsePath(asInterface, "Todo") // TODO: do we need to set a root class?
}
