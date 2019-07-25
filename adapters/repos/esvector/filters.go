package esvector

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func queryFromFilter(f *filters.LocalFilter) (map[string]interface{}, error) {

	if f == nil {
		return map[string]interface{}{
			"match_all": map[string]interface{}{},
		}, nil
	}

	m, err := matcherFromOperator(f.Root.Operator)
	if err != nil {
		return nil, err
	}

	filter := map[string]interface{}{
		m.queryType: map[string]interface{}{
			f.Root.On.Property.String(): map[string]interface{}{
				m.operator: f.Root.Value.Value,
			},
		},
	}

	q := map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": filter,
		},
	}

	spew.Dump(q)
	return q, nil
}

type matcher struct {
	queryType string
	operator  string
}

func matcherFromOperator(o filters.Operator) (m matcher, err error) {
	switch o {
	case filters.OperatorEqual:
		m.queryType = "term"
		m.operator = "value"
	case filters.OperatorLessThan:
		m.queryType = "range"
		m.operator = "lt"
	case filters.OperatorLessThanEqual:
		m.queryType = "range"
		m.operator = "lte"
	case filters.OperatorGreaterThan:
		m.queryType = "range"
		m.operator = "gt"
	case filters.OperatorGreaterThanEqual:
		m.queryType = "range"
		m.operator = "gte"
	default:
		err = fmt.Errorf("unsupported operator")
	}

	return
}
