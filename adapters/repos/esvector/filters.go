package esvector

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
)

func queryFromFilter(f *filters.LocalFilter) (map[string]interface{}, error) {
	if f == nil {
		return map[string]interface{}{
			"match_all": map[string]interface{}{},
		}, nil
	}

	return queryFromClause(f.Root)
}

func queryFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	switch clause.Operator {
	case filters.OperatorAnd, filters.OperatorOr, filters.OperatorNot:
		return compoundQueryFromClause(clause)
	default:
		return singleQueryFromClause(clause)
	}
}

func singleQueryFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	filter, err := filterFromClause(clause)
	if err != nil {
		return nil, err
	}

	if clause.Operator == filters.OperatorNotEqual {
		filter = negateFilter(filter)
	}

	q := map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": filter,
		},
	}

	return q, nil
}

func filterFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	if clause.Operator == filters.OperatorWithinGeoRange {
		return geoFilterFromClause(clause)
	}

	return primitiveFilterFromClause(clause)

	// TODO: check for cross-refs
}

func geoFilterFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	geoRange, ok := clause.Value.Value.(filters.GeoRange)
	if !ok {
		return nil, fmt.Errorf("got WithinGeoRange operator, but value was not a GeoRange")
	}

	return map[string]interface{}{
		"geo_distance": map[string]interface{}{
			"distance": geoRange.Distance,
			clause.On.Property.String(): map[string]interface{}{
				"lat": geoRange.Latitude,
				"lon": geoRange.Longitude,
			},
		},
	}, nil
}

func primitiveFilterFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	m, err := matcherFromOperator(clause.Operator)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		m.queryType: map[string]interface{}{
			clause.On.Property.String(): map[string]interface{}{
				m.operator: clause.Value.Value,
			},
		},
	}, nil

}

func compoundQueryFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	filters := make([]map[string]interface{}, len(clause.Operands), len(clause.Operands))
	for i, operand := range clause.Operands {
		filter, err := queryFromClause(&operand)
		if err != nil {
			return nil, fmt.Errorf("compund query at pos %d: %v", i, err)
		}
		filters[i] = filter
	}

	combinator, err := combinatorFromOperator(clause.Operator)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			combinator: filters,
		},
	}, nil
}

type matcher struct {
	queryType string
	operator  string
}

func matcherFromOperator(o filters.Operator) (m matcher, err error) {
	switch o {
	case filters.OperatorEqual:
		m.queryType = "match"
		m.operator = "query"
	case filters.OperatorNotEqual:
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

func combinatorFromOperator(o filters.Operator) (string, error) {
	switch o {
	case filters.OperatorAnd:
		return "must", nil
	case filters.OperatorOr:
		return "should", nil
	case filters.OperatorNot:
		return "must_not", nil
	default:
		return "", fmt.Errorf("unrecognized operator %s in compound query", o.Name())
	}
}

func negateFilter(f map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"bool": map[string]interface{}{
			"must_not": f,
		},
	}
}
