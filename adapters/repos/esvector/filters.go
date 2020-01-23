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

package esvector

import (
	"context"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/filters"
)

// new from 0.22.0: queryFromFilter can error with SubQueryNoResultsErr, it is
// up to the caller to decide how to handle this.
func (r *Repo) queryFromFilter(ctx context.Context, f *filters.LocalFilter) (map[string]interface{}, error) {
	if f == nil {
		return map[string]interface{}{
			"match_all": map[string]interface{}{},
		}, nil
	}

	return r.queryFromClause(ctx, f.Root)
}

func (r *Repo) queryFromClause(ctx context.Context, clause *filters.Clause) (map[string]interface{}, error) {
	switch clause.Operator {
	case filters.OperatorAnd, filters.OperatorOr, filters.OperatorNot:
		return r.compoundQueryFromClause(ctx, clause)
	default:
		return r.singleQueryFromClause(ctx, clause)
	}
}

func (r *Repo) singleQueryFromClause(ctx context.Context, clause *filters.Clause) (map[string]interface{}, error) {
	filter, err := r.filterFromClause(ctx, clause)
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

func (r *Repo) filterFromClause(ctx context.Context, clause *filters.Clause) (map[string]interface{}, error) {

	if clause.On.Child != nil {
		sqb := newSubQueryBuilder(r)
		res, err := sqb.fromClause(ctx, clause)
		if err != nil {
			switch err.(type) {
			case SubQueryNoResultsErr:
				return nil, err // don't annotate, so we can inspect it down the line

			default:
				return nil, fmt.Errorf("sub query: %v", err)

			}
		}

		x := storageIdentifiersToBeaconBoolFilter(res, clause.On.Property.String())
		return x, nil
	}

	if clause.Operator == filters.OperatorWithinGeoRange {
		return geoFilterFromClause(clause)
	}

	return primitiveFilterFromClause(clause)
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

	if clause.On.Property == "uuid" {
		clause.On.Property = "_id"
	}

	return map[string]interface{}{
		m.queryType: map[string]interface{}{
			clause.On.Property.String(): map[string]interface{}{
				m.operator: clause.Value.Value,
			},
		},
	}, nil

}

func refGeoFilterFromClause(clause *filters.Clause) (map[string]interface{}, error) {
	geoRange, ok := clause.Value.Value.(filters.GeoRange)
	if !ok {
		return nil, fmt.Errorf("got WithinGeoRange operator, but value was not a GeoRange")
	}

	return map[string]interface{}{
		"geo_distance": map[string]interface{}{
			"distance": geoRange.Distance,
			innerPath(clause.On): map[string]interface{}{
				"lat": geoRange.Latitude,
				"lon": geoRange.Longitude,
			},
		},
	}, nil
}

func (r *Repo) compoundQueryFromClause(ctx context.Context, clause *filters.Clause) (map[string]interface{}, error) {
	filters := make([]map[string]interface{}, len(clause.Operands), len(clause.Operands))
	for i, operand := range clause.Operands {
		filter, err := r.queryFromClause(ctx, &operand)
		if err != nil {
			if _, ok := err.(SubQueryNoResultsErr); ok {
				// don't annotate so we can catch this one down the line
				return nil, err
			}
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
	case filters.OperatorLike:
		m.queryType = "wildcard"
		m.operator = "value"
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

func innerPath(p *filters.Path) string {
	return strings.Join(p.SliceNonTitleized(), ".")
}
