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
