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

package common

import (
	"encoding/json"
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/aggregation"
)

// JSONNumberResolver turns json.Number types into number types usable by graphQL
func JSONNumberResolver(p graphql.ResolveParams) (interface{}, error) {
	switch v := p.Source.(type) {
	case map[string]interface{}:
		field, ok := v[p.Info.FieldName]
		if !ok {
			return nil, nil
		}

		switch n := field.(type) {
		case json.Number:
			return n.Float64()
		case int64:
			return float64(n), nil
		case int:
			return float64(n), nil
		case float64:
			return n, nil
		}

		return nil, fmt.Errorf("unknown number type for %t", field)

	case map[string]float64:
		return v[p.Info.FieldName], nil

	case aggregation.Text:
		switch p.Info.FieldName {
		// case "count":
		// 	// TODO gh-974: Support Count in text aggregations
		// 	return nil, nil

		default:
			return nil, fmt.Errorf("fieldName '%s' does not match text aggregation", p.Info.FieldName)
		}

	default:
		return nil, fmt.Errorf("json number resolver: unusable type %T", p.Source)
	}
}
