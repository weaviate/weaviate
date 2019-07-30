//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package common

import (
	"encoding/json"
	"fmt"

	"github.com/graphql-go/graphql"
)

// JSONNumberResolver turns json.Number types into number types usable by graphQL
func JSONNumberResolver(p graphql.ResolveParams) (interface{}, error) {
	sourceMap, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("source is not a map, but %t", p.Source)
	}

	field, ok := sourceMap[p.Info.FieldName]
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
}
