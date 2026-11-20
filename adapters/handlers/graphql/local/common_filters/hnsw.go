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
	"fmt"

	"github.com/tailor-platform/graphql"
	"github.com/weaviate/weaviate/entities/searchparams"
)

const hnswArgumentDescription = "HNSW-only search parameters. Applies to the vector search in this query (nearVector, nearObject, hybrid, nearText and the other near* searches). Other index types reject ef."

var hnswEFDescription = fmt.Sprintf(
	"Size of the HNSW candidate list for this query. Overrides the collection's vectorIndexConfig.ef, including dynamic ef. Must be between 1 and %d. When it is smaller than the number of neighbors requested, the search raises it to that number so results are not truncated. The index also caps ef to the number of indexed nodes.",
	searchparams.MaxEF,
)

// HNSWArgument is the GraphQL `hnsw { ef }` input shared by Get, Aggregate and Explore.
func HNSWArgument(prefix string) *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: hnswArgumentDescription,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sHNSWInpObj", prefix),
				Description: hnswArgumentDescription,
				Fields: graphql.InputObjectConfigFieldMap{
					"ef": &graphql.InputObjectFieldConfig{
						Description: hnswEFDescription,
						Type:        graphql.Int,
					},
				},
			},
		),
	}
}

// ExtractHNSW reads the optional hnsw search argument. A missing argument, or
// an hnsw object without ef, returns a nil override.
func ExtractHNSW(args map[string]interface{}) (*searchparams.HNSW, error) {
	raw, ok := args["hnsw"]
	if !ok || raw == nil {
		return nil, nil
	}
	source, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("hnsw must be an object")
	}
	efRaw, ok := source["ef"]
	if !ok || efRaw == nil {
		return nil, nil
	}
	ef, ok := efRaw.(int)
	if !ok {
		return nil, fmt.Errorf("hnsw.ef must be an integer")
	}
	params := &searchparams.HNSW{EF: &ef}
	if err := params.Validate(); err != nil {
		return nil, err
	}
	return params, nil
}
