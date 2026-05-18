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

package get

import (
	"fmt"

	"github.com/tailor-platform/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
)

func bm25Argument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sHybridGetBm25InpObj", prefix),
				Fields: bm25Fields(prefix),
			},
		),
	}
}

func bm25Fields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			Description: "The query to search for",
			Type:        graphql.String,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "The properties to search in",
			Type:        graphql.NewList(graphql.String),
		},
		"searchOperator": common_filters.GenerateBM25SearchOperatorFields(prefix),
		"highlightMaxFragments": &graphql.InputObjectFieldConfig{
			Description: "Maximum number of highlight fragments per property (default 3)",
			Type:        graphql.Int,
		},
		"highlightFragmentSize": &graphql.InputObjectFieldConfig{
			Description: "Characters of context around each matched term (default 50)",
			Type:        graphql.Int,
		},
	}
}
