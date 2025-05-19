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

package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

var (
	SearchOperatorAnd = "SEARCH_OPERATOR_AND"
	SearchOperatorOr  = "SEARCH_OPERATOR_OR"
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
		"minimumShouldMatch": &graphql.InputObjectFieldConfig{
			Description: "Minimum number of term matches required",
			Type:        graphql.Int,
		},
		"searchOperator": &graphql.InputObjectFieldConfig{
			Description: "Search operator",
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: fmt.Sprintf("%sBM25SearchOperatorEnum", prefix),
				Values: graphql.EnumValueConfigMap{
					"and": &graphql.EnumValueConfig{Value: SearchOperatorAnd},
					"or":  &graphql.EnumValueConfig{Value: SearchOperatorOr},
				},
				Description: "Search operator (OR/AND)",
			}),
		},
	}
}
