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

var (
	SearchOperatorAnd      = "OPERATOR_AND"
	SearchOperatorOr       = "OPERATOR_OR"
	SearchOperatorAndCross = "OPERATOR_AND_CROSS"
)

func IsAndOperator(operator string) bool {
	return operator == SearchOperatorAnd || operator == SearchOperatorAndCross
}

func GenerateBM25SearchOperatorFields(prefixName string) *graphql.InputObjectFieldConfig {
	searchesPrefixName := prefixName + "Searches"
	return &graphql.InputObjectFieldConfig{
		Description: fmt.Sprintf("The search operator to use for the %s", searchesPrefixName),
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: searchesPrefixName,
				Fields: graphql.InputObjectConfigFieldMap{
					"operator": &graphql.InputObjectFieldConfig{
						Description: "The search operator to use",
						Type: graphql.NewEnum(graphql.EnumConfig{ // EnumConfig is a struct that defines the enum
							Name: fmt.Sprintf("%sOperator", searchesPrefixName),
							Values: graphql.EnumValueConfigMap{
								"And": &graphql.EnumValueConfig{
									Value:       SearchOperatorAnd,
									Description: "All tokens must match",
								},
								"Or": &graphql.EnumValueConfig{
									Value:       SearchOperatorOr,
									Description: "At least one token must match",
								},
								"AndCross": &graphql.EnumValueConfig{
									Value:       SearchOperatorAndCross,
									Description: "All tokens must match, but they may be spread across the searched properties (rather than all within one property)",
								},
							},
						}),
					},
					"minimumOrTokensMatch": &graphql.InputObjectFieldConfig{
						Description: "The minimum number of tokens that should match (only for OR operator)",
						Type:        graphql.Int,
					},
				},
			},
		),
	}
}

// ExtractBM25
func ExtractBM25(source map[string]interface{}, explainScore bool) searchparams.KeywordRanking {
	var args searchparams.KeywordRanking

	p, ok := source["properties"]
	if ok {
		rawSlice := p.([]interface{})
		args.Properties = make([]string, len(rawSlice))
		for i, raw := range rawSlice {
			args.Properties[i] = raw.(string)
		}
	}

	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	args.AdditionalExplanations = explainScore
	args.Type = "bm25"

	operator, ok := source["searchOperator"]
	if ok {
		operator := operator.(map[string]interface{})
		args.SearchOperator = operator["operator"].(string)
		if operator["minimumOrTokensMatch"] != nil {
			args.MinimumOrTokensMatch = int(operator["minimumOrTokensMatch"].(int))
		}
	}

	return args
}
