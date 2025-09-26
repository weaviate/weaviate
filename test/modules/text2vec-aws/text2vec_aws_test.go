//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tests

import (
	"testing"

	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecAWS(rest, grpc, region string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		className := "VectorizerTest"
		tests := []struct {
			name  string
			model string
		}{
			{
				name:  "amazon.titan-embed-text-v1",
				model: "amazon.titan-embed-text-v1",
			},
			{
				name:  "amazon.titan-embed-text-v2:0",
				model: "amazon.titan-embed-text-v2:0",
			},
			{
				name:  "cohere.embed-english-v3",
				model: "cohere.embed-english-v3",
			},
			{
				name:  "cohere.embed-multilingual-v3",
				model: "cohere.embed-multilingual-v3",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				descriptionVectorizer := map[string]any{
					"text2vec-aws": map[string]any{
						"properties":         []any{"description"},
						"vectorizeClassName": false,
						"service":            "bedrock",
						"region":             region,
						"model":              tt.model,
					},
				}
				emptyVectorizer := map[string]any{
					"text2vec-aws": map[string]any{
						"properties":         []any{"empty"},
						"vectorizeClassName": false,
						"service":            "bedrock",
						"region":             region,
						"model":              tt.model,
					},
				}
				t.Run("search", func(t *testing.T) {
					companies.TestSuite(t, rest, grpc, className, descriptionVectorizer)
				})
				t.Run("empty values", func(t *testing.T) {
					companies.TestSuiteWithEmptyValues(t, rest, grpc, className, descriptionVectorizer, emptyVectorizer)
				})
			})
		}
	}
}
