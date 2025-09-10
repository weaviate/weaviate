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

func testText2MultivecJinaAI(rest, grpc, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		// Data
		className := "BooksGenerativeTest"
		tests := []struct {
			name  string
			model string
		}{
			{
				name:  "jina-colbert-v2",
				model: "jina-colbert-v2",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				descriptionVectorizer := map[string]any{
					vectorizerName: map[string]any{
						"properties":         []any{"description"},
						"vectorizeClassName": false,
						"model":              tt.model,
						"dimensions":         64,
					},
				}
				emptyVectorizer := map[string]any{
					vectorizerName: map[string]any{
						"properties":         []any{"empty"},
						"vectorizeClassName": false,
						"model":              tt.model,
						"dimensions":         64,
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
