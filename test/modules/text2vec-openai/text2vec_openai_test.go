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

func testText2VecOpenAI(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		// Data
		className := "BooksGenerativeTest"
		// data := companies.Companies
		// class := companies.BaseClass(className)
		tests := []struct {
			name       string
			model      string
			dimensions int
		}{
			{
				name: "default settings",
			},
			{
				name:       "text-embedding-3-large",
				model:      "text-embedding-3-large",
				dimensions: 256,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define module settings
				descriptionSettings := map[string]any{
					"properties":         []any{"description"},
					"vectorizeClassName": false,
				}
				emptySettings := map[string]any{
					"properties":         []any{"empty"},
					"vectorizeClassName": false,
				}
				if tt.model != "" {
					descriptionSettings["model"] = tt.model
					emptySettings["model"] = tt.model
				}
				if tt.dimensions > 0 {
					descriptionSettings["dimensions"] = tt.dimensions
					emptySettings["dimensions"] = tt.dimensions
				}
				descriptionVectorizer := map[string]any{"text2vec-openai": descriptionSettings}
				emptyVectorizer := map[string]any{"text2vec-openai": emptySettings}
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
