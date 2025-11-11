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

func testText2VecGoogle(rest, grpc, gcpProject, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		// Data
		dimensions1024 := 1024
		className := "VectorizerTest"
		tests := []struct {
			name       string
			model      string
			dimensions *int
			taskType   string
		}{
			{
				name:  "textembedding-gecko@latest",
				model: "textembedding-gecko@latest",
			},
			{
				name:  "textembedding-gecko-multilingual@latest",
				model: "textembedding-gecko-multilingual@latest",
			},
			{
				name:  "text-embedding-005",
				model: "text-embedding-005",
			},
			{
				name:  "text-multilingual-embedding-002",
				model: "text-multilingual-embedding-002",
			},
			{
				name:  "gemini-embedding-001",
				model: "gemini-embedding-001",
			},
			{
				name:       "gemini-embedding-001 with 1024 dimensions",
				model:      "gemini-embedding-001",
				dimensions: &dimensions1024,
			},
			{
				name:     "text-embedding-005 with FACT_VERIFICATION taskType",
				model:    "text-embedding-005",
				taskType: "FACT_VERIFICATION",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define class
				vectorizerSettings := map[string]any{
					"properties":         []any{"description"},
					"vectorizeClassName": false,
					"projectId":          gcpProject,
					"modelId":            tt.model,
				}
				emptyVectorizerSettings := map[string]any{
					"properties":         []any{"empty"},
					"vectorizeClassName": false,
					"projectId":          gcpProject,
					"modelId":            tt.model,
				}
				if tt.dimensions != nil {
					vectorizerSettings["dimensions"] = *tt.dimensions
					emptyVectorizerSettings["dimensions"] = *tt.dimensions
				}
				if tt.taskType != "" {
					vectorizerSettings["taskType"] = tt.taskType
					emptyVectorizerSettings["taskType"] = tt.taskType
				}
				descriptionVectorizer := map[string]any{vectorizerName: vectorizerSettings}
				emptyVectorizer := map[string]any{vectorizerName: emptyVectorizerSettings}
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
