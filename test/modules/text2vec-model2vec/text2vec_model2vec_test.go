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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecModel2Vec(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		className := "CompaniesModel2Vec"
		tests := []struct {
			name                    string
			insertNonVocabularWords bool
		}{
			{
				name: "default",
			},
			{
				name:                    "vectorize non-vocabular words",
				insertNonVocabularWords: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				descriptionVectorizer := map[string]any{
					"text2vec-model2vec": map[string]any{
						"properties": []any{"description"},
					},
				}
				emptyVectorizer := map[string]any{
					"text2vec-model2vec": map[string]any{
						"properties": []any{"empty"},
					},
				}
				t.Run("search", func(t *testing.T) {
					companies.TestSuite(t, rest, grpc, className, descriptionVectorizer)
				})
				t.Run("empty values", func(t *testing.T) {
					companies.TestSuiteWithEmptyValues(t, rest, grpc, className, descriptionVectorizer, emptyVectorizer)
				})
				if tt.insertNonVocabularWords {
					class := companies.BaseClass(className)
					class.VectorConfig = map[string]models.VectorConfig{
						"description": {
							Vectorizer: map[string]any{
								"text2vec-model2vec": map[string]any{
									"properties": []any{"description"},
								},
							},
							VectorIndexType: "flat",
						},
					}
					// create schema
					helper.CreateClass(t, class)
					defer helper.DeleteClass(t, class.Class)
					// insert objects
					companies.BatchInsertObjects(t, rest, className)
					t.Run("query with non-vocabular words", func(t *testing.T) {
						companies.PerformHybridSearchWithTextTestWithTargetVector(t, rest, className, "no-vocabular text: **77aaee sss fb", "")
					})
					t.Run("insert objects with non-vocabular words", func(t *testing.T) {
						obj := &models.Object{
							Class: className,
							ID:    "00000000-0000-0000-0000-00000000000a",
							Properties: map[string]any{
								"name":        "non vocabular words",
								"description": "words that are not in vocabulary: asdaddasd fjfjjgkk asda222ef (((sssss))) ko%$#$@@@@@@...",
							},
						}
						helper.CreateObject(t, obj)
						helper.AssertGetObjectEventually(t, obj.Class, obj.ID)

						dbObject, err := helper.GetObject(t, className, obj.ID, "vector")
						require.NoError(t, err)
						require.NotNil(t, dbObject)
						require.Len(t, dbObject.Vectors, 1)
						assert.True(t, len(dbObject.Vectors["description"].([]float32)) > 0)
					})
				}
			})
		}
	}
}
