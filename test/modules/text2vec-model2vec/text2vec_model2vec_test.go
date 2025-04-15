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

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecModel2Vec(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		className := "BooksGenerativeTest"
		data := companies.Companies
		class := companies.BaseClass(className)
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
				// Define class
				class.VectorConfig = map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]interface{}{
							"text2vec-model2vec": map[string]interface{}{
								"properties":         []interface{}{"description"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					companies.InsertObjects(t, host, class.Class)
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, company := range data {
						t.Run(company.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
						})
					}
				})
				// vector search
				t.Run("perform vector search", func(t *testing.T) {
					companies.PerformVectorSearchTest(t, host, class.Class)
				})
				// hybird search
				t.Run("perform hybrid search", func(t *testing.T) {
					companies.PerformHybridSearchTest(t, host, class.Class)
				})
				if tt.insertNonVocabularWords {
					t.Run("query with non-vocabular words", func(t *testing.T) {
						companies.PerformHybridSearchWithTextTest(t, host, class.Class, "no-vocabular text: **77aaee sss fb")
					})
					t.Run("insert objects with non-vocabular words", func(t *testing.T) {
						obj := &models.Object{
							Class: className,
							ID:    "00000000-0000-0000-0000-00000000000a",
							Properties: map[string]interface{}{
								"name":        "non vocabular words",
								"description": "words that are not in vocabulary: asdaddasd fjfjjgkk asda222ef (((sssss))) ko%$#$@@@@@@...",
							},
						}
						helper.CreateObject(t, obj)
						helper.AssertGetObjectEventually(t, obj.Class, obj.ID)

						dbObject, err := helper.GetObject(t, class.Class, obj.ID, "vector")
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
