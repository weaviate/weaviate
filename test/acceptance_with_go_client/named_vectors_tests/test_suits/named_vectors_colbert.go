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

package test_suits

import (
	acceptance_with_go_client "acceptance_tests_with_client"
	"acceptance_tests_with_client/fixtures"
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testColBERT(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("bring your own multivector", func(t *testing.T) {
			cleanup()

			className := fixtures.BringYourOwnColBERTClassName
			objects := fixtures.BringYourOwnColBERTObjects
			byoc := fixtures.BringYourOwnColBERTNamedVectorName

			t.Run("create schema", func(t *testing.T) {
				class := fixtures.BringYourOwnColBERTClass(className)
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)
			})

			t.Run("batch create objects", func(t *testing.T) {
				objs := []*models.Object{}
				for _, o := range objects {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(o.ID),
						Properties: map[string]interface{}{
							"name": o.Name,
						},
						Vectors: models.Vectors{
							byoc: o.Vector,
						},
					}
					objs = append(objs, obj)
				}

				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(objs...).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				for _, r := range resp {
					require.Equal(t, "SUCCESS", *r.Result.Status)
				}
			})

			t.Run("check existence", func(t *testing.T) {
				for _, obj := range objects {
					exists, err := client.Data().Checker().
						WithID(obj.ID).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})

			t.Run("get objects with vector", func(t *testing.T) {
				for _, o := range objects {
					objs, err := client.Data().ObjectsGetter().WithID(o.ID).WithClassName(className).WithVector().Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.Len(t, objs[0].Vectors, 1)
					assert.IsType(t, [][]float32{}, objs[0].Vectors[byoc])
					assert.Equal(t, o.Vector, objs[0].Vectors[byoc])
				}
			})

			t.Run("GraphQL get object with vector", func(t *testing.T) {
				_additional := graphql.Field{
					Name: "_additional",
					Fields: []graphql.Field{
						{Name: "id"},
						{Name: fmt.Sprintf("vectors{%s}", byoc)},
					},
				}
				for _, o := range objects {
					resp, err := client.GraphQL().Get().
						WithClassName(className).
						WithWhere(filters.Where().WithPath([]string{"id"}).WithOperator(filters.Equal).WithValueText(o.ID)).
						WithFields(_additional).
						Do(ctx)
					require.NoError(t, err)
					vectors := acceptance_with_go_client.GetVectors(t, resp, className, false, byoc)
					require.Len(t, vectors, 1)
					require.IsType(t, [][]float32{}, vectors[byoc])
					assert.Equal(t, o.Vector, vectors[byoc])
				}
			})
		})

		t.Run("multi vector validation", func(t *testing.T) {
			t.Run("multi vector vectorizer", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "MultiVectorLegacyVectorizer",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorIndexType: "hnsw",
					Vectorizer:      "text2colbert-jinaai",
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `multi vector vectorizer: \"text2colbert-jinaai\" is only allowed to be defined using named vector configuration`)
			})
			t.Run("multi vector vectorizer with module config", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "MultiVectorLegacyVectorizerWithModuleConfig",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					ModuleConfig: map[string]interface{}{
						"text2colbert-jinaai": map[string]interface{}{
							"skip": true,
						},
					},
					VectorIndexType: "hnsw",
					Vectorizer:      "text2colbert-jinaai",
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `multi vector vectorizer: \"text2colbert-jinaai\" is only allowed to be defined using named vector configuration`)
			})
			t.Run("colbert vectorizer with multi vector index", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "LegacyColbertVectorizerWithMultiVectorIndex",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorIndexType: "hnsw",
					VectorIndexConfig: map[string]interface{}{
						"multivector": map[string]interface{}{
							"enabled": true,
						},
					},
					Vectorizer: "text2colbert-jinaai",
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `class.VectorIndexConfig multi vector type index type is only configurable using named vectors`)
			})
			t.Run("regular vectorizer with multi vector index", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "LegacyOpenAIVectorizerWithMultiVectorIndex",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorIndexType: "hnsw",
					VectorIndexConfig: map[string]interface{}{
						"multivector": map[string]interface{}{
							"enabled": true,
						},
					},
					Vectorizer: "text2vec-openai",
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `class.VectorIndexConfig multi vector type index type is only configurable using named vectors`)
			})
			t.Run("legacy none vectorizer with multi vector index", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "LegacyNoneVectorizerWithMultiVectorIndex",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorIndexType: "hnsw",
					VectorIndexConfig: map[string]interface{}{
						"multivector": map[string]interface{}{
							"enabled": true,
						},
					},
					Vectorizer: "none",
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `class.VectorIndexConfig multi vector type index type is only configurable using named vectors`)
			})
			t.Run("named vector regular vectorizer with multi vector index", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "NamedVectorVectorizerWithMultiVectorIndex",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						c11y: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexConfig: map[string]interface{}{
								"multivector": map[string]interface{}{
									"enabled": true,
								},
							},
							VectorIndexType: "hnsw",
						},
					},
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, `parse vector config for c11y: multi vector index configured but vectorizer: \"text2vec-contextionary\" doesn't support multi vectors`)
			})
			t.Run("named vector is colbert vectorizer with regular vector index", func(t *testing.T) {
				cleanup()
				class := &models.Class{
					Class: "NamedVectorVectorizerWithMultiVectorIndex",
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						"colbert": {
							Vectorizer: map[string]interface{}{
								"text2colbert-jinaai": map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexConfig: map[string]interface{}{
								"multivector": map[string]interface{}{
									"enabled": false,
								},
							},
							VectorIndexType: "hnsw",
						},
					},
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				// Weaviate overrides the multivector setting if it finds that someone has enabled
				// multi vector vectorizer
				require.NoError(t, err)
			})
		})
	}
}
