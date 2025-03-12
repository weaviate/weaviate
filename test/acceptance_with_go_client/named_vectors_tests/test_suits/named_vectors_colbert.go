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
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
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
			normalVector2dName := fixtures.NormalVector2dName
			multiVector1dName := fixtures.MultiVector1dName
			multiVector2dName := fixtures.MultiVector2dName
			multiVector3dName := fixtures.MultiVector3dName
			multiVector1dFromMultiVector2d := func(mv [][]float32) [][]float32 {
				multiVector1d := make([][]float32, len(mv))
				for i, v := range mv {
					multiVector1d[i] = []float32{v[0]}
				}
				return multiVector1d
			}
			multiVector3dFromMultiVector2d := func(mv [][]float32) [][]float32 {
				multiVector3d := make([][]float32, len(mv))
				for i, v := range mv {
					multiVector3d[i] = []float32{v[0], v[1], v[1]}
				}
				return multiVector3d
			}

			_additional_byoc := graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
					{Name: fmt.Sprintf("vectors{%s}", byoc)},
				},
			}

			performNearVector := func(t *testing.T, client *wvt.Client, className string) {
				nearVector := client.GraphQL().NearVectorArgBuilder().
					WithVector([][]float32{{-0.000001, -0.000001}, {-0.000001, -0.000001}, {-0.000001, -0.000001}}).
					WithTargetVectors(byoc)
				resp, err := client.GraphQL().Get().
					WithClassName(className).
					WithNearVector(nearVector).
					WithFields(_additional_byoc).
					Do(ctx)
				require.NoError(t, err)
				ids := acceptance_with_go_client.GetIds(t, resp, className)
				require.NotEmpty(t, ids)
				assert.Len(t, ids, len(objects))
			}

			performNearObject := func(t *testing.T, client *wvt.Client, className string) {
				nearObject := client.GraphQL().NearObjectArgBuilder().
					WithID(objects[0].ID).
					WithTargetVectors(byoc)
				resp, err := client.GraphQL().Get().
					WithClassName(className).
					WithNearObject(nearObject).
					WithFields(_additional_byoc).
					Do(ctx)
				require.NoError(t, err)
				ids := acceptance_with_go_client.GetIds(t, resp, className)
				require.NotEmpty(t, ids)
				assert.Len(t, ids, len(objects))
			}

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
							byoc:               o.Vector,
							normalVector2dName: o.Vector[0],
							multiVector1dName:  multiVector1dFromMultiVector2d(o.Vector),
							multiVector2dName:  o.Vector,
							multiVector3dName:  multiVector3dFromMultiVector2d(o.Vector),
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

			t.Run("checks objects indexed", func(t *testing.T) {
				testAllObjectsIndexed(t, client, className)
			})

			t.Run("vector search after insert", func(t *testing.T) {
				performNearVector(t, client, className)
				performNearObject(t, client, className)

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
					require.Len(t, objs[0].Vectors, 5)
					assert.IsType(t, [][]float32{}, objs[0].Vectors[byoc])
					assert.Equal(t, o.Vector, objs[0].Vectors[byoc])
				}
			})

			t.Run("GraphQL get object with vector", func(t *testing.T) {
				for _, o := range objects {
					resp, err := client.GraphQL().Get().
						WithClassName(className).
						WithWhere(filters.Where().WithPath([]string{"id"}).WithOperator(filters.Equal).WithValueText(o.ID)).
						WithFields(_additional_byoc).
						Do(ctx)
					require.NoError(t, err)
					vectors := acceptance_with_go_client.GetVectors(t, resp, className, false, byoc)
					require.Len(t, vectors, 1)
					require.IsType(t, [][]float32{}, vectors[byoc])
					assert.Equal(t, o.Vector, vectors[byoc])
				}
			})

			t.Run("update vector", func(t *testing.T) {
				tests := []struct {
					name string
					obj  struct {
						ID     string
						Name   string
						Vector [][]float32
					}
					withMerge bool
					vector    [][]float32
				}{
					{
						name:   "update",
						obj:    objects[0],
						vector: [][]float32{{-0.11111111, -0.12222222}, {-0.13, -0.14}, {-0.15, -0.16}},
					},
					{
						name:      "merge",
						obj:       objects[1],
						withMerge: true,
						vector:    [][]float32{{-0.000001, -0.000001}, {-0.000001, -0.000001}, {-0.000001, -0.000001}},
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						firstObj := tt.obj
						updateVectors := models.Vectors{
							byoc:               tt.vector,
							normalVector2dName: tt.vector[0],
							multiVector1dName:  multiVector1dFromMultiVector2d(tt.vector),
							multiVector2dName:  tt.vector,
							multiVector3dName:  multiVector3dFromMultiVector2d(tt.vector),
						}
						objs, err := client.Data().ObjectsGetter().
							WithClassName(className).WithID(firstObj.ID).WithVector().Do(ctx)
						require.NoError(t, err)
						require.NotEmpty(t, objs)
						require.Len(t, objs[0].Vectors, 5)
						assert.Equal(t, firstObj.Vector, objs[0].Vectors[byoc])
						updater := client.Data().Updater().
							WithClassName(className).WithID(firstObj.ID).WithVectors(updateVectors)
						if tt.withMerge {
							err = updater.WithMerge().Do(ctx)
						} else {
							err = updater.Do(ctx)
						}
						require.NoError(t, err)
						objs, err = client.Data().ObjectsGetter().
							WithClassName(className).WithID(firstObj.ID).WithVector().Do(ctx)
						require.NoError(t, err)
						require.NotEmpty(t, objs)
						require.Len(t, objs[0].Vectors, 5)
						assert.Equal(t, updateVectors[byoc], objs[0].Vectors[byoc])
						resp, err := client.GraphQL().Get().
							WithClassName(className).
							WithWhere(filters.Where().WithPath([]string{"id"}).WithOperator(filters.Equal).WithValueText(firstObj.ID)).
							WithFields(_additional_byoc).
							Do(ctx)
						require.NoError(t, err)
						vectors := acceptance_with_go_client.GetVectors(t, resp, className, false, byoc)
						require.Len(t, vectors, 1)
						require.IsType(t, [][]float32{}, vectors[byoc])
						assert.Equal(t, updateVectors[byoc], vectors[byoc])
					})
				}
			})

			t.Run("checks objects indexed", func(t *testing.T) {
				testAllObjectsIndexed(t, client, className)
			})

			t.Run("vector search after partial update", func(t *testing.T) {
				performNearVector(t, client, className)
				performNearObject(t, client, className)
			})

			t.Run("update all objects", func(t *testing.T) {
				for _, obj := range objects {
					err = client.Data().Updater().
						WithClassName(className).WithID(obj.ID).WithVectors(models.Vectors{
						byoc:               obj.Vector,
						normalVector2dName: obj.Vector[0],
						multiVector1dName:  multiVector1dFromMultiVector2d(obj.Vector),
						multiVector2dName:  obj.Vector,
						multiVector3dName:  multiVector3dFromMultiVector2d(obj.Vector),
					}).Do(ctx)
					require.NoError(t, err)
				}
			})

			t.Run("checks objects indexed", func(t *testing.T) {
				testAllObjectsIndexed(t, client, className)
			})

			t.Run("vector search after update of all objects", func(t *testing.T) {
				performNearVector(t, client, className)
				performNearObject(t, client, className)
			})
			t.Run("WithVector[s]PerTarget searches", func(t *testing.T) {
				withVectorPerTargetTests := []struct {
					name       string
					nearVector *graphql.NearVectorArgumentBuilder
				}{
					{
						name: "NormalVector_dim2",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorPerTarget(map[string]models.Vector{
								normalVector2dName: []float32{0.1, 0.1},
							}).
							WithTargetVectors(normalVector2dName),
					},
					{
						name: "NormalVectors_dim3x2",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorsPerTarget(map[string][]models.Vector{
								normalVector2dName: {[]float32{0.1, 0.1}, []float32{0.1, 0.1}, []float32{0.1, 0.1}},
							}).
							WithTargetVectors(normalVector2dName),
					},
					{
						name: "MultiVector_dim3x2",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorPerTarget(map[string]models.Vector{
								multiVector2dName: [][]float32{{0.1, 0.1}, {0.1, 0.1}, {0.1, 0.1}},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVectors_dim1x3x2",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorsPerTarget(map[string][]models.Vector{
								multiVector2dName: {
									[][]float32{{0.1, 0.1}, {0.1, 0.1}, {0.1, 0.1}},
								},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVectors_dim2x3x2",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorsPerTarget(map[string][]models.Vector{
								multiVector2dName: {
									[][]float32{{0.1, 0.1}, {0.1, 0.1}, {0.1, 0.1}},
									[][]float32{{0.1, 0.1}, {0.1, 0.1}, {0.1, 0.1}},
								},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVector_1d2d3d",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorPerTarget(map[string]models.Vector{
								multiVector1dName: [][]float32{{0.1}, {0.1}},
								multiVector2dName: [][]float32{{0.1, 0.1}, {0.1, 0.1}},
								multiVector3dName: [][]float32{{0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVectors_1d2d3d",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorsPerTarget(map[string][]models.Vector{
								multiVector1dName: {
									[][]float32{{0.1}, {0.1}},
									[][]float32{{0.1}, {0.1}},
									[][]float32{{0.1}, {0.1}},
								},
								multiVector2dName: {
									[][]float32{{0.1, 0.1}, {0.1, 0.1}},
									[][]float32{{0.1, 0.1}, {0.1, 0.1}},
								},
								multiVector3dName: {
									[][]float32{{0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}},
								},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVector_all",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorPerTarget(map[string]models.Vector{
								normalVector2dName: []float32{0.1, 0.1},
								multiVector1dName:  [][]float32{{0.1}, {0.1}},
								multiVector2dName:  [][]float32{{0.1, 0.1}, {0.1, 0.1}},
								multiVector3dName:  [][]float32{{0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}},
							}).
							WithTargetVectors(multiVector2dName),
					},
					{
						name: "MultiVectors_all",
						nearVector: client.GraphQL().NearVectorArgBuilder().
							WithVectorsPerTarget(map[string][]models.Vector{
								normalVector2dName: {
									[]float32{0.1, 0.1},
									[]float32{0.1, 0.1},
								},
								multiVector1dName: {
									[][]float32{{0.1}, {0.1}},
									[][]float32{{0.1}, {0.1}},
								},
								multiVector2dName: {
									[][]float32{{0.1, 0.1}, {0.1, 0.1}},
									[][]float32{{0.1, 0.1}, {0.1, 0.1}},
								},
								multiVector3dName: {
									[][]float32{{0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}},
									[][]float32{{0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}},
								},
							}).
							WithTargetVectors(multiVector2dName),
					},
				}
				for _, tt := range withVectorPerTargetTests {
					t.Run(tt.name, func(t *testing.T) {
						resp, err := client.GraphQL().Get().
							WithClassName(className).
							WithNearVector(tt.nearVector).
							WithFields(_additional_byoc).
							Do(ctx)
						require.NoError(t, err)
						ids := acceptance_with_go_client.GetIds(t, resp, className)
						require.NotEmpty(t, ids)
						assert.Len(t, ids, len(objects))
					})
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
			t.Run("named vector is colbert vectorizer with empty vector index", func(t *testing.T) {
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
							VectorIndexType: "hnsw",
						},
					},
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				// Weaviate overrides the multivector setting if it finds that someone has enabled
				// multi vector vectorizer
				require.NoError(t, err)
				cls, err := client.Schema().ClassGetter().WithClassName(class.Class).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, cls.VectorConfig["colbert"])
				vc := cls.VectorConfig["colbert"].VectorIndexConfig
				require.NotNil(t, vc)
				vsAsMap, ok := vc.(map[string]interface{})
				require.True(t, ok)
				mv, ok := vsAsMap["multivector"].(map[string]interface{})
				require.True(t, ok)
				assert.True(t, mv["enabled"].(bool))
			})
			t.Run("multi vector index with flat vector index type", func(t *testing.T) {
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
							VectorIndexType: "flat",
						},
					},
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				// Weaviate overrides the multivector setting if it finds that someone has enabled
				// multi vector vectorizer
				require.Error(t, err)
				assert.ErrorContains(t, err, `parse vector index config: multi vector index is not supported for vector index type: \"flat\", only supported type is hnsw`)
			})
			t.Run("multi vector index with dynamic vector index type", func(t *testing.T) {
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
							VectorIndexType: "dynamic",
						},
					},
				}
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				// Weaviate overrides the multivector setting if it finds that someone has enabled
				// multi vector vectorizer
				require.Error(t, err)
				assert.ErrorContains(t, err, `parse vector index config: multi vector index is not supported for vector index type: \"dynamic\", only supported type is hnsw`)
			})
		})

		t.Run("validate multi vector dimensions", func(t *testing.T) {
			className := fixtures.BringYourOwnColBERTClassName
			objects := fixtures.BringYourOwnColBERTObjects
			byoc := fixtures.BringYourOwnColBERTNamedVectorName

			t.Run("batch create", func(t *testing.T) {
				cleanup()

				t.Run("create schema", func(t *testing.T) {
					class := fixtures.BringYourOwnColBERTClass(className)
					err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
					require.NoError(t, err)
				})

				t.Run("batch create objects", func(t *testing.T) {
					obj1 := &models.Object{
						Class: className,
						ID:    strfmt.UUID(objects[0].ID),
						Properties: map[string]interface{}{
							"name": objects[0].Name,
						},
						Vectors: models.Vectors{
							byoc: objects[0].Vector,
						},
					}
					objWrongDimensions := &models.Object{
						Class: className,
						ID:    strfmt.UUID(objects[1].ID),
						Properties: map[string]interface{}{
							"name": objects[1].Name,
						},
						Vectors: models.Vectors{
							byoc: [][]float32{{0.12, 0.12}, {0.11}},
						},
					}
					resp, err := client.Batch().ObjectsBatcher().
						WithObjects(obj1, objWrongDimensions).
						Do(ctx)
					require.NoError(t, err)
					require.NotNil(t, resp)
					success, failed := false, false
					for i := range resp {
						if *resp[i].Result.Status == "SUCCESS" {
							success = true
						}
						if *resp[i].Result.Status == "FAILED" {
							failed = true
						}
					}
					assert.True(t, success)
					assert.True(t, failed)
				})
			})

			t.Run("insert", func(t *testing.T) {
				cleanup()

				t.Run("create schema", func(t *testing.T) {
					class := fixtures.BringYourOwnColBERTClass(className)
					err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
					require.NoError(t, err)
				})

				t.Run("batch create objects", func(t *testing.T) {
					obj1 := &models.Object{
						Class: className,
						ID:    strfmt.UUID(objects[0].ID),
						Properties: map[string]interface{}{
							"name": objects[0].Name,
						},
						Vectors: models.Vectors{
							byoc: objects[0].Vector,
						},
					}
					resp, err := client.Batch().ObjectsBatcher().
						WithObjects(obj1).
						Do(ctx)
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Equal(t, "SUCCESS", *resp[0].Result.Status)
				})

				t.Run("insert additional object", func(t *testing.T) {
					objWrongDimensions := &models.Object{
						Class: className,
						ID:    strfmt.UUID(objects[1].ID),
						Properties: map[string]interface{}{
							"name": objects[1].Name,
						},
						Vectors: models.Vectors{
							byoc: [][]float32{{0.12, 0.12}, {0.11}},
						},
					}
					_, err := client.Data().Creator().
						WithClassName(objWrongDimensions.Class).
						WithID(objWrongDimensions.ID.String()).
						WithProperties(objWrongDimensions.Properties).
						WithVectors(objWrongDimensions.Vectors).
						Do(ctx)
					require.Error(t, err)
				})
			})
		})
	}
}
