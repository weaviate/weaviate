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

package test_suits

import (
	acceptance_with_go_client "acceptance_tests_with_client"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

func testCompresseVectorTypesRestart(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		className := "NamedVectorsCompressedIndexTypeRestart"

		host := compose.GetWeaviate().URI()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		numberOfObjects := 100

		targetVectorDimensions := map[string]int{
			"uncompressed":    800,
			"hnsw_bq":         1280,
			"hnsw_pq":         256,
			"hnsw_sq":         796,
			"flat":            512,
			"flat_bq":         1024,
			"mv_uncompressed": 64,
			"mv_bq":           128,
			"hnsw_1bit_rq":    1356,
			"hnsw_8bit_rq":    1600,
		}

		vectorConfig := map[string]models.VectorConfig{
			"uncompressed": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			"hnsw_bq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: bq(false),
			},
			"hnsw_pq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			"hnsw_sq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			"flat": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "flat",
			},
			"flat_bq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "flat",
				VectorIndexConfig: bq(false),
			},
			"mv_uncompressed": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
				VectorIndexConfig: map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
					},
				},
			},
			"mv_bq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: bq(true),
			},
			"hnsw_1bit_rq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			"hnsw_8bit_rq": {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
		}

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "description", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: vectorConfig,
		}

		generateVector := func(targetVector string) models.Vector {
			dimensions := targetVectorDimensions[targetVector]
			if strings.HasPrefix(targetVector, "mv_") {
				return generateRandomMultiVector(dimensions, 5)
			}
			return generateRandomVector(dimensions)
		}

		generateVectors := func() models.Vectors {
			vectors := models.Vectors{}
			for targetVector := range targetVectorDimensions {
				vectors[targetVector] = generateVector(targetVector)
			}
			return vectors
		}

		insertObjects := func(t *testing.T, n int) {
			objs := []*models.Object{}
			for i := range n {
				obj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(uuid.NewString()),
					Properties: map[string]any{
						"name":        fmt.Sprintf("name %v", i),
						"description": fmt.Sprintf("some description %v", i),
					},
					Vectors: generateVectors(),
				}
				objs = append(objs, obj)
			}
			batchInsertObjects(t, client, objs)
		}

		queryAllTargetVectors := func(t *testing.T, expectedResults int) {
			for targetVector := range targetVectorDimensions {
				nearVector := client.GraphQL().NearVectorArgBuilder().
					WithVector(generateVector(targetVector)).
					WithTargetVectors(targetVector)
				get := client.GraphQL().Get().
					WithClassName(className).
					WithNearVector(nearVector).
					WithLimit(1000).
					WithFields(graphql.Field{
						Name: "_additional",
						Fields: []graphql.Field{
							{Name: "id"},
						},
					})
				require.EventuallyWithT(t, func(ct *assert.CollectT) {
					resp, err := get.Do(context.Background())
					require.NoError(t, err)
					require.NotNil(t, resp)
					if len(resp.Data) == 0 {
						return
					}

					ids := acceptance_with_go_client.GetIds(t, resp, className)
					assert.Greater(ct, len(ids), 0, fmt.Sprintf("targetVector: %s", targetVector))
				}, 5*time.Second, 1*time.Millisecond)
			}
		}

		t.Run("create schema", func(t *testing.T) {
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})

		t.Run("batch create objects", func(t *testing.T) {
			insertObjects(t, numberOfObjects)
			testAllObjectsIndexed(t, client, className)
		})

		t.Run("query all target vectors", func(t *testing.T) {
			queryAllTargetVectors(t, numberOfObjects)
		})

		t.Run("enable quantization", func(t *testing.T) {
			// enable SQ
			vectorConfig["hnsw_sq"] = models.VectorConfig{
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: sq(numberOfObjects - 1),
			}
			class.VectorConfig = vectorConfig
			err := client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
			// enable PQ
			vectorConfig["hnsw_pq"] = models.VectorConfig{
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pq(numberOfObjects - 1),
			}
			class.VectorConfig = vectorConfig
			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
			// enable RQ 1bit
			vectorConfig["hnsw_1bit_rq"] = models.VectorConfig{
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: rq(1),
			}
			class.VectorConfig = vectorConfig
			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
			// enable RQ 8bit
			vectorConfig["hnsw_8bit_rq"] = models.VectorConfig{
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: rq(8),
			}
			class.VectorConfig = vectorConfig
			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
		})

		t.Run("query all target vectors after quantization enabled", func(t *testing.T) {
			queryAllTargetVectors(t, numberOfObjects)
		})

		t.Run("check if the vectors_compressed folders are created each per named vector", func(t *testing.T) {
			nodeStatus, err := client.Cluster().NodesStatusGetter().WithOutput("verbose").Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, nodeStatus)
			require.NotEmpty(t, nodeStatus.Nodes)
			require.NotEmpty(t, nodeStatus.Nodes[0].Shards)
			var shardName string
			for _, shard := range nodeStatus.Nodes[0].Shards {
				if shard.Class == className {
					shardName = shard.Name
				}
			}
			require.NotEmpty(t, shardName)

			weaviateContainer := compose.GetWeaviate().Container()
			path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), shardName)
			code, reader, err := weaviateContainer.Exec(ctx, []string{"ls", "-1", path})
			require.NoError(t, err)
			require.Equal(t, 0, code)

			buf := new(strings.Builder)
			_, err = io.Copy(buf, reader)
			require.NoError(t, err)
			output := buf.String()

			var vectorsCompressedFolders []string
			for line := range strings.SplitSeq(output, "\n") {
				if strings.HasPrefix(line, "vectors_compressed") {
					vectorsCompressedFolders = append(vectorsCompressedFolders, line)
				}
			}
			assert.NotContains(t, vectorsCompressedFolders, "vectors_compressed")
			for targetVector := range targetVectorDimensions {
				if strings.HasSuffix(targetVector, "q") {
					assert.Contains(t, vectorsCompressedFolders, fmt.Sprintf("vectors_compressed_%s", targetVector))
				}
			}
		})

		t.Run("restart", func(t *testing.T) {
			err := compose.Stop(ctx, compose.GetWeaviate().Name(), nil)
			require.NoError(t, err)

			err = compose.Start(ctx, compose.GetWeaviate().Name())
			require.NoError(t, err)

			host := compose.GetWeaviate().URI()
			client, err = wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
			require.NoError(t, err)
			queryAllTargetVectors(t, numberOfObjects)
		})
	}
}

func testLegacyAndNamedVectorRestart(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		numberOfObjects := 100
		tests := []struct {
			name              string
			vectorIndexConfig any
			vectorIndexType   string
		}{
			{
				name:              "flat BQ",
				vectorIndexConfig: bq(false),
				vectorIndexType:   "flat",
			},
			{
				name:              "hnsw RQ",
				vectorIndexConfig: rq(8),
				vectorIndexType:   "hnsw",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				className := "LegacyNamedVectorsCompressedIndexTypeRestart"

				host := compose.GetWeaviate().URI()
				client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
				require.Nil(t, err)

				client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

				targetVectorDimensions := map[string]int{
					"default": 32,
					"hnsw_sq": 1024,
				}

				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name: "name", DataType: []string{schema.DataTypeText.String()},
						},
						{
							Name: "description", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorIndexConfig: tt.vectorIndexConfig,
					VectorIndexType:   tt.vectorIndexType,
					Vectorizer:        "none",
				}

				generateVector := func(targetVector string) models.Vector {
					dimensions := targetVectorDimensions[targetVector]
					if strings.HasPrefix(targetVector, "mv_") {
						return generateRandomMultiVector(dimensions, 5)
					}
					return generateRandomVector(dimensions)
				}

				generateVectors := func() models.Vectors {
					vectors := models.Vectors{}
					for targetVector := range targetVectorDimensions {
						if targetVector != "default" {
							vectors[targetVector] = generateVector(targetVector)
						}
					}
					return vectors
				}

				insertObjects := func(t *testing.T, n int, onlyLegacy bool) {
					objs := []*models.Object{}
					for i := range n {
						obj := &models.Object{
							Class: className,
							ID:    strfmt.UUID(uuid.NewString()),
							Properties: map[string]any{
								"name":        fmt.Sprintf("name %v", i),
								"description": fmt.Sprintf("some description %v", i),
							},
						}
						obj.Vector = generateRandomVector(targetVectorDimensions["default"])
						if !onlyLegacy {
							obj.Vectors = generateVectors()
						}
						objs = append(objs, obj)
					}
					batchInsertObjects(t, client, objs)
				}

				queryAllTargetVectors := func(t *testing.T, onlyLegacy bool) {
					for targetVector := range targetVectorDimensions {
						if onlyLegacy && targetVector != "default" {
							continue
						}
						nearVector := client.GraphQL().NearVectorArgBuilder().
							WithVector(generateVector(targetVector)).
							WithTargetVectors(targetVector)
						get := client.GraphQL().Get().
							WithClassName(className).
							WithNearVector(nearVector).
							WithLimit(1000).
							WithFields(graphql.Field{
								Name: "_additional",
								Fields: []graphql.Field{
									{Name: "id"},
								},
							})
						require.EventuallyWithT(t, func(ct *assert.CollectT) {
							resp, err := get.Do(context.Background())
							require.NoError(t, err)
							require.NotNil(t, resp)
							if len(resp.Data) == 0 {
								return
							}

							ids := acceptance_with_go_client.GetIds(t, resp, className)
							assert.Greater(ct, len(ids), 0, fmt.Sprintf("targetVector: %s", targetVector))
						}, 5*time.Second, 1*time.Millisecond)
					}
				}

				t.Run("create schema", func(t *testing.T) {
					err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
					require.NoError(t, err)
				})

				t.Run("batch create only objects with legacy vector", func(t *testing.T) {
					insertObjects(t, numberOfObjects, true)
					testAllObjectsIndexed(t, client, className)
				})

				t.Run("query all target vectors", func(t *testing.T) {
					queryAllTargetVectors(t, true)
				})

				t.Run("add a new named vector with quantization enabled", func(t *testing.T) {
					// enable SQ with a limit above the number of inserted objects so that
					// vectors compressed folder for that named vector won't get created
					vectorConfig := map[string]models.VectorConfig{
						"hnsw_sq": {
							Vectorizer: map[string]any{
								"none": map[string]any{},
							},
							VectorIndexType:   "hnsw",
							VectorIndexConfig: sq(2*numberOfObjects + 1),
						},
					}
					class.VectorConfig = vectorConfig
					err := client.Schema().ClassUpdater().WithClass(class).Do(ctx)
					require.NoError(t, err)
					time.Sleep(1 * time.Second)
					insertObjects(t, numberOfObjects, false)
					testAllObjectsIndexed(t, client, className)
				})

				t.Run("query all target vectors after quantization enabled", func(t *testing.T) {
					queryAllTargetVectors(t, false)
				})

				t.Run("check that vectors_compressed folder for newly added named vector was not created", func(t *testing.T) {
					err := compose.Stop(ctx, compose.GetWeaviate().Name(), nil)
					require.NoError(t, err)

					err = compose.Start(ctx, compose.GetWeaviate().Name())
					require.NoError(t, err)

					host := compose.GetWeaviate().URI()
					client, err = wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
					require.NoError(t, err)

					nodeStatus, err := client.Cluster().NodesStatusGetter().WithOutput("verbose").Do(ctx)
					require.NoError(t, err)
					require.NotNil(t, nodeStatus)
					require.NotEmpty(t, nodeStatus.Nodes)
					require.NotEmpty(t, nodeStatus.Nodes[0].Shards)
					var shardName string
					for _, shard := range nodeStatus.Nodes[0].Shards {
						if shard.Class == className {
							shardName = shard.Name
						}
					}
					require.NotEmpty(t, shardName)

					weaviateContainer := compose.GetWeaviate().Container()
					path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), shardName)
					code, reader, err := weaviateContainer.Exec(ctx, []string{"ls", "-1", path})
					require.NoError(t, err)
					require.Equal(t, 0, code)

					buf := new(strings.Builder)
					_, err = io.Copy(buf, reader)
					require.NoError(t, err)
					output := buf.String()

					var vectorsCompressedFolders []string
					for line := range strings.SplitSeq(output, "\n") {
						if strings.HasPrefix(line, "vectors_compressed") {
							vectorsCompressedFolders = append(vectorsCompressedFolders, line)
						}
					}
					// check that vectors compressed folder wasn't created unnecessary for target vector
					for targetVector := range targetVectorDimensions {
						assert.NotContains(t, vectorsCompressedFolders, fmt.Sprintf("vectors_compressed_%s", targetVector))
					}
					// check that search still works
					queryAllTargetVectors(t, false)
				})
			})
		}

	}
}
