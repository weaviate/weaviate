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
	"context"
	"fmt"
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

		numberOfObjects := 10

		targetVectorDimensions := map[string]int{
			"uncompressed":    800,
			"hnsw_bq":         1280,
			"hnsw_pq":         256,
			"hnsw_sq":         796,
			"flat":            512,
			"flat_bq":         1024,
			"mv_uncompressed": 64,
			"mv_bq":           128,
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
					assert.Equal(ct, expectedResults, len(ids), fmt.Sprintf("targetVector: %s", targetVector))
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
			vectorConfig["hnsw_sq"] = models.VectorConfig{
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: sq(9),
			}
			class.VectorConfig = vectorConfig
			err := client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})

		t.Run("query all target vectors after quantization enabled", func(t *testing.T) {
			queryAllTargetVectors(t, numberOfObjects)
		})

		t.Run("wait 5 seconds for all of the indexes to settle", func(t *testing.T) {
			time.Sleep(5 * time.Second)
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
