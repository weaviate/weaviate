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
)

func testCompressedLegacyVectorIndex(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		className := "LegacyVectorIndex"

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		vectorDimensions := 1024
		vectorIndexType := "hnsw"
		pqVectorIndexConfig := map[string]any{
			"pq": map[string]any{
				"enabled":       true,
				"trainingLimit": float64(100),
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
			Vectorizer:      "none",
			VectorIndexType: vectorIndexType,
		}

		insertObjects := func(t *testing.T, n int) {
			objs := []*models.Object{}
			for i := 0; i < n; i++ {
				obj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(uuid.NewString()),
					Properties: map[string]any{
						"name":        fmt.Sprintf("name %v", i),
						"description": fmt.Sprintf("some description %v", i),
					},
					Vector: generateRandomVector(vectorDimensions),
				}
				objs = append(objs, obj)
			}

			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(objs...).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
		}

		performNearVector := func(t *testing.T) {
			nearVector := client.GraphQL().NearVectorArgBuilder().
				WithVector(generateRandomVector(vectorDimensions))
			get := client.GraphQL().Get().
				WithClassName(className).
				WithNearVector(nearVector).
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
				assert.Greater(ct, len(ids), 0)
			}, 5*time.Second, 1*time.Millisecond)
		}

		t.Run("create schema", func(t *testing.T) {
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})

		t.Run("batch create 100 objects", func(t *testing.T) {
			insertObjects(t, 100)
		})

		t.Run("query without quantization", func(t *testing.T) {
			performNearVector(t)
		})

		t.Run("enable quantization", func(t *testing.T) {
			class.VectorIndexConfig = pqVectorIndexConfig
			err := client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})

		t.Run("batch create 1000 objects", func(t *testing.T) {
			insertObjects(t, 1000)
		})

		t.Run("query with quantization turned on", func(t *testing.T) {
			performNearVector(t)
		})
	}
}
