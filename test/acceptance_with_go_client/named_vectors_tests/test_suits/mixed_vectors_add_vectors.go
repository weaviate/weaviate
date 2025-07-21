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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testMixedVectorsAddNewVectors(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: endpoint})
		require.NoError(t, err)

		fetchObject := func(t *testing.T, id string) *models.Object {
			objWrapper, err := client.Data().ObjectsGetter().
				WithClassName(className).
				WithID(id).
				WithVector().
				Do(ctx)
			require.NoError(t, err)
			require.Len(t, objWrapper, 1)
			return objWrapper[0]
		}

		t.Run("add vector to schema with legacy vector", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(ctx))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				Vectorizer:      text2vecContextionary,
				VectorIndexType: "hnsw",
				VectorConfig:    map[string]models.VectorConfig{},
			}

			// start with a collection that has only a legacy vector
			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID1).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love pizza",
				}).
				Do(ctx)
			require.NoError(t, err)

			// add a new named vector
			class.VectorConfig[contextionary] = models.VectorConfig{
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{},
				},
				VectorIndexType: "flat",
			}
			require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID2).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love burgers",
				}).
				Do(ctx)
			require.NoError(t, err)

			// add a second named vector
			class.VectorConfig[transformers] = models.VectorConfig{
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
			}
			require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID3).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love kebabs",
				}).
				Do(ctx)
			require.NoError(t, err)

			obj1 := fetchObject(t, UUID1)
			require.Len(t, obj1.Vector, 300)
			require.Len(t, obj1.Vectors, 0)

			obj2 := fetchObject(t, UUID2)
			require.Len(t, obj2.Vector, 300)
			require.Len(t, obj2.Vectors, 1)
			require.Equal(t, obj2.Vectors[contextionary].([]float32), []float32(obj2.Vector))

			obj3 := fetchObject(t, UUID3)
			require.Len(t, obj3.Vector, 300)
			require.Len(t, obj3.Vectors, 2)
			require.Len(t, obj3.Vectors[transformers], 384)
		})

		t.Run("add vector to schema with named vector", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(ctx))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					contextionary: {
						Vectorizer:      map[string]interface{}{text2vecContextionary: map[string]interface{}{}},
						VectorIndexType: "hnsw",
					},
				},
			}
			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID1).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love pizza",
				}).
				Do(ctx)
			require.NoError(t, err)

			// add a new named vector
			class.VectorConfig[transformers] = models.VectorConfig{
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{},
				},
				VectorIndexType: "flat",
			}
			require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID2).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love burgers",
				}).
				Do(ctx)
			require.NoError(t, err)

			obj1 := fetchObject(t, UUID1)
			require.Len(t, obj1.Vector, 0)
			require.Len(t, obj1.Vectors, 1)
			require.Len(t, obj1.Vectors[contextionary], 300)

			obj2 := fetchObject(t, UUID2)
			require.Len(t, obj2.Vector, 0)
			require.Len(t, obj2.Vectors, 2)
			require.Len(t, obj2.Vectors[contextionary], 300)
			require.Len(t, obj2.Vectors[transformers], 384)
		})

		t.Run("add colbert vector to a schema with legacy vector", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(ctx))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				Vectorizer:      text2vecContextionary,
				VectorIndexType: "flat",
				VectorConfig:    map[string]models.VectorConfig{},
			}

			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			_, err = client.Data().Creator().
				WithID(UUID1).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love pizza",
				}).
				Do(ctx)
			require.NoError(t, err)

			class.VectorConfig["multi"] = models.VectorConfig{
				VectorIndexConfig: map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
					},
				},
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
			}
			require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(ctx))

			multiVec := [][]float32{{1, 2, 3}, {4, 5, 6}}
			_, err = client.Data().Creator().
				WithID(UUID2).
				WithClassName(className).
				WithProperties(map[string]interface{}{
					"text": "I love pizza",
				}).
				WithVectors(map[string]models.Vector{
					"multi": multiVec,
				}).
				Do(ctx)
			require.NoError(t, err)

			obj1 := fetchObject(t, UUID1)
			require.Len(t, obj1.Vector, 300)
			require.Len(t, obj1.Vectors, 0)

			obj2 := fetchObject(t, UUID2)
			require.Len(t, obj2.Vector, 300)
			require.Len(t, obj2.Vectors, 1)
			require.Equal(t, multiVec, obj2.Vectors["multi"].([][]float32))

			nearVector := client.GraphQL().
				NearVectorArgBuilder().
				WithVectorsPerTarget(map[string][]models.Vector{
					"multi": {multiVec},
				})

			vectors := getVectorsWithNearVector(t, client, className, UUID2, nearVector, "multi")
			require.Len(t, vectors, 1)
			require.Equal(t, multiVec, vectors["multi"].([][]float32))
		})
	}
}
