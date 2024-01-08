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

package compression_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/test/docker"
)

func TestCompression_AdaptSegments(t *testing.T) {
	type testCase struct {
		dimensions int
	}

	testCases := []testCase{
		{dimensions: 768},
		{dimensions: 125},
		{dimensions: 4},
	}

	ctx := context.Background()
	className := "Compressed"

	t.Run("async indexing", func(t *testing.T) {
		compose, err := docker.New().
			WithWeaviate().
			WithWeaviateEnv("ASYNC_INDEXING", "true").
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: compose.GetWeaviate().URI()})
		require.NoError(t, err)
		cleanup := func() {
			err := client.Schema().AllDeleter().Do(ctx)
			require.NoError(t, err)
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("dimensions %d", tc.dimensions), func(t *testing.T) {
				defer cleanup()

				var class *models.Class

				t.Run("create class", func(t *testing.T) {
					class = createClass(t, client, className)
				})

				t.Run("compress", func(t *testing.T) {
					pq := class.VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{})
					pq["segments"] = 0
					pq["centroids"] = 256
					pq["trainingLimit"] = 999

					err = client.Schema().ClassUpdater().
						WithClass(class).
						Do(ctx)
					require.NoError(t, err)
				})

				t.Run("populate", func(t *testing.T) {
					populate(t, client, className, tc.dimensions)
				})

				t.Run("check eventually compressed", func(t *testing.T) {
					checkEventuallyCompressed(t, client, className)
				})
			})
		}
	})

	t.Run("sync indexing", func(t *testing.T) {
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
		require.NoError(t, err)
		cleanup := func() {
			err := client.Schema().AllDeleter().Do(ctx)
			require.NoError(t, err)
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("dimensions %d", tc.dimensions), func(t *testing.T) {
				defer cleanup()

				var class *models.Class

				t.Run("create class", func(t *testing.T) {
					class = createClass(t, client, className)
				})

				t.Run("populate", func(t *testing.T) {
					populate(t, client, className, tc.dimensions)
				})

				t.Run("compress", func(t *testing.T) {
					pq := class.VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{})
					pq["segments"] = 0
					pq["centroids"] = 256

					err = client.Schema().ClassUpdater().
						WithClass(class).
						Do(ctx)
					require.NoError(t, err)
				})

				t.Run("check eventually compressed", func(t *testing.T) {
					checkEventuallyCompressed(t, client, className)
				})
			})
		}
	})
}

func createClass(t *testing.T, client *wvt.Client, className string) *models.Class {
	ctx := context.Background()
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	vectorCacheMaxObjects := 10e12

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "int",
			DataType: schema.DataTypeInt.PropString(),
		}},
		Vectorizer: "none",
		VectorIndexConfig: map[string]interface{}{
			"maxConnections":        maxNeighbors,
			"efConstruction":        efConstruction,
			"ef":                    ef,
			"vectorCacheMaxObjects": vectorCacheMaxObjects,
			"distance":              "l2-squared",
			"pq": map[string]interface{}{
				"enabled": true,
				"encoder": map[string]interface{}{
					"distribution": hnsw.PQEncoderDistributionNormal,
					"type":         hnsw.PQEncoderTypeKMeans,
				},
			},
		},
	}

	err := client.Schema().ClassCreator().
		WithClass(class).
		Do(ctx)
	require.NoError(t, err)

	return class
}

func populate(t *testing.T, client *wvt.Client, className string, dimensions int) {
	ctx := context.Background()
	vectorsCount := 1000

	objects := make([]*models.Object, vectorsCount)
	for i, vector := range randomVecs(vectorsCount, dimensions) {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"int": i,
			},
			Vector: vector,
		}
	}
	resps, err := client.Batch().ObjectsBatcher().
		WithObjects(objects...).
		Do(ctx)
	require.NoError(t, err)
	require.Len(t, resps, vectorsCount)
	for _, resp := range resps {
		require.NotNil(t, resp.Result.Status)
		require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *resp.Result.Status)
	}
}

func checkEventuallyCompressed(t *testing.T, client *wvt.Client, className string) {
	ctx := context.Background()
	timeout := 15 * time.Minute
	interval := 1 * time.Second

	var compressed bool
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		resp, err := client.Cluster().NodesStatusGetter().
			WithClass(className).
			WithOutput("verbose").
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Nodes, 1)
		require.Len(t, resp.Nodes[0].Shards, 1)

		compressed = resp.Nodes[0].Shards[0].Compressed
		if compressed {
			break
		}
		time.Sleep(interval)
	}

	require.True(t, compressed)
}

func randomVecs(size int, dimensions int) [][]float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, size)
	for i := range vectors {
		vectors[i] = genVector(r, dimensions)
	}
	return vectors
}

func genVector(r *rand.Rand, dimensions int) []float32 {
	vector := make([]float32, dimensions)
	for i := range vector {
		// Some distances like dot could produce negative values when the vectors have negative values
		// This change will not affect anything when using a distance like l2, but will cover some bugs
		// when using distances like dot
		vector[i] = r.Float32()*2 - 1
	}
	return vector
}
