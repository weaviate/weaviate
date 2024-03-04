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
	type dimensionTestCase struct {
		vectors [][]float32
	}
	type vectorTypeTestCase struct {
		name          string
		createClassFn func(t *testing.T, client *wvt.Client, className string) *models.Class
		populateFn    func(t *testing.T, client *wvt.Client, className string, vectors [][]float32)
		compressFn    func(t *testing.T, client *wvt.Client, class *models.Class)
	}

	ctx := context.Background()
	className := "Compressed"
	vectorsCount := 1000

	dimensionTestCases := []dimensionTestCase{
		{vectors: randomVecs(vectorsCount, 768)},
		{vectors: randomVecs(vectorsCount, 4)},
	}

	t.Run("async indexing", func(t *testing.T) {
		vectorTypeTestCases := []vectorTypeTestCase{
			{
				name:          "legacy vector",
				createClassFn: createClassLegacyVector,
				populateFn:    populateLegacyVector,
				compressFn:    compressLegacyVectorAsync,
			},
			{
				name:          "target vectors",
				createClassFn: createClassTargetVectors,
				populateFn:    populateTargetVectors,
				compressFn:    compressTargetVectorsAsync,
			},
		}

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

		for _, dtc := range dimensionTestCases {
			t.Run(fmt.Sprintf("dimensions %d", len(dtc.vectors[0])), func(t *testing.T) {
				for _, vttc := range vectorTypeTestCases {
					t.Run(vttc.name, func(t *testing.T) {
						defer cleanup()

						var class *models.Class

						t.Run("create class", func(t *testing.T) {
							class = vttc.createClassFn(t, client, className)
						})

						t.Run("compress", func(t *testing.T) {
							vttc.compressFn(t, client, class)
						})

						t.Run("populate", func(t *testing.T) {
							vttc.populateFn(t, client, className, dtc.vectors)
						})

						t.Run("check eventually compressed", func(t *testing.T) {
							checkEventuallyCompressed(t, client, className)
						})
					})
				}
			})
		}
	})

	t.Run("sync indexing", func(t *testing.T) {
		vectorTypeTestCases := []vectorTypeTestCase{
			{
				name:          "legacy vector",
				createClassFn: createClassLegacyVector,
				populateFn:    populateLegacyVector,
				compressFn:    compressLegacyVectorSync,
			},
			{
				name:          "target vectors",
				createClassFn: createClassTargetVectors,
				populateFn:    populateTargetVectors,
				compressFn:    compressTargetVectorsSync,
			},
		}

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
		require.NoError(t, err)
		cleanup := func() {
			err := client.Schema().AllDeleter().Do(ctx)
			require.NoError(t, err)
		}

		for _, dtc := range dimensionTestCases {
			t.Run(fmt.Sprintf("dimensions %d", len(dtc.vectors[0])), func(t *testing.T) {
				for _, vttc := range vectorTypeTestCases {
					t.Run(vttc.name, func(t *testing.T) {
						defer cleanup()

						var class *models.Class

						t.Run("create class", func(t *testing.T) {
							class = vttc.createClassFn(t, client, className)
						})

						t.Run("populate", func(t *testing.T) {
							vttc.populateFn(t, client, className, dtc.vectors)
						})

						t.Run("compress", func(t *testing.T) {
							vttc.compressFn(t, client, class)
						})

						t.Run("check eventually compressed", func(t *testing.T) {
							checkEventuallyCompressed(t, client, className)
						})
					})
				}
			})
		}
	})
}

func createClassLegacyVector(t *testing.T, client *wvt.Client, className string) *models.Class {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "int",
			DataType: schema.DataTypeInt.PropString(),
		}},
		Vectorizer:        "none",
		VectorIndexConfig: hnswCompressVectorIndexConfig(),
	}

	createClass(t, client, class)
	return class
}

func createClassTargetVectors(t *testing.T, client *wvt.Client, className string) *models.Class {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "int",
			DataType: schema.DataTypeInt.PropString(),
		}},
		VectorConfig: map[string]models.VectorConfig{
			"vectorFlat": {
				VectorIndexType: "flat",
				Vectorizer: map[string]interface{}{
					"none": struct{}{},
				},
			},
			"vectorHnswCompress": {
				VectorIndexType: "hnsw",
				Vectorizer: map[string]interface{}{
					"none": struct{}{},
				},
				VectorIndexConfig: hnswCompressVectorIndexConfig(),
			},
		},
	}

	createClass(t, client, class)
	return class
}

func createClass(t *testing.T, client *wvt.Client, class *models.Class) {
	err := client.Schema().ClassCreator().
		WithClass(class).
		Do(context.Background())
	require.NoError(t, err)
}

func hnswCompressVectorIndexConfig() map[string]interface{} {
	return map[string]interface{}{
		"maxConnections":        32,
		"efConstruction":        64,
		"ef":                    32,
		"vectorCacheMaxObjects": 10e12,
		"distance":              "l2-squared",
		"pq": map[string]interface{}{
			"enabled": true,
			"encoder": map[string]interface{}{
				"distribution": hnsw.PQEncoderDistributionNormal,
				"type":         hnsw.PQEncoderTypeKMeans,
			},
		},
	}
}

func populateLegacyVector(t *testing.T, client *wvt.Client, className string, vectors [][]float32) {
	objects := make([]*models.Object, len(vectors))
	for i, vector := range vectors {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"int": i,
			},
			Vector: vector,
		}
	}

	populate(t, client, objects)
}

func populateTargetVectors(t *testing.T, client *wvt.Client, className string, vectors [][]float32) {
	objects := make([]*models.Object, len(vectors))
	for i, vector := range vectors {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"int": i,
			},
			Vectors: models.Vectors{
				"vectorFlat":         vector,
				"vectorHnswCompress": vector,
			},
		}
	}

	populate(t, client, objects)
}

func populate(t *testing.T, client *wvt.Client, objects []*models.Object) {
	resps, err := client.Batch().ObjectsBatcher().
		WithObjects(objects...).
		Do(context.Background())
	require.NoError(t, err)
	require.Len(t, resps, len(objects))
	for _, resp := range resps {
		require.NotNil(t, resp.Result.Status)
		require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *resp.Result.Status)
	}
}

func compressLegacyVectorAsync(t *testing.T, client *wvt.Client, class *models.Class) {
	updateConfigAsync(class.VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{}))
	compress(t, client, class)
}

func compressLegacyVectorSync(t *testing.T, client *wvt.Client, class *models.Class) {
	updateConfigSync(class.VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{}))
	compress(t, client, class)
}

func compressTargetVectorsAsync(t *testing.T, client *wvt.Client, class *models.Class) {
	updateConfigAsync(class.VectorConfig["vectorHnswCompress"].
		VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{}))
	compress(t, client, class)
}

func compressTargetVectorsSync(t *testing.T, client *wvt.Client, class *models.Class) {
	updateConfigSync(class.VectorConfig["vectorHnswCompress"].
		VectorIndexConfig.(map[string]interface{})["pq"].(map[string]interface{}))
	compress(t, client, class)
}

func updateConfigAsync(pq map[string]interface{}) {
	pq["segments"] = 0
	pq["centroids"] = 256
	pq["trainingLimit"] = 999
}

func updateConfigSync(pq map[string]interface{}) {
	pq["segments"] = 0
	pq["centroids"] = 256
}

func compress(t *testing.T, client *wvt.Client, class *models.Class) {
	err := client.Schema().ClassUpdater().
		WithClass(class).
		Do(context.Background())
	require.NoError(t, err)
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
		fmt.Printf("  ==> compressed %v\n", compressed)
		if compressed {
			break
		}
		time.Sleep(interval)
	}

	require.True(t, compressed)
}

func randomVecs(count int, dimensions int) [][]float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, count)
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
