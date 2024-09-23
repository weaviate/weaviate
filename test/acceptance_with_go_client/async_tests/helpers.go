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

package asynctests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func createClassLegacyVector(t *testing.T, client *wvt.Client, className string) *models.Class {
	class := &models.Class{
		Class:             className,
		Vectorizer:        "none",
		VectorIndexConfig: hnswCompressVectorIndexConfig(),
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

func batchObjects(t *testing.T, client *wvt.Client, objects []*models.Object) {
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

func randomObjects(className string, count int, dimensions int) []*models.Object {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([]*models.Object, count)
	for i := range vectors {
		vectors[i] = &models.Object{
			Class:  className,
			ID:     strfmt.UUID(uuid.New().String()),
			Vector: genVector(r, dimensions),
		}
	}
	return vectors
}

func updateRange(objects []*models.Object, start, end int) []*models.Object {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := start; i < end; i++ {
		objects[i].Vector = genVector(r, len(objects[i].Vector))
	}

	return objects[start:end]
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

func copyVectorsWith(vectors [][]float32, newValues [][]float32, index int) [][]float32 {
	newVectors := make([][]float32, len(vectors))
	copy(newVectors, vectors)
	for i, newValue := range newValues {
		if index+i >= len(newVectors) {
			break
		}

		newVectors[index+i] = newValue
	}

	return newVectors
}

func checkEventuallyIndexed(t *testing.T, client *wvt.Client, className string) {
	ctx := context.Background()
	timeout := 15 * time.Minute
	interval := 1 * time.Second

	var queueLength int64
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

		out, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Printf("  ==> status: %v\n", string(out))

		queueLength = resp.Nodes[0].Shards[0].VectorQueueLength
		fmt.Printf("  ==> queue length: %v\n", queueLength)
		if queueLength == 0 {
			break
		}
		time.Sleep(interval)
	}

	require.Zero(t, queueLength)
}

func getAllObjects(t *testing.T, client *wvt.Client, className string) []*models.Object {
	ctx := context.Background()
	var cursor string
	var objects []*models.Object
	for {
		resp, err := client.Data().ObjectsGetter().
			WithClassName(className).
			WithLimit(1000).
			WithAfter(cursor).
			WithVector().
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)

		if len(resp) == 0 {
			break
		}

		cursor = resp[len(resp)-1].ID.String()

		objects = append(objects, resp...)
	}

	return objects
}

func getAllVectors(t *testing.T, client *wvt.Client, className string) [][]float32 {
	objs := getAllObjects(t, client, className)
	vectors := make([][]float32, len(objs))

	for i, obj := range objs {
		vectors[i] = obj.Vector
	}

	return vectors
}

func objectsToVectors(objects []*models.Object) [][]float32 {
	vectors := make([][]float32, len(objects))
	for i, obj := range objects {
		vectors[i] = obj.Vector
	}
	return vectors
}
