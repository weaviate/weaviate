//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package shards_status

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	className  = "ShardsStatusAsync"
	numObjects = 12000
	dims       = 64
	batchSize  = 500
)

// TestShardsStatusReportsAsyncIndexingQueues covers weaviate/0-weaviate-issues#449:
// GET /schema/{class}/shards must populate vectorQueueSize (it was hardwired to
// 0) and must not report READY while any replica of a shard is still consuming
// its async vector-index queues. Clients such as the python client's
// wait_for_vector_indexing poll exactly this endpoint and previously declared
// indexing finished the moment a single replica looked drained.
func TestShardsStatusReportsAsyncIndexingQueues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		VectorConfig: map[string]models.VectorConfig{
			"first": {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
			"second": {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "flat",
			},
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	var maxQueueSize int64
	sawNonReady := false

	// pollOnce returns true once every shard is READY with an empty
	// (all-replica) vector queue, tracking the largest queue size seen.
	pollOnce := func(t *testing.T) bool {
		resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
			clschema.NewSchemaObjectsShardsGetParams().WithClassName(className), nil)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Payload)

		done := true
		for _, shard := range resp.Payload {
			if shard.VectorQueueSize > maxQueueSize {
				maxQueueSize = shard.VectorQueueSize
			}
			if shard.Status != "READY" {
				sawNonReady = true
			}
			if shard.Status != "READY" || shard.VectorQueueSize != 0 {
				done = false
			}
		}
		return done
	}

	rnd := rand.New(rand.NewSource(42))
	randVector := func() []float32 {
		vec := make([]float32, dims)
		for i := range vec {
			vec[i] = rnd.Float32()
		}
		return vec
	}

	// Poll while importing so at least one sample lands mid-indexing.
	for start := 0; start < numObjects; start += batchSize {
		batch := make([]*models.Object, 0, batchSize)
		for i := start; i < start+batchSize && i < numObjects; i++ {
			batch = append(batch, &models.Object{
				Class:      className,
				Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
				Vectors: models.Vectors{
					"first":  randVector(),
					"second": randVector(),
				},
			})
		}
		helper.CreateObjectsBatch(t, batch)
		pollOnce(t)
	}

	require.Eventually(t, func() bool { return pollOnce(t) },
		5*time.Minute, 200*time.Millisecond,
		"shards never converged to READY with empty vector queues")

	// The distinguishing regression assertions: before the fix the endpoint
	// hardwired vectorQueueSize to 0, so with 12k objects x 2 target vectors
	// x 3 replicas at least one poll must have observed a non-empty queue.
	assert.Positive(t, maxQueueSize, "vectorQueueSize was never populated while async indexing was in flight")
	assert.True(t, sawNonReady, "status never left READY while async indexing was in flight")
}
