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
	dims      = 64
	batchSize = 500
)

// shardsPoller polls GET /schema/{class}/shards and tracks the largest
// vectorQueueSize and any non-READY status it observes.
type shardsPoller struct {
	className    string
	tenant       *string
	maxQueueSize int64
	sawNonReady  bool
}

// poll returns done=true once every returned shard is READY with an empty
// (all-replica) vector queue. Transient request errors (e.g. a replica
// rejecting reads while loading) are returned, not fatal, so callers inside
// require.Eventually can retry.
func (p *shardsPoller) poll(t *testing.T) (bool, error) {
	params := clschema.NewSchemaObjectsShardsGetParams().WithClassName(p.className)
	if p.tenant != nil {
		params = params.WithTenant(p.tenant)
	}
	resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(params, nil)
	if err != nil {
		return false, err
	}
	if len(resp.Payload) == 0 {
		return false, fmt.Errorf("empty shards response")
	}

	done := true
	for _, shard := range resp.Payload {
		if shard.VectorQueueSize > p.maxQueueSize {
			p.maxQueueSize = shard.VectorQueueSize
		}
		if shard.Status != "READY" {
			p.sawNonReady = true
		}
		if shard.Status != "READY" || shard.VectorQueueSize != 0 {
			done = false
		}
	}
	return done, nil
}

func randVector(rnd *rand.Rand) []float32 {
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = rnd.Float32()
	}
	return vec
}

// TestShardsStatus covers weaviate/0-weaviate-issues#449: GET
// /schema/{class}/shards must populate vectorQueueSize (it was hardwired to 0)
// and must not report READY while any replica of a shard is still consuming
// its async vector-index queues. Clients such as the python client's
// wait_for_vector_indexing poll exactly this endpoint and previously declared
// indexing finished the moment a single replica looked drained.
func TestShardsStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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

	t.Run("replicated collection with named vectors", func(t *testing.T) {
		className := "ShardsStatusAsync"
		helper.CreateClass(t, &models.Class{
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
		})
		defer helper.DeleteClass(t, className)

		poller := &shardsPoller{className: className}
		rnd := rand.New(rand.NewSource(42))

		// Poll while importing so at least one sample lands mid-indexing.
		const numObjects = 12000
		for start := 0; start < numObjects; start += batchSize {
			batch := make([]*models.Object, 0, batchSize)
			for i := start; i < start+batchSize && i < numObjects; i++ {
				batch = append(batch, &models.Object{
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						"first":  randVector(rnd),
						"second": randVector(rnd),
					},
				})
			}
			helper.CreateObjectsBatch(t, batch)
			_, err := poller.poll(t)
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			done, err := poller.poll(t)
			return err == nil && done
		}, 5*time.Minute, 200*time.Millisecond,
			"shards never converged to READY with empty vector queues")

		// The distinguishing regression assertions: before the fix the endpoint
		// hardwired vectorQueueSize to 0, so with 12k objects x 2 target vectors
		// x 3 replicas at least one poll must have observed a non-empty queue.
		assert.Positive(t, poller.maxQueueSize,
			"vectorQueueSize was never populated while async indexing was in flight")
		assert.True(t, poller.sawNonReady,
			"status never left READY while async indexing was in flight")
	})

	t.Run("multi tenant collection", func(t *testing.T) {
		className := "ShardsStatusAsyncMT"
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 3},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			VectorConfig: map[string]models.VectorConfig{
				"first": {
					Vectorizer:      map[string]any{"none": map[string]any{}},
					VectorIndexType: "hnsw",
				},
			},
		})
		defer helper.DeleteClass(t, className)

		tenants := []string{"tenant1", "tenant2"}
		helper.CreateTenants(t, className, []*models.Tenant{
			{Name: tenants[0]}, {Name: tenants[1]},
		})

		poller := &shardsPoller{className: className}
		rnd := rand.New(rand.NewSource(43))

		const objectsPerTenant = 6000
		for _, tenant := range tenants {
			for start := 0; start < objectsPerTenant; start += batchSize {
				batch := make([]*models.Object, 0, batchSize)
				for i := start; i < start+batchSize && i < objectsPerTenant; i++ {
					batch = append(batch, &models.Object{
						Class:      className,
						Tenant:     tenant,
						Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
						Vectors:    models.Vectors{"first": randVector(rnd)},
					})
				}
				helper.CreateObjectsBatch(t, batch)
				_, err := poller.poll(t)
				require.NoError(t, err)
			}
		}

		require.Eventually(t, func() bool {
			done, err := poller.poll(t)
			return err == nil && done
		}, 5*time.Minute, 200*time.Millisecond,
			"tenant shards never converged to READY with empty vector queues")

		assert.Positive(t, poller.maxQueueSize,
			"vectorQueueSize was never populated for tenant shards while async indexing was in flight")

		// The ?tenant= filter must return exactly that tenant's shard, with
		// the aggregated queue drained.
		tenant := tenants[0]
		resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
			clschema.NewSchemaObjectsShardsGetParams().
				WithClassName(className).WithTenant(&tenant), nil)
		require.NoError(t, err)
		require.Len(t, resp.Payload, 1)
		assert.Equal(t, tenant, resp.Payload[0].Name)
		assert.Equal(t, "READY", resp.Payload[0].Status)
		assert.Zero(t, resp.Payload[0].VectorQueueSize)
	})
}
