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

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TestObserveAggregateGauges_NoDeadlockWithBackupContention exercises the
// new sweep against the contention pattern Etienne flagged: a backup holding
// shardCreateLocks.Lock(name) while the sweep iterates loaded shards. The
// sweep must not deadlock and must complete repeatedly within the test
// budget. The race detector catches any data races introduced.
func TestObserveAggregateGauges_NoDeadlockWithBackupContention(t *testing.T) {
	metrics := monitoring.GetMetrics()
	metrics.Group = true
	t.Cleanup(func() { metrics.Group = false })

	class := vectorTestClass()
	db := createTestDatabaseWithClass(t, metrics, class)
	idx := db.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)
	insertVectors(t, idx, 200, defaultVectorDimensions)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var sweepIters, backupIters atomic.Int64
	var wg sync.WaitGroup

	// Sweep loop: hammer observeAggregateGauges.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			db.metricsObserver.observeAggregateGauges()
			sweepIters.Add(1)
		}
	}()

	// Backup-style contention: take shardCreateLocks.Lock(name) on every
	// shard for a brief moment, simulating backupShardWithHardlinks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			idx.ForEachShard(func(name string, _ ShardLike) error {
				idx.shardCreateLocks.Lock(name)
				time.Sleep(200 * time.Microsecond)
				idx.shardCreateLocks.Unlock(name)
				return nil
			})
			backupIters.Add(1)
		}
	}()

	// Hot-path inserts.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 1000
		for ctx.Err() == nil {
			obj := buildVectorObject(class.Class, i, defaultVectorDimensions)
			_ = idx.putObject(context.Background(), obj, nil, "", 0)
			i++
		}
	}()

	wg.Wait()

	require.Greater(t, sweepIters.Load(), int64(0), "sweep must run at least once")
	require.Greater(t, backupIters.Load(), int64(0), "backup contention must fire")
}

// TestObserveAggregateGauges_SkipsColdTenants verifies the sweep does not
// force-activate lazy-loaded shards. With grouping on, the sweep must walk
// only loaded shards.
func TestObserveAggregateGauges_SkipsColdTenants(t *testing.T) {
	metrics := monitoring.GetMetrics()
	metrics.Group = true
	t.Cleanup(func() { metrics.Group = false })

	class := vectorTestClass()
	db := createTestDatabaseWithClass(t, metrics, class)
	idx := db.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)

	loadedBefore := 0
	idx.ForEachLoadedShard(func(string, ShardLike) error {
		loadedBefore++
		return nil
	})

	db.metricsObserver.observeAggregateGauges()

	loadedAfter := 0
	idx.ForEachLoadedShard(func(string, ShardLike) error {
		loadedAfter++
		return nil
	})

	assert.Equal(t, loadedBefore, loadedAfter, "sweep must not load cold shards")
}

// TestObserveAggregateGauges_SumsAcrossShard exercises the correctness path:
// per-shard writes are gated off in grouped mode, so the n/a series must
// reflect the values read directly from the shard's HNSW index.
func TestObserveAggregateGauges_SumsAcrossShard(t *testing.T) {
	metrics := monitoring.GetMetrics()
	metrics.Group = true
	t.Cleanup(func() { metrics.Group = false })

	class := vectorTestClass()
	db := createTestDatabaseWithClass(t, metrics, class)
	idx := db.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)
	const n = 50
	insertVectors(t, idx, n, defaultVectorDimensions)

	// Async indexing: wait until HNSW has caught up.
	want := 0
	require.Eventually(t, func() bool {
		want = 0
		idx.ForEachLoadedShard(func(_ string, sl ShardLike) error {
			sl.ForEachVectorIndex(func(_ string, vi VectorIndex) error {
				if h, ok := vi.(*hnsw.HNSW); ok {
					want += h.Size()
				}
				return nil
			})
			return nil
		})
		return want > 0
	}, 10*time.Second, 50*time.Millisecond, "HNSW must report non-zero size after inserts")

	db.metricsObserver.observeAggregateGauges()

	naLabels := prometheus.Labels{"class_name": "n/a", "shard_name": "n/a"}
	got := testutil.ToFloat64(metrics.VectorIndexSize.With(naLabels))
	assert.Equal(t, float64(want), got)
}

// vectorTestClass returns a single-tenant class with a default HNSW
// vector index, suitable for the createTestDatabaseWithClass helper which
// produces a singleShardState fixture.
func vectorTestClass() *models.Class {
	return &models.Class{
		Class:             "VectorTestClass",
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
}

func buildVectorObject(className string, i, dims int) *storobj.Object {
	obj := &models.Object{
		Class: className,
		ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
		Properties: map[string]interface{}{
			"name": fmt.Sprintf("test-object-%d", i),
		},
	}
	v := make([]float32, dims)
	for j := range v {
		v[j] = float32(i+j) / 1000.0
	}
	return storobj.FromObject(obj, v, nil, nil)
}

func insertVectors(t *testing.T, idx *Index, count, dims int) {
	t.Helper()
	for i := 0; i < count; i++ {
		obj := buildVectorObject(string(idx.Config.ClassName), i, dims)
		require.NoError(t, idx.putObject(context.Background(), obj, nil, "", 0))
	}
}
