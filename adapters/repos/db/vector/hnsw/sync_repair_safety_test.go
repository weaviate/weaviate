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

package hnsw

// Tests for PR #11956 sync repair safety
// Verifies no deadlock when searches hit broken EP while inserts hold locks

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestSyncRepair_NoDeadlock_ConcurrentSearchAndInsert verifies that the
// synchronous repair in entrypointDistWithRepair does not deadlock when:
// - Searches hit a broken entrypoint and trigger repair
// - Inserts are running concurrently (potentially holding locks)
//
// This tests that h.RLock() is released before repair calls h.Lock().
// If there's a deadlock, this test will hang and timeout.
func TestSyncRepair_NoDeadlock_ConcurrentSearchAndInsert(t *testing.T) {
	ctx := context.Background()
	const numNodes = 20
	const numSearchers = 10
	const numInserters = 5

	vectors := make([][]float32, numNodes+numInserters*10)
	vectorErrors := make([]error, numNodes+numInserters*10)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var mu sync.Mutex

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if int(id) < len(vectorErrors) && vectorErrors[id] != nil {
			return nil, vectorErrors[id]
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if int(id) < len(vectorErrors) && vectorErrors[id] != nil {
			return nil, vectorErrors[id]
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "sync-repair-deadlock-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             vectorForIDThunk,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: tempVectorThunk,
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	// Insert initial nodes
	for i := 0; i < numNodes; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Delete the entrypoint to force repair path
	ep := index.getEntrypoint()
	mu.Lock()
	vectorErrors[ep] = storobj.NewErrNotFoundf(ep, "deleted")
	mu.Unlock()
	index.cache.Delete(ctx, ep)

	var wg sync.WaitGroup
	var completedSearches atomic.Int32
	var completedInserts atomic.Int32

	// Launch concurrent searchers - will hit broken EP and trigger repair
	for s := 0; s < numSearchers; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				query := []float32{float32(id), float32(i), 1.0}
				_, _, _ = index.SearchByVector(ctx, query, 5, nil)
				completedSearches.Add(1)
			}
		}(s)
	}

	// Launch concurrent inserters - hold locks during insert
	nextID := uint64(numNodes)
	var idMu sync.Mutex
	for ins := 0; ins < numInserters; ins++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				idMu.Lock()
				id := nextID
				nextID++
				idMu.Unlock()

				vec := []float32{float32(id), float32(id + 1), float32(id + 2)}
				mu.Lock()
				if int(id) < len(vectors) {
					vectors[id] = vec
				}
				mu.Unlock()

				_ = index.Add(ctx, id, vec)
				completedInserts.Add(1)
			}
		}()
	}

	// If there's a deadlock, this will hang and timeout
	wg.Wait()

	t.Logf("Completed: %d searches, %d inserts - NO DEADLOCK",
		completedSearches.Load(), completedInserts.Load())
}

// TestSyncRepair_EPRace_ConcurrentRepair verifies that concurrent EP repairs
// are handled safely by the CAS mechanism (delete.go:697-712).
// Multiple goroutines attempt repair simultaneously - only one should succeed.
func TestSyncRepair_EPRace_ConcurrentRepair(t *testing.T) {
	ctx := context.Background()
	const numNodes = 10
	const numRepairers = 20

	vectors := make([][]float32, numNodes)
	vectorErrors := make([]error, numNodes)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var mu sync.Mutex

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if int(id) < len(vectorErrors) && vectorErrors[id] != nil {
			return nil, vectorErrors[id]
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if int(id) < len(vectorErrors) && vectorErrors[id] != nil {
			return nil, vectorErrors[id]
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "sync-repair-ep-race-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             vectorForIDThunk,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: tempVectorThunk,
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	// Insert initial nodes
	for i := 0; i < numNodes; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Delete entrypoint
	ep := index.getEntrypoint()
	mu.Lock()
	vectorErrors[ep] = storobj.NewErrNotFoundf(ep, "deleted")
	mu.Unlock()
	index.cache.Delete(ctx, ep)

	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Start barrier - all goroutines wait here until all are ready
	startBarrier := make(chan struct{})

	// Launch many concurrent searches that will all hit the broken EP
	for r := 0; r < numRepairers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startBarrier // Wait for start signal

			query := []float32{float32(id), 1.0, 2.0}
			results, _, err := index.SearchByVector(ctx, query, 5, nil)
			if err == nil && len(results) > 0 {
				successCount.Add(1)
			}
		}(r)
	}

	// Release all goroutines simultaneously
	close(startBarrier)
	wg.Wait()

	// All searches should complete (no race corruption)
	// Final EP should be valid (not the deleted one)
	finalEP := index.getEntrypoint()
	require.NotEqual(t, ep, finalEP, "entrypoint should have been repaired")

	t.Logf("CAS race test: %d successful searches, EP changed from %d to %d",
		successCount.Load(), ep, finalEP)
}
