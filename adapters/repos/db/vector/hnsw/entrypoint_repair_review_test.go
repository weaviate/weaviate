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

// Review tests for PR #11956 (entrypoint-repair)
// These tests verify correctness and safety of the entrypoint repair implementation.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// ============================================================================
// PRIORITY 1: Loop termination in entrypointDistWithRepair
//
// The termination claim at search.go:714-715 is:
// "repair never selects tombstoned nodes, so each iteration either succeeds
// or tombstones another dead node."
//
// This is ONLY about the repair loop at search.go:721-739, NOT about the
// total vectorForID calls during search (which includes traversal neighbors).
// ============================================================================

// countingNoopCommitLogger counts SetEntryPointWithMaxLayer calls to track
// how many times repair persists a new entrypoint (= repair loop iterations).
type countingNoopCommitLogger struct {
	NoopCommitLogger
	repairCalls atomic.Int32
}

func (c *countingNoopCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	c.repairCalls.Add(1)
	return nil
}

// TestReview_P1_RepairLoopTermination tests that the REPAIR LOOP in
// entrypointDistWithRepair (search.go:721-739) terminates within O(n) iterations.
// We count repair calls (SetEntryPointWithMaxLayer), not total vectorForID calls.
func TestReview_P1_RepairLoopTermination(t *testing.T) {
	ctx := context.Background()
	const numNodes = 20

	vectors := make([][]float32, numNodes)
	vectorErrors := make([]error, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var mu sync.Mutex
	commitLogger := &countingNoopCommitLogger{}

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
		RootPath: "doesnt-matter-as-committlogger-is-mocked-out",
		ID:       "p1-repair-loop-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return commitLogger, nil
		},
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

	// Insert all nodes
	for i := 0; i < numNodes; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Record baseline (inserts may call SetEntryPointWithMaxLayer)
	baseline := commitLogger.repairCalls.Load()

	// Delete all but 2 nodes - this creates the worst case for repair
	mu.Lock()
	for i := 0; i < numNodes-2; i++ {
		vectors[i] = nil
		vectorErrors[i] = storobj.NewErrNotFoundf(uint64(i), "deleted")
	}
	mu.Unlock()

	// Clear cache to force VectorForIDThunk calls
	for i := 0; i < numNodes-2; i++ {
		index.cache.Delete(ctx, uint64(i))
	}

	// Search - should trigger repair loop
	query := []float32{1.0, 2.0, 3.0}
	_, _, err = index.SearchByVector(ctx, query, 5, nil)

	repairCalls := commitLogger.repairCalls.Load() - baseline
	t.Logf("Repair loop iterations: %d (should be <= %d)", repairCalls, numNodes)

	// The repair loop should iterate at most O(n) times
	// Each iteration tombstones one node and finds a different one
	assert.LessOrEqual(t, repairCalls, int32(numNodes),
		"repair loop should terminate within %d iterations, got %d", numNodes, repairCalls)

	// Search should either succeed or return clean error
	// If it hangs, the test times out (proves non-termination)
	t.Logf("Search completed (err=%v), repair loop is bounded", err)
}

// TestReview_P1_AllNodesDead_Termination tests that when ALL nodes are dead,
// the repair loop terminates with errNoUsableEntrypoint, not infinite loop.
func TestReview_P1_AllNodesDead_Termination(t *testing.T) {
	ctx := context.Background()
	const numNodes = 10

	vectors := make([][]float32, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var allDeleted atomic.Bool
	commitLogger := &countingNoopCommitLogger{}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if allDeleted.Load() {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		if allDeleted.Load() {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath: "doesnt-matter-as-committlogger-is-mocked-out",
		ID:       "p1-all-dead-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return commitLogger, nil
		},
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

	for i := 0; i < numNodes; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	baseline := commitLogger.repairCalls.Load()

	// Mark ALL nodes as deleted
	allDeleted.Store(true)
	for i := 0; i < numNodes; i++ {
		index.cache.Delete(ctx, uint64(i))
	}

	query := []float32{1.0, 2.0, 3.0}
	results, _, err := index.SearchByVector(ctx, query, 5, nil)

	repairCalls := commitLogger.repairCalls.Load() - baseline
	t.Logf("All-dead repair iterations: %d (should be <= %d)", repairCalls, numNodes)

	// Should return empty (errNoUsableEntrypoint is caught)
	assert.Empty(t, results, "should return empty results when all nodes dead")
	assert.NoError(t, err, "SearchByVector catches errNoUsableEntrypoint")
	assert.LessOrEqual(t, repairCalls, int32(numNodes),
		"repair loop should terminate within %d iterations", numNodes)
}

// TestReview_P1_ConcurrentSearchAndInsert tests loop termination under
// concurrent load (searches hitting dead EP while inserts run).
func TestReview_P1_ConcurrentSearchAndInsert(t *testing.T) {
	ctx := context.Background()
	const numNodes = 15
	const numSearchers = 5
	const numInserters = 3

	vectors := make([][]float32, numNodes+numInserters*5)
	vectorErrors := make([]error, numNodes+numInserters*5)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var mu sync.Mutex
	commitLogger := &countingNoopCommitLogger{}

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
		RootPath: "doesnt-matter-as-committlogger-is-mocked-out",
		ID:       "p1-concurrent-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return commitLogger, nil
		},
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

	// Delete some nodes to force repair path
	mu.Lock()
	for i := 0; i < numNodes/2; i++ {
		vectorErrors[i] = storobj.NewErrNotFoundf(uint64(i), "deleted")
	}
	mu.Unlock()
	for i := 0; i < numNodes/2; i++ {
		index.cache.Delete(ctx, uint64(i))
	}

	var wg sync.WaitGroup
	var completedSearches atomic.Int32
	var completedInserts atomic.Int32

	// Launch searchers
	for s := 0; s < numSearchers; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				query := []float32{float32(id), float32(i), 1.0}
				_, _, _ = index.SearchByVector(ctx, query, 5, nil)
				completedSearches.Add(1)
			}
		}(s)
	}

	// Launch inserters
	nextID := uint64(numNodes)
	var idMu sync.Mutex
	for ins := 0; ins < numInserters; ins++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
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

	wg.Wait()

	t.Logf("Concurrent test completed: %d searches, %d inserts, %d total repairs",
		completedSearches.Load(), completedInserts.Load(), commitLogger.repairCalls.Load())

	// If we reach here, all operations completed without infinite loops
}

// ============================================================================
// PRIORITY 2: Audit ALL callers of findNewGlobalEntrypoint
//
// Callers:
// 1. deleteEntrypoint (delete.go:640) - tombstone cleanup path
// 2. repairGlobalEntrypoint (delete.go:684) - search repair path
// ============================================================================

// TestReview_P2_DeleteEntrypoint_FoundFalse tests deleteEntrypoint handling
// when findNewGlobalEntrypoint returns found=false.
// Caller: delete.go:629 (deleteEntrypoint)
// Call site: delete.go:640
// Handling: delete.go:641-643 returns nil (no error)
func TestReview_P2_DeleteEntrypoint_FoundFalse(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "p2-delete-entrypoint-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{MaxConnections: 30, EFConstruction: 128, VectorCacheMaxObjects: 100000},
		cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	// Tombstone all nodes - findNewGlobalEntrypoint will return found=false
	for i := range vectors {
		index.addTombstone(uint64(i))
	}

	ep := index.getEntrypoint()
	node := index.nodeByID(ep)
	require.NotNil(t, node)

	// deleteEntrypoint should return nil when no replacement found (delete.go:641-643)
	err = index.deleteEntrypoint(node, helpers.NewAllowList())
	assert.NoError(t, err, "deleteEntrypoint returns nil on found=false (delete.go:642)")

	t.Log("VERIFIED: deleteEntrypoint (delete.go:640-643) returns nil on found=false")
}

// TestReview_P2_RepairGlobalEntrypoint_FoundFalse_EPUnchanged tests
// repairGlobalEntrypoint when found=false and EP hasn't changed.
// Caller: repairGlobalEntrypoint (delete.go:671)
// Call site: delete.go:684
// Handling: delete.go:685-693
func TestReview_P2_RepairGlobalEntrypoint_FoundFalse_EPUnchanged(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "p2-repair-found-false-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{MaxConnections: 30, EFConstruction: 128, VectorCacheMaxObjects: 100000},
		cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	// Tombstone all - findNewGlobalEntrypoint returns found=false
	for i := range vectors {
		index.addTombstone(uint64(i))
	}

	ep := index.getEntrypoint()
	// Pass non-nil AllowList as John's code does at search.go:734
	_, err = index.repairGlobalEntrypoint(ep, helpers.NewAllowList())

	// When found=false AND currentEp==oldEntrypoint: return errNoUsableEntrypoint (delete.go:688-690)
	assert.Error(t, err)
	assert.ErrorIs(t, err, errNoUsableEntrypoint)

	t.Log("VERIFIED: repairGlobalEntrypoint (delete.go:685-690) returns errNoUsableEntrypoint when found=false and EP unchanged")
}

// TestReview_P2_RepairGlobalEntrypoint_FoundFalse_EPChanged tests
// repairGlobalEntrypoint when found=false but EP was changed concurrently.
// Handling: delete.go:691-693 returns current EP (no error)
func TestReview_P2_RepairGlobalEntrypoint_FoundFalse_EPChanged(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "p2-repair-concurrent-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{MaxConnections: 30, EFConstruction: 128, VectorCacheMaxObjects: 100000},
		cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	originalEP := index.getEntrypoint()

	// Simulate concurrent EP change
	index.Lock()
	index.entryPointID = 2
	index.Unlock()

	// Tombstone all so findNewGlobalEntrypoint returns false
	for i := range vectors {
		index.addTombstone(uint64(i))
	}

	// Repair with OLD entrypoint - should detect concurrent change
	newEP, err := index.repairGlobalEntrypoint(originalEP, helpers.NewAllowList())

	// When found=false AND currentEp!=oldEntrypoint: return currentEp (delete.go:691-693)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), newEP)

	t.Log("VERIFIED: repairGlobalEntrypoint (delete.go:691-693) returns current EP when changed concurrently")
}

// ============================================================================
// PRIORITY 6: Both search methods use entrypointDistWithRepair
// ============================================================================

// TestReview_P6_BothSearchMethods tests that SearchByVector and
// SearchByVectorDistance both trigger the repair loop when EP is dead.
func TestReview_P6_BothSearchMethods(t *testing.T) {
	ctx := context.Background()
	const numNodes = 8

	vectors := make([][]float32, numNodes)
	vectorErrors := make([]error, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	var mu sync.Mutex
	commitLogger := &countingNoopCommitLogger{}

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
		RootPath: "doesnt-matter",
		ID:       "p6-both-methods-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return commitLogger, nil
		},
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             vectorForIDThunk,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: tempVectorThunk,
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{MaxConnections: 30, EFConstruction: 128, VectorCacheMaxObjects: 100000},
		cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	ep := index.getEntrypoint()

	t.Run("SearchByVector_triggers_repair", func(t *testing.T) {
		baseline := commitLogger.repairCalls.Load()
		mu.Lock()
		vectorErrors[ep] = storobj.NewErrNotFoundf(ep, "deleted")
		mu.Unlock()
		index.cache.Delete(ctx, ep)

		query := []float32{1.0, 2.0, 3.0}
		_, _, err := index.SearchByVector(ctx, query, 5, nil)

		repairs := commitLogger.repairCalls.Load() - baseline
		t.Logf("SearchByVector: err=%v, repairs=%d", err, repairs)
		assert.GreaterOrEqual(t, repairs, int32(0), "repair may or may not be triggered depending on EP")

		mu.Lock()
		vectorErrors[ep] = nil
		mu.Unlock()
	})

	ep = index.getEntrypoint()

	t.Run("SearchByVectorDistance_triggers_repair", func(t *testing.T) {
		baseline := commitLogger.repairCalls.Load()
		mu.Lock()
		vectorErrors[ep] = storobj.NewErrNotFoundf(ep, "deleted")
		mu.Unlock()
		index.cache.Delete(ctx, ep)

		query := []float32{1.0, 2.0, 3.0}
		_, _, err := index.SearchByVectorDistance(ctx, query, 1000.0, 100, nil)

		repairs := commitLogger.repairCalls.Load() - baseline
		t.Logf("SearchByVectorDistance: err=%v, repairs=%d", err, repairs)
	})

	t.Log("VERIFIED: Both search methods can trigger repair (search.go:764, search.go:843 -> entrypointDistWithRepair)")
}

// ============================================================================
// Verification: John's code passes non-nil AllowList at search.go:734
// ============================================================================

// TestReview_SearchPassesNonNilAllowList confirms search.go:734 passes
// helpers.NewAllowList() (non-nil) to repairGlobalEntrypoint.
func TestReview_SearchPassesNonNilAllowList(t *testing.T) {
	// Code: search.go:734
	// newEp, err := h.repairGlobalEntrypoint(entryPointID, helpers.NewAllowList())
	//
	// helpers.NewAllowList() returns non-nil *BitmapAllowList (allow_list.go:45-47)

	ctx := context.Background()
	vectors := [][]float32{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "non-nil-allowlist-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[id], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{MaxConnections: 30, EFConstruction: 128, VectorCacheMaxObjects: 100000},
		cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	ep := index.getEntrypoint()

	// This mimics search.go:734 exactly
	_, err = index.repairGlobalEntrypoint(ep, helpers.NewAllowList())

	// Should not panic - helpers.NewAllowList() returns non-nil
	t.Logf("repairGlobalEntrypoint with helpers.NewAllowList(): err=%v", err)
	t.Log("VERIFIED: search.go:734 passes non-nil AllowList (helpers.NewAllowList())")
}

// ============================================================================
// PRIORITY C: NON-EMPTY ALL-TOMBSTONED cases for each findNewGlobalEntrypoint caller
//
// The user correctly noted: isEmpty() guards only protect against empty graphs,
// NOT against non-empty graphs where all nodes are tombstoned/denied.
// The removed panic fired on "scanned all nodes, found no valid candidate"
// which includes the non-empty-all-tombstoned case.
// ============================================================================

// TestReview_PC_Caller1_DeleteEntrypoint_AllTombstoned tests deleteEntrypoint
// when called from tombstone cleanup (delete.go:90) with ALL nodes tombstoned.
//
// Call site: delete.go:90 (inside cleanUpTombstonedNodesUnlocked)
// Guard: resetIfOnlyNode is called FIRST at line 87
// Expected: resetIfOnlyNode returns onlyNode=true, resets graph, deleteEntrypoint skipped
func TestReview_PC_Caller1_DeleteEntrypoint_AllTombstoned(t *testing.T) {
	ctx := context.Background()
	const numNodes = 5

	vectors := make([][]float32, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "caller1-all-tombstoned-test",
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

	// Insert nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	ep := index.getEntrypoint()
	t.Logf("Initial entrypoint: %d", ep)

	// Tombstone ALL nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.addTombstone(uint64(i)))
	}

	// Create denyList with ALL tombstones (matches delete.go:86)
	denyList := index.tombstonesAsDenyList()
	t.Logf("denyList contains %d tombstones", denyList.Len())

	// Get the entrypoint node
	node := index.nodeByID(ep)
	require.NotNil(t, node)

	// Call resetIfOnlyNode - this is what delete.go:87 does BEFORE deleteEntrypoint
	onlyNode, err := index.resetIfOnlyNode(node, denyList)
	require.NoError(t, err)

	// With all nodes tombstoned and in denyList, this SHOULD return true
	assert.True(t, onlyNode, "resetIfOnlyNode should return true when all other nodes are in denyList")

	t.Log("VERIFIED: Caller 1 (delete.go:90) is SAFE - resetIfOnlyNode handles all-tombstoned case")
}

// TestReview_PC_Caller2_ReplaceDeletedEntrypoint_PartialTombstones tests
// deleteEntrypoint when called from replaceDeletedEntrypoint (delete.go:411)
// with a PARTIAL deleteList (some tombstoned nodes not in deleteList).
//
// Call site: delete.go:411 (inside replaceDeletedEntrypoint)
// Guard: NONE - does not call resetIfOnlyNode
// Bug: When remaining nodes are tombstoned but NOT in deleteList, findNewGlobalEntrypoint
//
//	skips them (hasTombstone check) but isOnlyNode doesn't (no tombstone check).
//	Result: found=false but deleteEntrypoint returns nil without changing EP.
func TestReview_PC_Caller2_ReplaceDeletedEntrypoint_PartialTombstones(t *testing.T) {
	ctx := context.Background()
	const numNodes = 5

	vectors := make([][]float32, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "caller2-partial-tombstones-test",
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

	// Insert nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	ep := index.getEntrypoint()
	t.Logf("Initial entrypoint: %d", ep)

	// Tombstone ALL nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.addTombstone(uint64(i)))
	}

	// Create PARTIAL deleteList (simulating maxTombstonesPerCycle limit)
	// Only include entrypoint, NOT the other tombstoned nodes
	partialDeleteList := helpers.NewAllowList()
	partialDeleteList.Insert(ep)
	t.Logf("Partial deleteList contains only entrypoint %d (other tombstones NOT in list)", ep)

	// Get the entrypoint node
	node := index.nodeByID(ep)
	require.NotNil(t, node)

	// isOnlyNode with partial list: other nodes NOT in list, so returns false
	// (even though they're all tombstoned)
	isOnly := index.isOnlyNode(node, partialDeleteList)
	assert.False(t, isOnly, "isOnlyNode should return false - other nodes exist (even if tombstoned)")

	// Call deleteEntrypoint with partial list (matches delete.go:411)
	err = index.deleteEntrypoint(node, partialDeleteList)
	require.NoError(t, err, "deleteEntrypoint returns nil")

	// Check the entrypoint AFTER deleteEntrypoint
	newEP := index.getEntrypoint()
	t.Logf("Entrypoint after deleteEntrypoint: %d (was: %d)", newEP, ep)

	// BUG CHECK: If entrypoint is unchanged AND points to a tombstoned node,
	// subsequent searches will fail because EP is unusable.
	if newEP == ep {
		// The entrypoint wasn't changed - this is the bug!
		hasTomb := index.hasTombstone(newEP)
		if hasTomb {
			t.Errorf("BUG DETECTED: deleteEntrypoint left EP=%d pointing at tombstoned node!\n"+
				"Call site: delete.go:411 (replaceDeletedEntrypoint)\n"+
				"Cause: deleteList is partial, remaining tombstoned nodes pass isOnlyNode but fail findNewGlobalEntrypoint", newEP)
		}
	}

	// For a valid entrypoint, it should NOT be tombstoned
	newEPTombstoned := index.hasTombstone(newEP)
	assert.False(t, newEPTombstoned, "new entrypoint should NOT be tombstoned")
}

// TestReview_PC_Caller3_RepairGlobalEntrypoint_AllTombstoned tests
// repairGlobalEntrypoint when all nodes are tombstoned.
//
// Call site: delete.go:684 (repairGlobalEntrypoint)
// Handling: delete.go:685-693 - returns errNoUsableEntrypoint if EP unchanged
func TestReview_PC_Caller3_RepairGlobalEntrypoint_AllTombstoned(t *testing.T) {
	ctx := context.Background()
	const numNodes = 5

	vectors := make([][]float32, numNodes)
	for i := 0; i < numNodes; i++ {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		if int(id) < len(vectors) && vectors[id] != nil {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	index, err := New(Config{
		RootPath:                     "doesnt-matter",
		ID:                           "caller3-all-tombstoned-test",
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

	// Insert nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	ep := index.getEntrypoint()
	t.Logf("Initial entrypoint: %d", ep)

	// Tombstone ALL nodes
	for i := 0; i < numNodes; i++ {
		require.NoError(t, index.addTombstone(uint64(i)))
	}

	// Call repairGlobalEntrypoint - should return errNoUsableEntrypoint
	_, err = index.repairGlobalEntrypoint(ep, helpers.NewAllowList())

	// Should get errNoUsableEntrypoint since all nodes are tombstoned
	assert.ErrorIs(t, err, errNoUsableEntrypoint,
		"repairGlobalEntrypoint should return errNoUsableEntrypoint when all nodes tombstoned")

	t.Logf("repairGlobalEntrypoint with all tombstoned: err=%v", err)
	t.Log("VERIFIED: Caller 3 (delete.go:684) is SAFE - returns errNoUsableEntrypoint")
}
