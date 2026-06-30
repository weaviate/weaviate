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

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// =============================================================================
// Mock types for testing
// =============================================================================

// searchFallbackMockCompressorDistancer returns ErrNotFound for deleted nodes.
type searchFallbackMockCompressorDistancer struct {
	deleted  map[uint64]bool
	vectors  [][]float32
	queryVec []float32
}

func (m *searchFallbackMockCompressorDistancer) DistanceToNode(id uint64) (float32, error) {
	if m.deleted[id] {
		return 0, storobj.NewErrNotFoundf(id, "deleted")
	}
	if int(id) >= len(m.vectors) || m.vectors[id] == nil {
		return 0, storobj.NewErrNotFoundf(id, "nil vector")
	}
	vec := m.vectors[id]
	var dist float32
	for i := range vec {
		d := vec[i] - m.queryVec[i]
		dist += d * d
	}
	return dist, nil
}

func (m *searchFallbackMockCompressorDistancer) DistanceToFloat(vec []float32) (float32, error) {
	var dist float32
	for i := range vec {
		d := vec[i] - m.queryVec[i]
		dist += d * d
	}
	return dist, nil
}

// searchFallbackMockVectorCompressor implements VectorCompressor for testing.
type searchFallbackMockVectorCompressor struct {
	deleted map[uint64]bool
	vectors [][]float32
}

func (m *searchFallbackMockVectorCompressor) NewDistancer(q []float32) (compressionhelpers.CompressorDistancer, compressionhelpers.ReturnDistancerFn) {
	return &searchFallbackMockCompressorDistancer{deleted: m.deleted, vectors: m.vectors, queryVec: q}, func() {}
}

func (m *searchFallbackMockVectorCompressor) Drop() error                            { return nil }
func (m *searchFallbackMockVectorCompressor) GrowCache(uint64)                       {}
func (m *searchFallbackMockVectorCompressor) SetCacheMaxSize(int64)                  {}
func (m *searchFallbackMockVectorCompressor) GetCacheMaxSize() int64                 { return 0 }
func (m *searchFallbackMockVectorCompressor) Delete(context.Context, uint64)         {}
func (m *searchFallbackMockVectorCompressor) Preload(uint64, []float32)              {}
func (m *searchFallbackMockVectorCompressor) PreloadMulti(uint64, []uint64, [][]float32) {
}
func (m *searchFallbackMockVectorCompressor) PreloadPassage(uint64, uint64, uint64, []float32) {
}
func (m *searchFallbackMockVectorCompressor) GetKeys(uint64) (uint64, uint64)    { return 0, 0 }
func (m *searchFallbackMockVectorCompressor) SetKeys(uint64, uint64, uint64)     {}
func (m *searchFallbackMockVectorCompressor) Prefetch(uint64)                    {}
func (m *searchFallbackMockVectorCompressor) CountVectors() int64                { return int64(len(m.vectors)) }
func (m *searchFallbackMockVectorCompressor) MaxVectorID() uint64                { return uint64(len(m.vectors)) }
func (m *searchFallbackMockVectorCompressor) Len() int32                         { return int32(len(m.vectors)) }
func (m *searchFallbackMockVectorCompressor) PrefillCache(context.Context)       {}
func (m *searchFallbackMockVectorCompressor) PrefillMultiCache(context.Context, map[uint64][]uint64) {
}
func (m *searchFallbackMockVectorCompressor) DistanceBetweenCompressedVectorsFromIDs(context.Context, uint64, uint64) (float32, error) {
	return 0, nil
}
func (m *searchFallbackMockVectorCompressor) NewDistancerFromID(uint64) (compressionhelpers.CompressorDistancer, error) {
	return nil, nil
}
func (m *searchFallbackMockVectorCompressor) NewBag() compressionhelpers.CompressionDistanceBag {
	return nil
}
func (m *searchFallbackMockVectorCompressor) PersistCompression(compressionhelpers.CommitLogger) {}
func (m *searchFallbackMockVectorCompressor) Stats() compressionhelpers.CompressionStats {
	return nil
}
func (m *searchFallbackMockVectorCompressor) Get(id uint64) ([]float32, error) {
	if int(id) < len(m.vectors) {
		return m.vectors[id], nil
	}
	return nil, storobj.NewErrNotFoundf(id, "not found")
}
func (m *searchFallbackMockVectorCompressor) GetCompressed(id uint64) (any, error) { return nil, nil }

// searchFallbackCountingCommitLogger counts SetEntryPointWithMaxLayer calls for single-flight verification
type searchFallbackCountingCommitLogger struct {
	NoopCommitLogger
	setEntrypointCalls atomic.Int32
	mu                 sync.Mutex
	entrypointHistory  []uint64
}

func (c *searchFallbackCountingCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	c.setEntrypointCalls.Add(1)
	c.mu.Lock()
	c.entrypointHistory = append(c.entrypointHistory, id)
	c.mu.Unlock()
	return nil
}

func makeSearchFallbackCountingCommitLogger(counter *searchFallbackCountingCommitLogger) func() (CommitLogger, error) {
	return func() (CommitLogger, error) {
		return counter, nil
	}
}

// =============================================================================
// Test: Search with invalid entrypoint returns results via fallback
// =============================================================================

// TestSearchFallback_InvalidEntrypoint_SearchByVector tests that SearchByVector
// returns correct results via local fallback when the global entrypoint is deleted.
// This exercises the resolveEntrypoint shared helper.
//
// Without fix: returns error "entrypoint was deleted in the object store..."
// With fix: finds fallback entrypoint, returns valid results, triggers async repair
func TestSearchFallback_InvalidEntrypoint_SearchByVector(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{100, 100}, {1, 1}, {2, 2}}
	deleted := map[uint64]bool{0: true} // node 0 is the deleted entrypoint

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "search-fallback-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if deleted[id] {
				return nil, storobj.NewErrNotFoundf(id, "deleted from store")
			}
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, storobj.NewErrNotFoundf(id, "unknown")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Manually set up graph: node 0 is entrypoint (deleted), nodes 1,2 are valid
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{2}})
	conns2, _ := packedconn.NewWithElements([][]uint64{{1}})
	index.nodes = []*vertex{
		{id: 0, level: 0, connections: conns0},
		{id: 1, level: 0, connections: conns1},
		{id: 2, level: 0, connections: conns2},
	}

	// Search - should NOT fail, should return results via fallback
	results, dists, err := index.SearchByVector(ctx, []float32{1.5, 1.5}, 2, nil)

	require.NoError(t, err, "search should succeed via fallback, not return error")
	require.NotEmpty(t, results, "should return results via fallback")
	assert.Len(t, dists, len(results), "distances should match results")

	// Results should include valid nodes (1 and/or 2), not the deleted node 0
	for _, id := range results {
		assert.NotEqual(t, uint64(0), id, "results should not include deleted entrypoint")
	}

	// Deleted entrypoint should be tombstoned
	assert.True(t, index.hasTombstone(0), "deleted entrypoint should be tombstoned")
}

// TestSearchFallback_InvalidEntrypoint_KnnSearchByVectorMaxDist tests the same
// fallback behavior for KnnSearchByVectorMaxDist.
func TestSearchFallback_InvalidEntrypoint_KnnSearchByVectorMaxDist(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{100, 100}, {1, 1}, {2, 2}}

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "search-fallback-maxdist-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, storobj.NewErrNotFoundf(id, "unknown")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Manually set up graph with deleted entrypoint
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{2}})
	conns2, _ := packedconn.NewWithElements([][]uint64{{1}})
	index.nodes = []*vertex{
		{id: 0, level: 0, connections: conns0},
		{id: 1, level: 0, connections: conns1},
		{id: 2, level: 0, connections: conns2},
	}

	// Enable compressed mode with mock compressor that returns ErrNotFound for node 0
	index.compressed.Store(true)
	index.compressor = &searchFallbackMockVectorCompressor{
		deleted: map[uint64]bool{0: true},
		vectors: vectors,
	}

	// Search - should NOT fail
	results, err := index.KnnSearchByVectorMaxDist(ctx, []float32{1.5, 1.5}, 1000, 100, nil)

	require.NoError(t, err, "KnnSearchByVectorMaxDist should succeed via fallback")
	require.NotEmpty(t, results, "should return results via fallback")

	// Results should not include the deleted node 0
	for _, id := range results {
		assert.NotEqual(t, uint64(0), id, "results should not include deleted entrypoint")
	}

	// Deleted entrypoint should be tombstoned
	assert.True(t, index.hasTombstone(0), "deleted entrypoint should be tombstoned")
}

// =============================================================================
// Test: Empty graph returns empty results (not error)
// =============================================================================

// TestSearchFallback_EmptyGraph_ReturnsEmpty tests that searching an empty graph
// returns empty results, not an error.
func TestSearchFallback_EmptyGraph_ReturnsEmpty(t *testing.T) {
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "empty-graph-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			return nil, storobj.NewErrNotFoundf(id, "empty graph")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Graph is empty - no nodes
	index.nodes = nil

	// SearchByVector on empty graph
	results, dists, err := index.SearchByVector(ctx, []float32{1, 1}, 10, nil)
	require.NoError(t, err, "search on empty graph should not error")
	assert.Empty(t, results, "empty graph should return empty results")
	assert.Empty(t, dists, "empty graph should return empty distances")

	// KnnSearchByVectorMaxDist on empty graph
	results2, err := index.KnnSearchByVectorMaxDist(ctx, []float32{1, 1}, 1000, 100, nil)
	require.NoError(t, err, "KnnSearchByVectorMaxDist on empty graph should not error")
	assert.Empty(t, results2, "empty graph should return empty results")
}

// =============================================================================
// Test: Async repair fires and subsequent search takes fast path
// =============================================================================

// TestSearchFallback_AsyncRepair_SubsequentSearchFastPath tests that:
// 1. First search with invalid entrypoint triggers async repair
// 2. After repair completes, subsequent search uses the repaired entrypoint (fast path)
func TestSearchFallback_AsyncRepair_SubsequentSearchFastPath(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{100, 100}, {1, 1}, {2, 2}, {3, 3}}
	deleted := map[uint64]bool{0: true}

	commitLogger := &searchFallbackCountingCommitLogger{}

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "async-repair-test",
		MakeCommitLoggerThunk: makeSearchFallbackCountingCommitLogger(commitLogger),
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if deleted[id] {
				return nil, storobj.NewErrNotFoundf(id, "deleted from store")
			}
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, storobj.NewErrNotFoundf(id, "unknown")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Set up graph: node 0 is entrypoint (deleted), nodes 1,2,3 are valid
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2, 3}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{2, 3}})
	conns2, _ := packedconn.NewWithElements([][]uint64{{1, 3}})
	conns3, _ := packedconn.NewWithElements([][]uint64{{1, 2}})
	index.nodes = []*vertex{
		{id: 0, level: 0, connections: conns0},
		{id: 1, level: 0, connections: conns1},
		{id: 2, level: 0, connections: conns2},
		{id: 3, level: 0, connections: conns3},
	}

	// First search - should use fallback and trigger async repair
	results, _, err := index.SearchByVector(ctx, []float32{1.5, 1.5}, 2, nil)
	require.NoError(t, err, "first search should succeed via fallback")
	require.NotEmpty(t, results, "first search should return results")

	// Poll with timeout: wait for async repair to complete
	// The repair should change entryPointID from 0 to a valid node (1, 2, or 3)
	repairCompleted := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if index.entryPointID != 0 {
			repairCompleted = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.True(t, repairCompleted, "async repair should complete and change entrypoint from 0")
	assert.NotEqual(t, uint64(0), index.entryPointID, "entrypoint should be repaired to a valid node")

	// Verify commit logger recorded the repair
	assert.GreaterOrEqual(t, commitLogger.setEntrypointCalls.Load(), int32(1),
		"repair should have persisted new entrypoint to commit log")

	// Second search - should use the repaired entrypoint (fast path)
	// Reset the deleted flag for verification - we want to see the fast path works
	// The new entrypoint should be valid
	newEntrypoint := index.entryPointID
	assert.False(t, deleted[newEntrypoint], "repaired entrypoint should not be in deleted set")

	results2, _, err := index.SearchByVector(ctx, []float32{1.5, 1.5}, 2, nil)
	require.NoError(t, err, "second search should succeed via fast path")
	require.NotEmpty(t, results2, "second search should return results")
}

// =============================================================================
// Test: Single-flight - concurrent searches result in exactly ONE repair
// =============================================================================

// TestSearchFallback_SingleFlight_OnlyOneRepair tests that many concurrent searches
// hitting the bad entrypoint result in exactly ONE actual repair/persist.
// No query should block waiting for the repair - all use local fallback immediately.
func TestSearchFallback_SingleFlight_OnlyOneRepair(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{100, 100}, {1, 1}, {2, 2}, {3, 3}, {4, 4}}
	deleted := map[uint64]bool{0: true}

	commitLogger := &searchFallbackCountingCommitLogger{}

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "single-flight-test",
		MakeCommitLoggerThunk: makeSearchFallbackCountingCommitLogger(commitLogger),
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if deleted[id] {
				return nil, storobj.NewErrNotFoundf(id, "deleted from store")
			}
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, storobj.NewErrNotFoundf(id, "unknown")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Set up graph: node 0 is entrypoint (deleted), nodes 1-4 are valid
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2, 3, 4}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{2, 3, 4}})
	conns2, _ := packedconn.NewWithElements([][]uint64{{1, 3, 4}})
	conns3, _ := packedconn.NewWithElements([][]uint64{{1, 2, 4}})
	conns4, _ := packedconn.NewWithElements([][]uint64{{1, 2, 3}})
	index.nodes = []*vertex{
		{id: 0, level: 0, connections: conns0},
		{id: 1, level: 0, connections: conns1},
		{id: 2, level: 0, connections: conns2},
		{id: 3, level: 0, connections: conns3},
		{id: 4, level: 0, connections: conns4},
	}

	// Launch many concurrent searches
	const numSearches = 50
	var wg sync.WaitGroup
	searchErrors := make(chan error, numSearches)
	searchResults := make(chan []uint64, numSearches)

	// Track search completion times to verify no blocking
	searchStartTime := time.Now()

	for i := 0; i < numSearches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, _, err := index.SearchByVector(ctx, []float32{float32(i), float32(i)}, 2, nil)
			if err != nil {
				searchErrors <- err
			} else {
				searchResults <- results
			}
		}()
	}

	// Wait for all searches to complete
	wg.Wait()
	close(searchErrors)
	close(searchResults)

	searchDuration := time.Since(searchStartTime)

	// All searches should succeed
	for err := range searchErrors {
		t.Errorf("search failed: %v", err)
	}

	// All searches should return results
	resultCount := 0
	for results := range searchResults {
		require.NotEmpty(t, results, "each search should return results")
		resultCount++
	}
	assert.Equal(t, numSearches, resultCount, "all searches should return results")

	// Searches should complete quickly (not block waiting for repair)
	// Allow generous timeout but catch obvious blocking
	assert.Less(t, searchDuration, 5*time.Second,
		"searches should complete quickly, not block on repair")

	// Wait for async repair to complete using require.Eventually (avoids flaky time.Sleep)
	require.Eventually(t, func() bool {
		return commitLogger.setEntrypointCalls.Load() > 0 && index.getEntrypoint() != 0
	}, 2*time.Second, 10*time.Millisecond, "async repair should complete")

	// Single-flight: exactly ONE repair should have been persisted
	// (The CAS in repairGlobalEntrypoint ensures only one wins)
	repairCount := commitLogger.setEntrypointCalls.Load()
	assert.Equal(t, int32(1), repairCount,
		"exactly ONE repair should be persisted (single-flight via CAS), got %d", repairCount)

	// Entrypoint should be repaired
	assert.NotEqual(t, uint64(0), index.getEntrypoint(), "entrypoint should be repaired")
}

// =============================================================================
// Test: Async repair excludes tombstoned nodes (Comment 2)
// =============================================================================

// TestSearchFallback_AsyncRepair_ExcludesTombstones tests that the async entrypoint
// repair does NOT select a tombstoned node as the new entrypoint.
//
// Without fix: repair calls repairGlobalEntrypoint with an empty deny-list,
// allowing findNewGlobalEntrypoint to select a tombstoned node.
//
// With fix: repair uses tombstonesAsDenyList() to exclude tombstoned nodes.
func TestSearchFallback_AsyncRepair_ExcludesTombstones(t *testing.T) {
	ctx := context.Background()

	// Create vectors: node 0 (entrypoint) is deleted, nodes 1,2,3 will be tombstoned,
	// node 4 is the only valid non-tombstoned node
	vectors := [][]float32{{100, 100}, {10, 10}, {20, 20}, {30, 30}, {1, 1}}
	deleted := map[uint64]bool{0: true}

	commitLogger := &searchFallbackCountingCommitLogger{}

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "tombstone-exclude-test",
		MakeCommitLoggerThunk: makeSearchFallbackCountingCommitLogger(commitLogger),
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			if deleted[id] {
				return nil, storobj.NewErrNotFoundf(id, "deleted from store")
			}
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, storobj.NewErrNotFoundf(id, "unknown")
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	// Set up graph manually: node 0 is entrypoint, nodes 1-4 are connected
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2, 3, 4}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{2, 3, 4}})
	conns2, _ := packedconn.NewWithElements([][]uint64{{1, 3, 4}})
	conns3, _ := packedconn.NewWithElements([][]uint64{{1, 2, 4}})
	conns4, _ := packedconn.NewWithElements([][]uint64{{1, 2, 3}})
	index.nodes = []*vertex{
		{id: 0, level: 0, connections: conns0},
		{id: 1, level: 0, connections: conns1},
		{id: 2, level: 0, connections: conns2},
		{id: 3, level: 0, connections: conns3},
		{id: 4, level: 0, connections: conns4},
	}

	// Mark nodes 1, 2, 3 as tombstoned - only node 4 should be valid for repair
	index.addTombstone(1)
	index.addTombstone(2)
	index.addTombstone(3)

	// Trigger a search which will cause fallback + async repair
	results, _, err := index.SearchByVector(ctx, []float32{1.5, 1.5}, 2, nil)
	require.NoError(t, err, "search should succeed via fallback")
	require.NotEmpty(t, results, "should return results")

	// Wait for async repair to complete
	require.Eventually(t, func() bool {
		return commitLogger.setEntrypointCalls.Load() > 0 && index.getEntrypoint() != 0
	}, 2*time.Second, 10*time.Millisecond, "async repair should complete")

	// The repaired entrypoint should be node 4 (the only non-tombstoned node)
	newEntrypoint := index.getEntrypoint()
	assert.Equal(t, uint64(4), newEntrypoint,
		"repaired entrypoint should be node 4 (only non-tombstoned node), got %d", newEntrypoint)

	// Verify the repaired entrypoint is NOT tombstoned
	assert.False(t, index.hasTombstone(newEntrypoint),
		"repaired entrypoint should NOT be a tombstoned node")
}
