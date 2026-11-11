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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// countingCommitLogger wraps NoopCommitLogger and counts SetEntryPointWithMaxLayer calls
type countingCommitLogger struct {
	NoopCommitLogger
	setEntrypointCalls atomic.Int32
}

func (c *countingCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	c.setEntrypointCalls.Add(1)
	return nil
}

func makeCountingCommitLogger(counter *countingCommitLogger) func(opts ...CommitlogOption) (CommitLogger, error) {
	return func(opts ...CommitlogOption) (CommitLogger, error) {
		return counter, nil
	}
}

func newEntrypointRepairIndex(t *testing.T, commitLogger MakeCommitLogger, gone *atomic.Int64) (*hnsw, [][]float32) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-repair-test",
		MakeCommitLoggerThunk: commitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int64(id) == gone.Load() {
				return nil, storobj.NewErrNotFoundf(id, "deleted from object store")
			}
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return []float32{0.5, 0.5, 0.5}, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Drop(ctx) })
	// all on level 0, so the descent cannot hop past the broken entrypoint
	index.randFunc = func() float64 { return 0.5 }

	for i := 0; i < len(vectors); i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}
	return index, vectors
}

// TestEntrypointRepair_GlobalEntrypointUnderMaintenance: an entrypoint under
// maintenance is used as it is, nothing is repaired or persisted.
func TestEntrypointRepair_GlobalEntrypointUnderMaintenance(t *testing.T) {
	ctx := context.Background()
	gone := &atomic.Int64{}
	gone.Store(-1)
	commitLogger := &countingCommitLogger{}
	index, vectors := newEntrypointRepairIndex(t, makeCountingCommitLogger(commitLogger), gone)

	originalEntrypoint := index.entryPointID
	entrypointNode := index.nodeByID(originalEntrypoint)
	require.NotNil(t, entrypointNode)
	entrypointNode.markAsMaintenance()
	defer entrypointNode.unmarkAsMaintenance()
	persisted := commitLogger.setEntrypointCalls.Load()

	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))
	require.NoError(t, index.Add(ctx, newID, newVector))

	assert.Equal(t, originalEntrypoint, index.getEntrypoint(), "an entrypoint under maintenance is not replaced")
	assert.Equal(t, persisted, commitLogger.setEntrypointCalls.Load(), "nothing to persist")
	ids, _, err := index.SearchByVector(ctx, newVector, len(vectors)+1, nil)
	require.NoError(t, err)
	assert.Contains(t, ids, newID)
}

// TestEntrypointRepair_DeadEntrypoint: an entrypoint whose object is gone is
// repaired to a live node once.
func TestEntrypointRepair_DeadEntrypoint(t *testing.T) {
	ctx := context.Background()
	gone := &atomic.Int64{}
	gone.Store(-1)
	commitLogger := &countingCommitLogger{}
	index, vectors := newEntrypointRepairIndex(t, makeCountingCommitLogger(commitLogger), gone)

	originalEntrypoint := index.entryPointID
	gone.Store(int64(originalEntrypoint))
	index.cache.Delete(ctx, originalEntrypoint)
	persisted := commitLogger.setEntrypointCalls.Load()

	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))
	require.NoError(t, index.Add(ctx, newID, newVector))

	repaired := index.getEntrypoint()
	assert.NotEqual(t, originalEntrypoint, repaired)
	assert.True(t, index.hasTombstone(originalEntrypoint), "the dead entrypoint is tombstoned")
	assert.False(t, index.hasTombstone(repaired))
	assert.Equal(t, persisted+1, commitLogger.setEntrypointCalls.Load(), "one repair persisted")
	ids, _, err := index.SearchByVector(ctx, newVector, len(vectors)+1, nil)
	require.NoError(t, err)
	assert.Contains(t, ids, newID)
	assert.NotContains(t, ids, originalEntrypoint)
}

// TestEntrypointRepair_AllNodesUnderMaintenance: an insert proceeds while
// every node is under maintenance.
func TestEntrypointRepair_AllNodesUnderMaintenance(t *testing.T) {
	ctx := context.Background()
	gone := &atomic.Int64{}
	gone.Store(-1)
	index, vectors := newEntrypointRepairIndex(t, MakeNoopCommitLogger, gone)

	for i := 0; i < len(vectors); i++ {
		node := index.nodeByID(uint64(i))
		require.NotNil(t, node)
		node.markAsMaintenance()
	}
	defer func() {
		for i := 0; i < len(vectors); i++ {
			index.nodeByID(uint64(i)).unmarkAsMaintenance()
		}
	}()

	insertDone := make(chan error, 1)
	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))
	go func() {
		insertDone <- index.Add(ctx, newID, newVector)
	}()

	select {
	case err := <-insertDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("insert did not terminate while every node was under maintenance")
	}
	ids, _, err := index.SearchByVector(ctx, newVector, len(vectors)+1, nil)
	require.NoError(t, err)
	assert.Contains(t, ids, newID)
}

// TestEntrypointRepair_ConcurrentRepairAlreadyChanged tests Case C:
// concurrent repair already changed the entrypoint → the repaired value is used.
func TestEntrypointRepair_ConcurrentRepairAlreadyChanged(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-repair-concurrent-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return vectors[0], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx)

	// Insert initial nodes
	for i := 0; i < len(vectors); i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	originalEntrypoint := index.entryPointID

	// Mark the original entrypoint under maintenance
	originalNode := index.nodeByID(originalEntrypoint)
	require.NotNil(t, originalNode)
	originalNode.markAsMaintenance()

	// Simulate concurrent repair: manually change the entrypoint to a different valid node
	// before the insert's repair runs
	var newEntrypointID uint64
	for i := uint64(0); i < uint64(len(vectors)); i++ {
		if i != originalEntrypoint {
			node := index.nodeByID(i)
			if node != nil && !node.isUnderMaintenance() {
				newEntrypointID = i
				break
			}
		}
	}
	require.NotEqual(t, originalEntrypoint, newEntrypointID, "should find a different valid node")

	// Manually set the entrypoint (simulating concurrent repair)
	index.Lock()
	index.entryPointID = newEntrypointID
	index.Unlock()

	prevMaxLayer := index.currentMaximumLayer

	// Now perform an insert - should use the already-repaired entrypoint
	insertDone := make(chan error, 1)
	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))

	go func() {
		insertDone <- index.Add(ctx, newID, newVector)
	}()

	select {
	case err := <-insertDone:
		assert.NoError(t, err, "insert should succeed using the concurrently-repaired entrypoint")

		// EP stays as set by concurrent repair, unless the new node drew a
		// level above the previous max layer and legitimately promoted itself
		finalEntrypoint := index.entryPointID
		assert.NotEqual(t, originalEntrypoint, finalEntrypoint,
			"entrypoint must not revert to the under-maintenance original")
		if finalEntrypoint != newEntrypointID {
			require.Equal(t, newID, finalEntrypoint,
				"entrypoint may only differ from the concurrent repair value via promotion of the new node")
			newNode := index.nodeByID(newID)
			require.NotNil(t, newNode)
			assert.Greater(t, int(newNode.level), prevMaxLayer,
				"new node may only become entrypoint if it was promoted to a higher layer")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("insert did not terminate — spinning in pickEntrypoint")
	}

	// Cleanup
	originalNode.unmarkAsMaintenance()
}

// TestEntrypointRepair_ConcurrentInserts tests the concurrency scenario:
// several inserts hit a dead entrypoint at once, only one repair should perform
// the actual update (CAS semantics), and the entrypoint should end on a single
// consistent valid value.
func TestEntrypointRepair_ConcurrentInserts(t *testing.T) {
	ctx := context.Background()
	gone := &atomic.Int64{}
	gone.Store(-1)
	// Use a counting commit logger to verify exactly one repair persists
	commitLogger := &countingCommitLogger{}
	index, vectors := newEntrypointRepairIndex(t, makeCountingCommitLogger(commitLogger), gone)

	// Record baseline SetEntryPointWithMaxLayer calls (from initial inserts that promoted entrypoint)
	baselineCalls := commitLogger.setEntrypointCalls.Load()

	originalEntrypoint := index.entryPointID
	gone.Store(int64(originalEntrypoint))
	index.cache.Delete(ctx, originalEntrypoint)

	// Launch multiple concurrent inserts with a start barrier to ensure overlap
	const numConcurrentInserts = 10
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	// Start barrier: all goroutines wait here until all are ready
	startBarrier := make(chan struct{})

	for i := 0; i < numConcurrentInserts; i++ {
		wg.Add(1)
		go func(insertNum int) {
			defer wg.Done()

			// Wait for start signal
			<-startBarrier

			newID := uint64(len(vectors) + insertNum)
			newVector := []float32{0.5 + float32(insertNum)*0.01, 0.5, 0.5}

			err := index.Add(ctx, newID, newVector)
			if err != nil {
				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	// Release all goroutines simultaneously
	close(startBarrier)

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All inserts completed
		t.Logf("Concurrent inserts completed: %d successes, %d errors",
			successCount.Load(), errorCount.Load())

		// All inserts should succeed (after repair)
		assert.Equal(t, int32(numConcurrentInserts), successCount.Load(),
			"all concurrent inserts should succeed after repair")

		// The entrypoint should be consistent and valid
		finalEntrypoint := index.entryPointID
		finalNode := index.nodeByID(finalEntrypoint)
		require.NotNil(t, finalNode, "final entrypoint node should exist")
		assert.False(t, index.hasTombstone(finalEntrypoint),
			"final entrypoint should not be tombstoned")

		// one CAS repair persist, plus one per node that may have promoted
		// itself (repair can lower currentMaximumLayer, so any level > 0 counts)
		promotionCandidates := int32(0)
		for i := 0; i < numConcurrentInserts; i++ {
			node := index.nodeByID(uint64(len(vectors) + i))
			require.NotNil(t, node, "inserted node should exist in the index")
			if int(node.level) > 0 {
				promotionCandidates++
			}
		}
		repairCalls := commitLogger.setEntrypointCalls.Load() - baselineCalls
		assert.GreaterOrEqual(t, repairCalls, int32(1),
			"the dead entrypoint should have been repaired and persisted")
		assert.LessOrEqual(t, repairCalls, 1+promotionCandidates,
			"SetEntryPointWithMaxLayer calls should be explained by one CAS repair plus level promotions")

		// Verify entrypoint changed from original
		assert.NotEqual(t, originalEntrypoint, finalEntrypoint,
			"entrypoint should have been repaired to a different node")

	case <-time.After(10 * time.Second):
		t.Fatal("concurrent inserts did not terminate — spinning in pickEntrypoint")
	}
}

// vectorsForEntrypointRepairTest returns a small set of test vectors
func vectorsForEntrypointRepairTest() [][]float32 {
	return [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
		{0.7, 0.8, 0.9},
		{0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7},
	}
}
