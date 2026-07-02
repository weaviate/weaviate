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

func makeCountingCommitLogger(counter *countingCommitLogger) func() (CommitLogger, error) {
	return func() (CommitLogger, error) {
		return counter, nil
	}
}

// TestEntrypointRepair_GlobalEntrypointUnderMaintenance tests the primary scenario:
// when the global entrypoint is marked under maintenance, an insert should trigger
// repair and succeed (not hang or timeout).
func TestEntrypointRepair_GlobalEntrypointUnderMaintenance(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	// Build index with several nodes
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-repair-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return vectors[0], nil // fallback for new inserts
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
	defer index.Drop(ctx, false)

	// Insert initial nodes to establish the index
	for i := 0; i < len(vectors); i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Get the current entrypoint
	originalEntrypoint := index.entryPointID
	require.True(t, originalEntrypoint < uint64(len(vectors)), "entrypoint should be one of the initial nodes")

	// Mark the global entrypoint as under maintenance
	entrypointNode := index.nodeByID(originalEntrypoint)
	require.NotNil(t, entrypointNode, "entrypoint node should exist")
	entrypointNode.markAsMaintenance()

	// Perform an insert with a short timeout - should NOT hang
	insertDone := make(chan error, 1)
	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))

	go func() {
		insertDone <- index.Add(ctx, newID, newVector)
	}()

	select {
	case err := <-insertDone:
		// Insert terminated - check result
		// On unfixed code, this may be an error or the test may hang
		// On fixed code, this should succeed
		if err != nil {
			t.Fatalf("insert failed with error: %v (expected success after repair)", err)
		}

		// Verify the entrypoint was repaired to a valid node (not under maintenance)
		newEntrypoint := index.entryPointID
		if newEntrypoint == originalEntrypoint {
			// Either repair happened and selected a different node, or repair didn't happen
			// In either case, the original entrypoint is still under maintenance
			newEntrypointNode := index.nodeByID(newEntrypoint)
			if newEntrypointNode != nil && newEntrypointNode.isUnderMaintenance() {
				t.Errorf("entrypoint is still under maintenance after insert - repair did not happen")
			}
		}

		// Verify the new node was inserted
		insertedNode := index.nodeByID(newID)
		assert.NotNil(t, insertedNode, "inserted node should exist in the index")

	case <-time.After(5 * time.Second):
		t.Fatal("insert did not terminate — spinning in pickEntrypoint")
	}

	// Cleanup: unmark maintenance so Drop can proceed cleanly
	entrypointNode.unmarkAsMaintenance()
}

// TestEntrypointRepair_RepairSelectsAlsoUnusableNode tests Case B:
// All nodes are under maintenance → graph is effectively empty →
// insert routes to first-node path, new node becomes entrypoint.
func TestEntrypointRepair_RepairSelectsAlsoUnusableNode(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-repair-also-unusable-test",
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
	defer index.Drop(ctx, false)

	// Insert initial nodes
	for i := 0; i < len(vectors); i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	t.Logf("entrypoint before %d", index.getEntrypoint())

	// Mark ALL nodes as under maintenance
	for i := 0; i < len(vectors); i++ {
		node := index.nodeByID(uint64(i))
		if node != nil {
			node.markAsMaintenance()
		}
	}

	// Graph is effectively empty (all nodes under maintenance = no usable nodes)
	assert.True(t, index.isEffectivelyEmpty(), "graph should be effectively empty when all nodes are under maintenance")

	// Perform an insert - should succeed via first-node path
	newVector := []float32{0.5, 0.5, 0.5}
	newID := uint64(len(vectors))

	err = index.Add(ctx, newID, newVector)
	require.NoError(t, err, "insert should succeed via first-node path")

	// New node should be the entrypoint
	newEP := index.getEntrypoint()
	t.Logf("entrypoint after %d", newEP)
	assert.Equal(t, newID, newEP, "new node should become entrypoint")

	// Cleanup
	for i := 0; i < len(vectors); i++ {
		node := index.nodeByID(uint64(i))
		if node != nil {
			node.unmarkAsMaintenance()
		}
	}
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
	defer index.Drop(ctx, false)

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
			assert.Greater(t, newNode.level, prevMaxLayer,
				"new node may only become entrypoint if it was promoted to a higher layer")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("insert did not terminate — spinning in pickEntrypoint")
	}

	// Cleanup
	originalNode.unmarkAsMaintenance()
}

// TestEntrypointRepair_ConcurrentInserts tests the concurrency scenario:
// several inserts hit the bad entrypoint at once, only one repair should perform
// the actual update (CAS semantics), and the entrypoint should end on a single
// consistent valid value.
func TestEntrypointRepair_ConcurrentInserts(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	// Use a counting commit logger to verify exactly one repair persists
	commitLogger := &countingCommitLogger{}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-repair-concurrent-inserts-test",
		MakeCommitLoggerThunk: makeCountingCommitLogger(commitLogger),
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors)+10 { // allow for new inserts
				if int(id) < len(vectors) {
					return vectors[id], nil
				}
				return []float32{0.5, 0.5, 0.5}, nil
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
	defer index.Drop(ctx, false)

	// Insert initial nodes
	for i := 0; i < len(vectors); i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Record baseline SetEntryPointWithMaxLayer calls (from initial inserts that promoted entrypoint)
	baselineCalls := commitLogger.setEntrypointCalls.Load()

	originalEntrypoint := index.entryPointID

	// Mark the entrypoint under maintenance
	entrypointNode := index.nodeByID(originalEntrypoint)
	require.NotNil(t, entrypointNode)
	entrypointNode.markAsMaintenance()

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

		// The entrypoint should be consistent and valid (not under maintenance)
		finalEntrypoint := index.entryPointID
		finalNode := index.nodeByID(finalEntrypoint)
		require.NotNil(t, finalNode, "final entrypoint node should exist")
		assert.False(t, finalNode.isUnderMaintenance(),
			"final entrypoint should not be under maintenance")

		// one CAS repair persist, plus one per node that may have promoted
		// itself (repair can lower currentMaximumLayer, so any level > 0 counts)
		promotionCandidates := int32(0)
		for i := 0; i < numConcurrentInserts; i++ {
			node := index.nodeByID(uint64(len(vectors) + i))
			require.NotNil(t, node, "inserted node should exist in the index")
			if node.level > 0 {
				promotionCandidates++
			}
		}
		repairCalls := commitLogger.setEntrypointCalls.Load() - baselineCalls
		assert.GreaterOrEqual(t, repairCalls, int32(1),
			"the broken entrypoint should have been repaired and persisted")
		assert.LessOrEqual(t, repairCalls, 1+promotionCandidates,
			"SetEntryPointWithMaxLayer calls should be explained by one CAS repair plus level promotions")

		// Verify entrypoint changed from original
		assert.NotEqual(t, originalEntrypoint, finalEntrypoint,
			"entrypoint should have been repaired to a different node")

	case <-time.After(10 * time.Second):
		t.Fatal("concurrent inserts did not terminate — spinning in pickEntrypoint")
	}

	// Cleanup
	entrypointNode.unmarkAsMaintenance()
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

// TestEntrypointRepair_ConcurrentInsertsIntoAllTombstoned tests that concurrent
// inserts into an all-tombstoned graph produce ONE connected graph, not N
// disconnected nodes. This verifies that the first-node path has proper
// concurrency guards.
func TestEntrypointRepair_ConcurrentInsertsIntoAllTombstoned(t *testing.T) {
	ctx := context.Background()
	const numInitialNodes = 5
	const numConcurrentInserts = 50 // Use more to increase race probability

	// Pre-create vectors for all nodes (initial + new inserts)
	vectors := make([][]float32, numInitialNodes+numConcurrentInserts)
	for i := range vectors {
		vectors[i] = []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3}
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "concurrent-all-tombstoned-test",
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
	defer index.Drop(ctx, false)

	// Insert initial nodes
	for i := 0; i < numInitialNodes; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	// Tombstone ALL nodes - graph becomes "effectively empty"
	for i := 0; i < numInitialNodes; i++ {
		index.addTombstone(uint64(i))
	}

	// Verify graph is effectively empty
	assert.True(t, index.isEffectivelyEmpty(), "graph should be effectively empty after all tombstoned")

	// Launch concurrent inserts with a start barrier
	var wg sync.WaitGroup
	var successCount atomic.Int32
	startBarrier := make(chan struct{})

	for i := 0; i < numConcurrentInserts; i++ {
		wg.Add(1)
		go func(insertNum int) {
			defer wg.Done()
			<-startBarrier // Wait for start signal

			newID := uint64(numInitialNodes + insertNum)
			err := index.Add(ctx, newID, vectors[newID])
			if err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	// Release all goroutines simultaneously
	close(startBarrier)
	wg.Wait()

	t.Logf("Concurrent inserts completed: %d successes", successCount.Load())

	// All inserts should succeed
	assert.Equal(t, int32(numConcurrentInserts), successCount.Load(),
		"all concurrent inserts should succeed")

	// Verify ONE entrypoint exists
	finalEntrypoint := index.getEntrypoint()
	entrypointNode := index.nodeByID(finalEntrypoint)
	require.NotNil(t, entrypointNode, "entrypoint node should exist")
	assert.False(t, index.hasTombstone(finalEntrypoint), "entrypoint should not be tombstoned")
	t.Logf("Final entrypoint: %d", finalEntrypoint)

	// Verify all new nodes are connected (reachable from entrypoint)
	// A disconnected node would have empty connections AND not be the entrypoint
	disconnectedCount := 0
	for i := 0; i < numConcurrentInserts; i++ {
		nodeID := uint64(numInitialNodes + i)
		node := index.nodeByID(nodeID)
		require.NotNil(t, node, "inserted node %d should exist", nodeID)

		conns := node.connections.GetLayer(0)

		// A node is disconnected if: it's not the entrypoint AND has no connections
		if nodeID != finalEntrypoint && len(conns) == 0 {
			disconnectedCount++
			t.Logf("Node %d is DISCONNECTED (no connections, not entrypoint)", nodeID)
		}
	}

	// Assert NO disconnected nodes - this proves the first-node path has proper guards
	assert.Equal(t, 0, disconnectedCount,
		"all inserted nodes should be connected (no disconnected nodes from concurrent insertInitialElement calls)")
}
