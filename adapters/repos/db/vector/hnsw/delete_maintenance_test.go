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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// failingCommitLogger wraps NoopCommitLogger but returns an error on ReplaceLinksAtLevel
// to simulate a failure during neighbor reconnection in tombstone cleanup.
type failingCommitLogger struct {
	NoopCommitLogger
	failOnReplaceLinks bool
	mu                 sync.Mutex
}

func (f *failingCommitLogger) ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOnReplaceLinks {
		return errors.New("injected commit log error")
	}
	return nil
}

func (f *failingCommitLogger) setFailOnReplaceLinks(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnReplaceLinks = fail
}

// TestMaintenanceFlagLeakOnReconnectError verifies that a node's maintenance flag
// is cleared even when reconnectNeighboursOf fails during tombstone cleanup.
//
// Bug reproduction: In reassignNeighbor, if reconnectNeighboursOf returns an error
// after markAsMaintenance() is called, unmarkAsMaintenance() is skipped, leaving
// the node permanently marked as under maintenance.
func TestMaintenanceFlagLeakOnReconnectError(t *testing.T) {
	ctx := context.Background()

	// Create vectors for 3 nodes
	vectors := [][]float32{
		{1, 0, 0}, // node 0 - will be tombstoned
		{0, 1, 0}, // node 1 - connected to node 0, will be reassigned
		{0, 0, 1}, // node 2 - alternate connection target
	}

	commitLogger := &failingCommitLogger{}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath: t.TempDir(),
		ID:       "maintenance-flag-leak-test",
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return commitLogger, nil
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
		AllocChecker: memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	// Insert all nodes
	for i, vec := range vectors {
		err := index.Add(ctx, uint64(i), vec)
		require.NoError(t, err)
	}

	// Verify node 1 is not under maintenance before cleanup
	node1 := index.nodeByID(1)
	require.NotNil(t, node1)
	require.False(t, node1.isUnderMaintenance(), "node should not be under maintenance before cleanup")

	// Delete node 0 (creates tombstone)
	err = index.Delete(0)
	require.NoError(t, err)

	// Verify node 1 has connection to node 0 (the tombstoned node)
	// This ensures reassignNeighbor will process node 1
	node1.Lock()
	hasConnectionToTombstoned := false
	for layer := uint8(0); layer < node1.connections.Layers(); layer++ {
		conns := node1.connections.CopyLayer(nil, layer)
		for _, conn := range conns {
			if conn == 0 {
				hasConnectionToTombstoned = true
				break
			}
		}
	}
	node1.Unlock()
	require.True(t, hasConnectionToTombstoned, "node 1 should have connection to tombstoned node 0")

	// Enable failure injection for ReplaceLinksAtLevel
	// This will cause reconnectNeighboursOf to fail after markAsMaintenance is called
	commitLogger.setFailOnReplaceLinks(true)

	// Run tombstone cleanup - this should trigger reassignNeighbor for node 1
	// which will mark it as maintenance and then fail
	deleteList := helpers.NewAllowList()
	deleteList.Insert(0) // node 0 is tombstoned

	// Call reassignNeighbor directly to reproduce the bug
	_, err = index.reassignNeighbor(ctx, 1, deleteList, func() bool { return false }, &sync.Map{})

	// We expect an error from the injected failure
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected commit log error")

	// THE BUG: After the error, node 1 should NOT be under maintenance
	// But without the fix, it remains under maintenance because unmarkAsMaintenance was skipped
	assert.False(t, node1.isUnderMaintenance(),
		"node should NOT be under maintenance after failed cleanup - maintenance flag leaked!")
}

// TestMaintenanceFlagLeakOnInsertError verifies that a node's maintenance flag
// is cleared even when findAndConnectNeighbors fails during insertion.
//
// Bug reproduction: In addOne, if findAndConnectNeighbors returns an error
// after markAsMaintenance() is called and the node is added to h.nodes,
// unmarkAsMaintenance() must still be called.
func TestMaintenanceFlagLeakOnInsertError(t *testing.T) {
	ctx := context.Background()

	// Create vectors - we need at least one existing node for non-first insert path
	vectors := [][]float32{
		{1, 0, 0}, // node 0 - initial node
		{0, 1, 0}, // node 1 - will fail during insert
	}

	commitLogger := &failingCommitLogger{}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath: t.TempDir(),
		ID:       "insert-maintenance-flag-leak-test",
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return commitLogger, nil
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
		AllocChecker: memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	// Insert first node (uses insertInitialElement path, no maintenance flag)
	err = index.Add(ctx, 0, vectors[0])
	require.NoError(t, err)

	// Enable failure injection for ReplaceLinksAtLevel
	// This will cause findAndConnectNeighbors to fail after the node is added to h.nodes
	commitLogger.setFailOnReplaceLinks(true)

	// Try to insert second node - this should fail in findAndConnectNeighbors
	err = index.Add(ctx, 1, vectors[1])
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected commit log error")

	// Verify node 1 was added to h.nodes before the failure
	node1 := index.nodeByID(1)
	require.NotNil(t, node1, "node 1 must be in h.nodes for this test to exercise the post-addition error path")

	// THE BUG: After the error, node 1 should NOT be under maintenance
	// But without the fix, it remains under maintenance because unmarkAsMaintenance was skipped
	assert.False(t, node1.isUnderMaintenance(),
		"node should NOT be under maintenance after failed insert - maintenance flag leaked!")
}
