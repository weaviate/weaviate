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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// ---------------------------------------------------------------------------
// mockBroadcaster — records what was broadcast and returns canned responses.
// ---------------------------------------------------------------------------

type mockBroadcaster struct {
	localNode      string
	createCalled   []string
	deleteCalled   []string
	statusCalled   []string
	remoteStatuses map[string][]replica.AsyncCheckpointNodeStatus
}

func newMockBroadcaster(localNode string) *mockBroadcaster {
	return &mockBroadcaster{
		localNode:      localNode,
		remoteStatuses: make(map[string][]replica.AsyncCheckpointNodeStatus),
	}
}

func (m *mockBroadcaster) LocalNodeName() string { return m.localNode }

func (m *mockBroadcaster) BroadcastCreateAsyncCheckpoint(_ context.Context, shardNames []string, _ int64, _ time.Time) {
	m.createCalled = append(m.createCalled, shardNames...)
}

func (m *mockBroadcaster) BroadcastDeleteAsyncCheckpoint(_ context.Context, shardNames []string) {
	m.deleteCalled = append(m.deleteCalled, shardNames...)
}

func (m *mockBroadcaster) BroadcastGetAsyncCheckpointStatus(_ context.Context, shardNames []string) map[string][]replica.AsyncCheckpointNodeStatus {
	m.statusCalled = append(m.statusCalled, shardNames...)
	out := make(map[string][]replica.AsyncCheckpointNodeStatus)
	for _, name := range shardNames {
		if entries, ok := m.remoteStatuses[name]; ok {
			out[name] = entries
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// helpers for building a minimal Index with controlled shard maps.
// ---------------------------------------------------------------------------

func newTestIndexForCheckpoint(t *testing.T) *Index {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Index{closingCtx: ctx}
}

// storeLoadedTestShard inserts a ShardLike into the Index's shard map as a
// "loaded" shard — i.e. any concrete ShardLike that is not a LazyLoadShard.
func storeLoadedTestShard(idx *Index, name string, shard ShardLike) {
	(*sync.Map)(&idx.shards).Store(name, shard)
}

// storeUnloadedTestShard inserts an unloaded LazyLoadShard into the Index's
// shard map. ForEachLoadedShard skips it; ForEachShard still sees it.
func storeUnloadedTestShard(idx *Index, name string) {
	(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{loaded: false})
}

// ---------------------------------------------------------------------------
// createAsyncCheckpointsImpl
// ---------------------------------------------------------------------------

func TestCreateAsyncCheckpointsImpl(t *testing.T) {
	cutoffMs := time.Now().Add(time.Hour).UnixMilli()
	createdAt := time.UnixMilli(time.Now().UnixMilli())

	t.Run("loaded shard gets local call and broadcast", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().CreateAsyncCheckpoint(cutoffMs, createdAt).Return(nil)

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}

		err := createAsyncCheckpointsImpl(context.Background(), []string{"s1"}, loaded, cutoffMs, createdAt, "MyClass", bc)
		require.NoError(t, err)
		assert.Equal(t, []string{"s1"}, bc.createCalled)
	})

	t.Run("unloaded shard gets only broadcast", func(t *testing.T) {
		bc := newMockBroadcaster("node-a")
		// loaded map is empty: node is not a replica of "s1"
		err := createAsyncCheckpointsImpl(context.Background(), []string{"s1"}, map[string]ShardLike{}, cutoffMs, createdAt, "MyClass", bc)
		require.NoError(t, err)
		assert.Equal(t, []string{"s1"}, bc.createCalled)
	})

	t.Run("loaded and unloaded shards both get broadcast; only loaded gets local call", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().CreateAsyncCheckpoint(cutoffMs, createdAt).Return(nil)

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}

		err := createAsyncCheckpointsImpl(context.Background(), []string{"s1", "s2"}, loaded, cutoffMs, createdAt, "MyClass", bc)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "s2"}, bc.createCalled)
		// s1 had a local call (enforced by the mock expectation); s2 did not.
	})

	t.Run("local error stops fan-out for that shard", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().CreateAsyncCheckpoint(cutoffMs, createdAt).Return(errors.New("checkpoint rejected"))

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}

		err := createAsyncCheckpointsImpl(context.Background(), []string{"s1"}, loaded, cutoffMs, createdAt, "MyClass", bc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "checkpoint rejected")
		// broadcast must NOT have been called after the local failure
		assert.Empty(t, bc.createCalled)
	})
}

// ---------------------------------------------------------------------------
// deleteAsyncCheckpointsImpl
// ---------------------------------------------------------------------------

func TestDeleteAsyncCheckpointsImpl(t *testing.T) {
	t.Run("loaded shard gets local delete and broadcast", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().DeleteAsyncCheckpoint()

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}

		err := deleteAsyncCheckpointsImpl(context.Background(), []string{"s1"}, loaded, bc)
		require.NoError(t, err)
		assert.Equal(t, []string{"s1"}, bc.deleteCalled)
	})

	t.Run("unloaded shard gets only broadcast", func(t *testing.T) {
		bc := newMockBroadcaster("node-a")
		err := deleteAsyncCheckpointsImpl(context.Background(), []string{"s1"}, map[string]ShardLike{}, bc)
		require.NoError(t, err)
		assert.Equal(t, []string{"s1"}, bc.deleteCalled)
	})

	t.Run("loaded and unloaded both get broadcast", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().DeleteAsyncCheckpoint()

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}

		err := deleteAsyncCheckpointsImpl(context.Background(), []string{"s1", "s2"}, loaded, bc)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "s2"}, bc.deleteCalled)
	})
}

// ---------------------------------------------------------------------------
// getAsyncCheckpointStatusImpl
// ---------------------------------------------------------------------------

func TestGetAsyncCheckpointStatusImpl(t *testing.T) {
	root := hashtree.Digest{1, 2}
	cutoffMs := int64(1000)
	createdAt := time.UnixMilli(cutoffMs)

	remoteEntry := replica.AsyncCheckpointNodeStatus{Node: "node-b", CutoffMs: cutoffMs, Root: root}

	t.Run("loaded shard includes local entry plus remote entries", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().AsyncCheckpointRoot().Return(root, cutoffMs, createdAt, true)

		bc := newMockBroadcaster("node-a")
		bc.remoteStatuses["s1"] = []replica.AsyncCheckpointNodeStatus{remoteEntry}

		loaded := map[string]ShardLike{"s1": shard}
		result := getAsyncCheckpointStatusImpl(context.Background(), []string{"s1"}, loaded, bc)

		require.Len(t, result["s1"], 2)
		assert.Equal(t, "node-a", result["s1"][0].Node)
		assert.Equal(t, cutoffMs, result["s1"][0].CutoffMs)
		assert.Equal(t, root, result["s1"][0].Root)
		assert.Equal(t, remoteEntry, result["s1"][1])
	})

	t.Run("unloaded shard has only remote entries in result", func(t *testing.T) {
		bc := newMockBroadcaster("node-a")
		bc.remoteStatuses["s1"] = []replica.AsyncCheckpointNodeStatus{remoteEntry}

		// empty loaded map: this node is not a replica of s1
		result := getAsyncCheckpointStatusImpl(context.Background(), []string{"s1"}, map[string]ShardLike{}, bc)

		require.Len(t, result["s1"], 1)
		assert.Equal(t, remoteEntry, result["s1"][0])
		assert.Contains(t, bc.statusCalled, "s1")
	})

	t.Run("shard with no active checkpoint shows zero cutoff", func(t *testing.T) {
		shard := NewMockShardLike(t)
		// ok=false: no active checkpoint
		shard.EXPECT().AsyncCheckpointRoot().Return(hashtree.Digest{}, 0, time.Time{}, false)

		bc := newMockBroadcaster("node-a")
		loaded := map[string]ShardLike{"s1": shard}
		result := getAsyncCheckpointStatusImpl(context.Background(), []string{"s1"}, loaded, bc)

		require.Len(t, result["s1"], 1)
		assert.Equal(t, int64(0), result["s1"][0].CutoffMs)
	})
}

// ---------------------------------------------------------------------------
// resolveAllShards
// ---------------------------------------------------------------------------

func TestResolveAllShards(t *testing.T) {
	t.Run("empty filter returns all known shards", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)
		storeLoadedTestShard(idx, "s1", NewMockShardLike(t))
		storeUnloadedTestShard(idx, "s2")

		allNames, loaded, err := idx.resolveAllShards(nil)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "s2"}, allNames)
		assert.Contains(t, loaded, "s1")
		assert.NotContains(t, loaded, "s2")
	})

	t.Run("explicit filter with only existing shards", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)
		storeLoadedTestShard(idx, "s1", NewMockShardLike(t))
		storeUnloadedTestShard(idx, "s2")
		storeLoadedTestShard(idx, "s3", NewMockShardLike(t))

		allNames, loaded, err := idx.resolveAllShards([]string{"s1", "s2"})
		require.NoError(t, err)
		// Only the requested shards are targeted.
		assert.ElementsMatch(t, []string{"s1", "s2"}, allNames)
		assert.NotContains(t, allNames, "s3")
		// loaded contains all loaded shards of the index (used for lookup); s2 is
		// absent because it is unloaded; s3 is present because it is loaded but
		// not in the requested filter (allNames drives iteration, not loaded).
		assert.Contains(t, loaded, "s1")
		assert.NotContains(t, loaded, "s2")
	})

	t.Run("non-existent shard name returns error", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)
		storeLoadedTestShard(idx, "s1", NewMockShardLike(t))

		_, _, err := idx.resolveAllShards([]string{"ghost"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ghost")
	})

	t.Run("unloaded shard in explicit filter is not an error", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)
		storeUnloadedTestShard(idx, "s1")

		allNames, loaded, err := idx.resolveAllShards([]string{"s1"})
		require.NoError(t, err)
		assert.Equal(t, []string{"s1"}, allNames)
		assert.Empty(t, loaded) // not a replica → not in loaded map
	})

	t.Run("empty index returns empty results", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)

		allNames, loaded, err := idx.resolveAllShards(nil)
		require.NoError(t, err)
		assert.Empty(t, allNames)
		assert.Empty(t, loaded)
	})

	t.Run("duplicate shard names in filter are deduplicated", func(t *testing.T) {
		idx := newTestIndexForCheckpoint(t)
		storeLoadedTestShard(idx, "s1", NewMockShardLike(t))
		storeLoadedTestShard(idx, "s2", NewMockShardLike(t))

		allNames, _, err := idx.resolveAllShards([]string{"s1", "s2", "s1", "s2"})
		require.NoError(t, err)
		assert.Equal(t, []string{"s1", "s2"}, allNames)
	})
}
