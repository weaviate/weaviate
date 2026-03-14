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

package export

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/export"
)

func TestNodeStatus_SnapshotMatchesOriginal(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	ns := &NodeStatus{
		NodeName:    "node-1",
		Status:      export.Transferring,
		Error:       "some error",
		CompletedAt: now,
		ShardProgress: map[string]map[string]*ShardProgress{
			"ClassA": {
				"shard0": {
					Status:          export.ShardTransferring,
					ObjectsExported: 100,
					Error:           "",
					SkipReason:      "",
				},
				"shard1": {
					Status:          export.ShardFailed,
					ObjectsExported: 50,
					Error:           "disk full",
					SkipReason:      "",
				},
			},
			"ClassB": {
				"shard0": {
					Status:          export.ShardSkipped,
					ObjectsExported: 0,
					Error:           "",
					SkipReason:      "tenant is COLD",
				},
			},
		},
	}

	snap := ns.snapshot()

	// JSON output must be identical.
	origJSON, err := json.Marshal(ns)
	require.NoError(t, err)
	snapJSON, err := json.Marshal(snap)
	require.NoError(t, err)
	assert.JSONEq(t, string(origJSON), string(snapJSON))

	// Scalar fields.
	assert.Equal(t, ns.NodeName, snap.NodeName)
	assert.Equal(t, ns.Status, snap.Status)
	assert.Equal(t, ns.Error, snap.Error)
	assert.Equal(t, ns.CompletedAt, snap.CompletedAt)

	// ShardProgress structure matches.
	require.Equal(t, len(ns.ShardProgress), len(snap.ShardProgress))
	for className, shards := range ns.ShardProgress {
		require.Contains(t, snap.ShardProgress, className)
		require.Equal(t, len(shards), len(snap.ShardProgress[className]))
		for shardName, sp := range shards {
			require.Contains(t, snap.ShardProgress[className], shardName)
			snapSP := snap.ShardProgress[className][shardName]
			assert.Equal(t, sp.Status, snapSP.Status)
			assert.Equal(t, sp.ObjectsExported, snapSP.ObjectsExported)
			assert.Equal(t, sp.Error, snapSP.Error)
			assert.Equal(t, sp.SkipReason, snapSP.SkipReason)
		}
	}
}

func TestNodeStatus_SnapshotIsDeepCopy(t *testing.T) {
	ns := &NodeStatus{
		NodeName: "node-1",
		Status:   export.Transferring,
		ShardProgress: map[string]map[string]*ShardProgress{
			"ClassA": {
				"shard0": {
					Status:          export.ShardTransferring,
					ObjectsExported: 100,
				},
			},
		},
	}

	snap := ns.snapshot()

	// Mutate the original after snapshot.
	ns.ShardProgress["ClassA"]["shard0"].ObjectsExported = 999
	ns.ShardProgress["ClassA"]["shard0"].Status = export.ShardFailed
	ns.Status = export.Failed
	ns.Error = "mutated"

	// Snapshot must be unaffected.
	assert.Equal(t, export.Transferring, snap.Status)
	assert.Equal(t, "", snap.Error)
	assert.Equal(t, int64(100), snap.ShardProgress["ClassA"]["shard0"].ObjectsExported)
	assert.Equal(t, export.ShardTransferring, snap.ShardProgress["ClassA"]["shard0"].Status)
}

func TestNodeStatus_SnapshotEmptyProgress(t *testing.T) {
	ns := &NodeStatus{
		NodeName: "node-1",
		Status:   export.Success,
	}

	snap := ns.snapshot()

	assert.Equal(t, ns.NodeName, snap.NodeName)
	assert.Equal(t, ns.Status, snap.Status)
	assert.Nil(t, snap.ShardProgress)
}

func TestSyncLiveCounts(t *testing.T) {
	t.Parallel()

	ns := &NodeStatus{
		NodeName:      "node1",
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
	}

	ns.SetShardProgress("Article", "shard0", export.ShardTransferring, 0, "", "")

	// Simulate workers incrementing the atomic counter.
	ns.AddShardExported("Article", "shard0", 100)
	ns.AddShardExported("Article", "shard0", 50)

	// SyncAndSnapshot should copy atomics and return a consistent snapshot.
	snap := ns.SyncAndSnapshot()
	assert.Equal(t, int64(150), snap.ShardProgress["Article"]["shard0"].ObjectsExported)
}

func TestAddShardExported_Concurrent(t *testing.T) {
	t.Parallel()

	ns := &NodeStatus{
		NodeName:      "node1",
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
	}

	ns.SetShardProgress("Article", "shard0", export.ShardTransferring, 0, "", "")

	const goroutines = 10
	const incrementsPerGoroutine = 1000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range incrementsPerGoroutine {
				ns.AddShardExported("Article", "shard0", 1)
			}
		}()
	}
	wg.Wait()

	ns.mu.Lock()
	sp := ns.ShardProgress["Article"]["shard0"]
	ns.mu.Unlock()

	require.Equal(t, int64(goroutines*incrementsPerGoroutine), sp.objectsWritten.Load())
}

func TestAddShardExported_MissingShard(t *testing.T) {
	t.Parallel()

	ns := &NodeStatus{
		NodeName:      "node1",
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
	}

	// Should not panic when shard doesn't exist.
	ns.AddShardExported("NonExistent", "shard0", 100)
}
