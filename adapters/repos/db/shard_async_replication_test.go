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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// uuidLow and uuidHigh are deterministic UUIDs with clear binary ordering:
// uuidLow starts with 0x00 (lower half of UUID space),
// uuidHigh starts with 0xF0 (upper half of UUID space).
// This guarantees DigestObjectsInRange always returns them in the same order.
const (
	uuidLow  = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	uuidHigh = strfmt.UUID("f0000000-0000-0000-0000-000000000001")
	uuidMid  = strfmt.UUID("70000000-0000-0000-0000-000000000001")
)

// tsFarPast is a timestamp (Unix ms) well in the past (1970-01-01 00:00:01 UTC).
const tsFarPast int64 = 1

// testObj builds a storobj with a known UUID and UpdateTime. The UpdateTime is
// stored as-is by PutObject (not overwritten by the shard layer).
func testObjWithTime(class string, id strfmt.UUID, updateTime int64) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              class,
			LastUpdateTimeUnix: updateTime,
		},
	}
}

// equalTimestampReplicationClient is a test double that echoes every digest
// back with its UpdateTime unchanged, simulating an equal-timestamp conflict
// where the remote holds the same version as the source.
type equalTimestampReplicationClient struct {
	FakeReplicationClient
}

func (c *equalTimestampReplicationClient) CompareDigests(
	_ context.Context, _, _, _ string, digests []routerTypes.RepairResponse,
) ([]routerTypes.RepairResponse, error) {
	echo := make([]routerTypes.RepairResponse, len(digests))
	copy(echo, digests)
	return echo, nil
}

// allStaleReplicationClient is a test double that reports every digest it
// receives as missing on the remote side (UpdateTime == 0).
type allStaleReplicationClient struct {
	FakeReplicationClient
}

func (c *allStaleReplicationClient) CompareDigests(
	_ context.Context, _, _, _ string, digests []routerTypes.RepairResponse,
) ([]routerTypes.RepairResponse, error) {
	stale := make([]routerTypes.RepairResponse, len(digests))
	for i, d := range digests {
		stale[i] = routerTypes.RepairResponse{ID: d.ID, UpdateTime: 0}
	}
	return stale, nil
}

// withReplicationClient returns an indexOpt that replaces the index's
// replicator with a new one backed by the given client. This allows tests to
// control what CompareDigests returns without modifying production code.
func withReplicationClient(t *testing.T, client replica.Client) func(*Index) {
	t.Helper()
	return func(idx *Index) {
		logger, _ := test.NewNullLogger()

		mockRouter := routerTypes.NewMockRouter(t)
		localReplica := routerTypes.Replica{NodeName: "node1", ShardName: "shard1", HostAddr: "127.0.0.1"}
		mockRouter.EXPECT().
			GetWriteReplicasLocation(mock.Anything, mock.Anything, mock.Anything).
			Return(routerTypes.WriteReplicaSet{Replicas: []routerTypes.Replica{localReplica}}, nil).
			Maybe()
		mockRouter.EXPECT().
			GetReadReplicasLocation(mock.Anything, mock.Anything, mock.Anything).
			Return(routerTypes.ReadReplicaSet{Replicas: []routerTypes.Replica{localReplica}}, nil).
			Maybe()

		nodeResolver := cluster.NewMockNodeResolver(t)

		rep, err := replica.NewReplicator(
			idx.Config.ClassName.String(),
			mockRouter,
			nodeResolver,
			"node1",
			func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
			client,
			monitoring.GetMetrics(),
			logger,
		)
		require.NoError(t, err)
		idx.replicator = rep
	}
}

// fullRangeConfig returns an AsyncReplicationConfig that scans the entire UUID
// space (hashtreeHeight=1 → 2 leaves) in batches of batchSize objects.
func fullRangeConfig(batchSize int) AsyncReplicationConfig {
	return AsyncReplicationConfig{
		hashtreeHeight: 1,
		diffBatchSize:  batchSize,
	}
}

// ─── Shard.CompareDigests ────────────────────────────────────────────────────

// withDeletionStrategy returns an indexOpt that sets the deletion strategy on
// the index config, allowing CompareDigests tombstone handling to be tested
// without a full cluster setup.
func withDeletionStrategy(strategy string) func(*Index) {
	return func(idx *Index) {
		idx.Config.DeletionStrategy = strategy
	}
}

// flushShard forces all pending memtable data to disk so that CompareDigests
// (which uses GetErrDeletedOnDisk and is therefore disk-only) can observe the
// objects and tombstones written by PutObject / DeleteObject.
func flushShard(t *testing.T, ctx context.Context, shard ShardLike) {
	t.Helper()
	s, ok := shard.(*Shard)
	require.True(t, ok, "flushShard: expected *Shard, got %T", shard)
	require.NoError(t, s.store.FlushMemtables(ctx))
}

// TestShardCompareDigests validates the server-side CompareDigests logic:
// missing objects are returned with UpdateTime==0, stale objects with the local
// UpdateTime, and up-to-date objects are not returned at all.
func TestShardCompareDigests(t *testing.T) {
	ctx := context.Background()
	const class = "CompareDigestsTest"

	// localTime is the timestamp stored on the shard for the known objects.
	const localTime int64 = 1_000_000

	t.Run("MissingObject", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		// Shard is empty — every source digest is missing.
		source := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: localTime + 1},
		}
		result, err := shard.CompareDigests(ctx, source)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, string(uuidLow), result[0].ID)
		assert.Equal(t, int64(0), result[0].UpdateTime, "missing object must have UpdateTime==0")
	})

	t.Run("StaleLocalObject", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		flushShard(t, ctx, shard)

		// Source claims a newer version → local is stale.
		source := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: localTime + 1},
		}
		result, err := shard.CompareDigests(ctx, source)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, string(uuidLow), result[0].ID)
		assert.Equal(t, localTime, result[0].UpdateTime,
			"stale object must carry local UpdateTime so caller can use it as remoteStaleUpdateTime")
	})

	t.Run("SourceStrictlyOlder", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		flushShard(t, ctx, shard)

		// Source has a strictly older version → local is ahead, not returned.
		source := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: localTime - 1},
		}
		result, err := shard.CompareDigests(ctx, source)
		require.NoError(t, err)
		assert.Empty(t, result, "object where local is strictly newer must not be returned")
	})

	t.Run("EqualTimestampConflict", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		flushShard(t, ctx, shard)

		// Source and local have the same UpdateTime — a potential conflict that the
		// target cannot resolve alone. The target returns the object so the source
		// can apply the node-name tiebreaker.
		source := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: localTime},
		}
		result, err := shard.CompareDigests(ctx, source)
		require.NoError(t, err)
		require.Len(t, result, 1, "equal-timestamp conflict must be returned for source-side tiebreak")
		assert.Equal(t, string(uuidLow), result[0].ID)
		assert.Equal(t, localTime, result[0].UpdateTime)
	})

	// Tombstoned tests verify that CompareDigests applies the configured
	// deletion conflict strategy on the target side for tombstoned objects.
	//
	// deletionTs is the timestamp used for all DeleteObject calls below.
	// sourceNewerTs  > deletionTs  → source live object is newer than the tombstone.
	// sourceSameTs  == deletionTs  → equal timestamp; deletion should win.
	// sourceOlderTs  < deletionTs  → deletion is newer than the source object.
	const (
		deletionTs    int64 = 2_000_000
		sourceNewerTs int64 = 3_000_000
		sourceSameTs  int64 = deletionTs
		sourceOlderTs int64 = 1_000
	)

	t.Run("Tombstoned/NoAutomatedResolution", func(t *testing.T) {
		// Strategy=NoAutomatedResolution: tombstone must be suppressed regardless
		// of timestamps so the source neither propagates nor deletes.
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyNoAutomatedResolution))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.UnixMilli(deletionTs)))
		flushShard(t, ctx, shard)

		for _, sourceTs := range []int64{sourceOlderTs, sourceSameTs, sourceNewerTs} {
			result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
				{ID: string(uuidLow), UpdateTime: sourceTs},
			})
			require.NoError(t, err)
			assert.Empty(t, result, "NoAutomatedResolution: tombstone must not be reported (sourceTs=%d)", sourceTs)
		}
	})

	t.Run("Tombstoned/DeleteOnConflict", func(t *testing.T) {
		// Strategy=DeleteOnConflict: deletion always wins, instruct source to
		// delete regardless of timestamps.
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyDeleteOnConflict))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.UnixMilli(deletionTs)))
		flushShard(t, ctx, shard)

		for _, sourceTs := range []int64{sourceOlderTs, sourceSameTs, sourceNewerTs} {
			result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
				{ID: string(uuidLow), UpdateTime: sourceTs},
			})
			require.NoError(t, err)
			require.Len(t, result, 1, "DeleteOnConflict: tombstone must always be reported (sourceTs=%d)", sourceTs)
			assert.True(t, result[0].Deleted, "DeleteOnConflict: result must have Deleted=true")
			assert.Equal(t, deletionTs, result[0].UpdateTime, "DeleteOnConflict: UpdateTime must be the deletion timestamp")
		}
	})

	t.Run("Tombstoned/TimeBasedResolution_SourceNewer", func(t *testing.T) {
		// Strategy=TimeBasedResolution, source strictly newer than tombstone:
		// return as a normal stale entry (no Deleted flag) so source propagates
		// the live object to restore it on this shard.
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyTimeBasedResolution))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.UnixMilli(deletionTs)))
		flushShard(t, ctx, shard)

		result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: sourceNewerTs},
		})
		require.NoError(t, err)
		require.Len(t, result, 1, "TimeBased/SourceNewer: stale entry must be returned so source propagates")
		assert.False(t, result[0].Deleted, "TimeBased/SourceNewer: Deleted must be false — source should propagate")
		assert.Equal(t, deletionTs, result[0].UpdateTime, "TimeBased/SourceNewer: UpdateTime must be the deletion timestamp")
	})

	t.Run("Tombstoned/TimeBasedResolution_DeletionNewer", func(t *testing.T) {
		// Strategy=TimeBasedResolution, deletion is newer: instruct source to delete.
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyTimeBasedResolution))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.UnixMilli(deletionTs)))
		flushShard(t, ctx, shard)

		result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: sourceOlderTs},
		})
		require.NoError(t, err)
		require.Len(t, result, 1, "TimeBased/DeletionNewer: tombstone must be reported so source deletes")
		assert.True(t, result[0].Deleted, "TimeBased/DeletionNewer: Deleted must be true")
		assert.Equal(t, deletionTs, result[0].UpdateTime)
	})

	t.Run("Tombstoned/TimeBasedResolution_EqualTimestamps", func(t *testing.T) {
		// Strategy=TimeBasedResolution, timestamps are equal: deletion wins
		// (condition is strictly greater-than, so equal falls to the delete path).
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyTimeBasedResolution))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.UnixMilli(deletionTs)))
		flushShard(t, ctx, shard)

		result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: sourceSameTs},
		})
		require.NoError(t, err)
		require.Len(t, result, 1, "TimeBased/EqualTimestamps: deletion wins on tie")
		assert.True(t, result[0].Deleted, "TimeBased/EqualTimestamps: Deleted must be true")
	})

	t.Run("Tombstoned/TimeBasedResolution_UnknownDeletionTime", func(t *testing.T) {
		// Strategy=TimeBasedResolution with an unknown deletion timestamp (zero):
		// cannot determine winner, so conservatively instruct source to delete.
		shard, _ := testShard(t, ctx, class,
			withDeletionStrategy(models.ReplicationConfigDeletionStrategyTimeBasedResolution))
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime)))
		// DeleteObject with zero time stores a tombstone without a timestamp.
		require.NoError(t, shard.DeleteObject(ctx, uuidLow, time.Time{}))
		flushShard(t, ctx, shard)

		result, err := shard.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: sourceNewerTs},
		})
		require.NoError(t, err)
		require.Len(t, result, 1, "TimeBased/UnknownDeletionTime: must conservatively instruct deletion")
		assert.True(t, result[0].Deleted)
		assert.Equal(t, int64(0), result[0].UpdateTime, "UpdateTime must be zero when deletion timestamp is unknown")
	})

	t.Run("MixedBatch", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidLow, localTime))) // stale
		require.NoError(t, shard.PutObject(ctx, testObjWithTime(class, uuidMid, localTime))) // up-to-date
		// uuidHigh is not inserted → missing
		flushShard(t, ctx, shard)

		source := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: localTime + 1}, // source newer → stale local
			{ID: string(uuidMid), UpdateTime: localTime - 1}, // source older → up-to-date local
			{ID: string(uuidHigh), UpdateTime: localTime},    // not on shard → missing
		}
		result, err := shard.CompareDigests(ctx, source)
		require.NoError(t, err)
		require.Len(t, result, 2, "only stale and missing objects should be returned")

		byID := make(map[string]routerTypes.RepairResponse, len(result))
		for _, r := range result {
			byID[r.ID] = r
		}
		stale, ok := byID[string(uuidLow)]
		require.True(t, ok, "stale object must be present")
		assert.Equal(t, localTime, stale.UpdateTime)

		missing, ok := byID[string(uuidHigh)]
		require.True(t, ok, "missing object must be present")
		assert.Equal(t, int64(0), missing.UpdateTime)

		_, ok = byID[string(uuidMid)]
		assert.False(t, ok, "up-to-date object must not be present")
	})

	t.Run("EmptyInput", func(t *testing.T) {
		shard, _ := testShard(t, ctx, class)
		result, err := shard.CompareDigests(ctx, nil)
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = shard.CompareDigests(ctx, []routerTypes.RepairResponse{})
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

// ─── objectsToPropagateWithinRange ───────────────────────────────────────────

// TestObjectsToPropagateWithinRange covers the scanning and filtering logic
// inside objectsToPropagateWithinRange. Tests use well-known UUIDs so that
// batch ordering is deterministic. Objects must be flushed to disk before
// scanning because DigestObjectsInRange uses CursorOnDisk and skips memtables.
func TestObjectsToPropagateWithinRange(t *testing.T) {
	ctx := context.Background()
	const class = "PropagateRangeTest"

	// concreteShard returns the *Shard from a testShard call.
	// testShard passes EnableLazyLoadShards=true as the disableLazyLoad flag,
	// so initShard creates a real *Shard directly (not a LazyLoadShard).
	concreteShard := func(t *testing.T, sl ShardLike) *Shard {
		t.Helper()
		s, ok := sl.(*Shard)
		require.True(t, ok, "expected *Shard from testShard (EnableLazyLoadShards=true disables lazy loading)")
		return s
	}

	t.Run("EmptyShard", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cfg := fullRangeConfig(100)

		local, remote, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, local)
		assert.Equal(t, 0, remote)
		assert.Empty(t, objs)
		_ = idx
	})

	// MemtableObjectsNotPropagated verifies that objects still in the memtable
	// (not yet flushed to disk) are invisible to DigestObjectsInRange and are
	// therefore not propagated in the current hashbeat cycle.
	t.Run("MemtableObjectsNotPropagated", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		cfg := fullRangeConfig(100)

		local, remote, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, local, "memtable objects must not be returned by disk-only scan")
		assert.Equal(t, 0, remote, "CompareDigests must not be called when no on-disk objects found")
		assert.Empty(t, objs)
		_ = idx
	})

	// SourceBehindRemote verifies the no-op path: source objects are all
	// up-to-date on the remote (CompareDigests returns nothing stale), so no
	// objects are queued for propagation even though they were scanned and compared.
	t.Run("SourceBehindRemote", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		local, remote, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 2, local)
		assert.Equal(t, 2, remote, "both objects must have been sent to remote for comparison")
		assert.Empty(t, objs, "remote is up-to-date → nothing to propagate")
		_ = idx
	})

	// LimitEnforcement verifies that the propagation limit caps the number of
	// objects queued, not the number scanned. Uses allStaleReplicationClient so
	// every compared digest is returned as stale (UpdateTime==0, missing).
	t.Run("LimitEnforcement", func(t *testing.T) {
		const limit = 1
		sl, idx := testShard(t, ctx, class)
		// Insert two eligible objects; both will be reported as stale by the client.
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		sl2, idx2 := testShard(t, ctx, class, withReplicationClient(t, &allStaleReplicationClient{}))
		require.NoError(t, sl2.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl2.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl2)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		_, _, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, limit, nil,
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(objs), limit,
			"number of queued propagations must not exceed the limit")
		_ = sl
		_ = idx
		_ = idx2
	})

	// EqualTimestampTiebreaker verifies that when source and target have the same
	// UpdateTime for an object (a conflict), the node with the lexicographically
	// lower name propagates and the other does not.
	//
	// The test shard's replicator reports local node name "node1".
	// "node0" < "node1" < "node2" lexicographically.
	t.Run("EqualTimestampTiebreaker", func(t *testing.T) {
		// equalTimestampClient reports every digest as having the same UpdateTime
		// as the source sent, simulating an equal-timestamp conflict on the remote.
		equalTimestampClient := &equalTimestampReplicationClient{}

		sl, idx := testShard(t, ctx, class, withReplicationClient(t, equalTimestampClient))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		t.Run("LocalWins_LowerName", func(t *testing.T) {
			// local="node1", target="node2": "node1" < "node2" → local wins → propagate.
			_, _, objs, err := s.objectsToPropagateWithinRange(
				ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			)
			require.NoError(t, err)
			assert.Len(t, objs, 1, "local node wins tiebreak → must propagate")
		})

		t.Run("LocalLoses_HigherName", func(t *testing.T) {
			// local="node1", target="node0": "node1" > "node0" → local loses → skip.
			_, _, objs, err := s.objectsToPropagateWithinRange(
				ctx, cfg, "http://fake", "node0", 0, 1, 100, nil,
			)
			require.NoError(t, err)
			assert.Empty(t, objs, "local node loses tiebreak → must not propagate")
		})
		_ = idx
	})
}

// ─── Async Replication Lifecycle ─────────────────────────────────────────────

// simulateInitInProgress sets the shard's async-replication fields to exactly
// the state that initAsyncReplication establishes when a fresh hashtree must be
// built from scratch: hashtree non-nil, not yet fully initialised, channel open,
// Once fresh.  Direct field assignment gives tests deterministic control over
// the "init in progress" state without spawning a real init goroutine or relying
// on any timing.
//
// The returned cancel func is stored as s.asyncReplicationCancelFunc so that
// the disable paths (SetAsyncReplicationState, mayStopAsyncReplication) can
// call it without panicking.  Callers must invoke it after the test completes.
func simulateInitInProgress(t *testing.T, s *Shard) context.CancelFunc {
	t.Helper()
	ht, err := hashtree.NewHashTree(1)
	require.NoError(t, err)
	_, cancel := context.WithCancel(context.Background())
	s.asyncReplicationRWMux.Lock()
	s.hashtree = ht
	s.hashtreeFullyInitialized = false
	s.minimalHashtreeInitializationCh = make(chan struct{})
	s.minimalHashtreeInitializationOnce = &sync.Once{}
	s.asyncReplicationCancelFunc = cancel
	s.asyncReplicationRWMux.Unlock()
	return cancel
}

// TestAsyncReplicationLifecycle validates the correctness of the
// waitForMinimalHashTreeInitialization / channel-close / sync.Once machinery
// introduced to fix the deadlock where goroutines holding RLock while blocking
// on the init channel prevented SetAsyncReplicationState from ever acquiring
// the write lock needed to close that channel.
//
// All subtests are deterministic: they do not sleep or rely on goroutine
// scheduling order.  The 5-second awaitDone guard only fires on real deadlocks;
// in normal runs these operations complete in microseconds.
func TestAsyncReplicationLifecycle(t *testing.T) {
	ctx := context.Background()
	const class = "AsyncReplicationLifecycleTest"

	// concreteShard unwraps the *Shard from testShard (EnableLazyLoadShards=true
	// causes initShard to create a real *Shard directly, not a LazyLoadShard).
	concreteShard := func(t *testing.T, sl ShardLike) *Shard {
		t.Helper()
		s, ok := sl.(*Shard)
		require.True(t, ok, "expected *Shard; EnableLazyLoadShards must be true")
		return s
	}

	// awaitDone waits for done to close, failing the test if the liveness
	// timeout elapses first. Fatalf is called directly so the goroutine count
	// in the failure message points at the right test.
	awaitDone := func(t *testing.T, done <-chan struct{}, msg string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("liveness timeout: %s", msg)
		}
	}

	// ── Fast-path: hashtree nil ───────────────────────────────────────────────
	// Test 4: when async replication has never been enabled the hashtree field
	// is nil; waitForMinimalHashTreeInitialization must return nil immediately
	// without touching minimalHashtreeInitializationCh (which is also nil).
	t.Run("FastPathHashtreeNil", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		require.Nil(t, s.hashtree, "pre-condition: hashtree must be nil")
		require.NoError(t, s.waitForMinimalHashTreeInitialization(ctx))
	})

	// ── Fast-path: already fully initialised ─────────────────────────────────
	// When hashtreeFullyInitialized is true the function must return nil
	// immediately without blocking on the channel.
	t.Run("FastPathFullyInitialized", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		ht, err := hashtree.NewHashTree(1)
		require.NoError(t, err)
		s.asyncReplicationRWMux.Lock()
		s.hashtree = ht
		s.hashtreeFullyInitialized = true
		s.asyncReplicationRWMux.Unlock()
		require.NoError(t, s.waitForMinimalHashTreeInitialization(ctx))
	})

	// ── Test 1: unblocked by normal init completion ───────────────────────────
	// A goroutine blocked in waitForMinimalHashTreeInitialization must be
	// unblocked when releaseInitialization closes the channel.  Closing before
	// the goroutine reaches the select is equally valid (receive on a closed
	// channel returns immediately), so no ordering synchronisation is needed —
	// the test is correct regardless of scheduling.
	t.Run("UnblockedByInitCompletion", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := s.waitForMinimalHashTreeInitialization(ctx); err != nil {
				t.Errorf("expected nil error, got %v", err)
			}
		}()

		// Simulate releaseInitialization (called inside initHashtree once the
		// first scan pass completes).
		s.minimalHashtreeInitializationOnce.Do(func() {
			close(s.minimalHashtreeInitializationCh)
		})
		awaitDone(t, done, "waitForMinimalHashTreeInitialization did not return after channel close")
	})

	// ── Test 6: context cancellation unblocks a waiting goroutine ────────────
	t.Run("ContextCancellation", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		waitCtx, waitCancel := context.WithCancel(ctx)
		result := make(chan error, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			result <- s.waitForMinimalHashTreeInitialization(waitCtx)
		}()

		waitCancel()
		awaitDone(t, done, "waitForMinimalHashTreeInitialization did not return after context cancellation")
		require.ErrorIs(t, <-result, context.Canceled)
	})

	// ── Multiple waiters all unblocked simultaneously ─────────────────────────
	// Closing a channel broadcasts to all receivers at once.  All N goroutines
	// must return nil — not just the first one.
	t.Run("MultipleWaitersUnblockedAtOnce", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		const n = 10
		results := make(chan error, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for range n {
			go func() {
				defer wg.Done()
				results <- s.waitForMinimalHashTreeInitialization(ctx)
			}()
		}

		s.minimalHashtreeInitializationOnce.Do(func() {
			close(s.minimalHashtreeInitializationCh)
		})

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		awaitDone(t, done, "not all goroutines unblocked after channel close")

		close(results)
		for err := range results {
			assert.NoError(t, err)
		}
	})

	// ── Test 2: SetAsyncReplicationState(disabled) unblocks waiters ──────────
	// This is the exact scenario Fix #9 addresses: goroutines blocked in
	// waitForMinimalHashTreeInitialization must be unblocked when
	// SetAsyncReplicationState disables async replication.
	//
	// Correctness is independent of scheduling order:
	//   • If goroutines reach select before disable: channel is closed by disable
	//     → select returns → goroutines exit.
	//   • If disable runs first: hashtree is nil by the time goroutines take
	//     their RLock → fast-path returns nil immediately.
	// Either way all goroutines return nil — the test is non-flaky.
	t.Run("DisableUnblocksWaiters", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		const n = 5
		results := make(chan error, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for range n {
			go func() {
				defer wg.Done()
				results <- s.waitForMinimalHashTreeInitialization(ctx)
			}()
		}

		require.NoError(t, s.SetAsyncReplicationState(context.Background(), AsyncReplicationConfig{}, false))

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		awaitDone(t, done, "goroutines did not unblock after SetAsyncReplicationState(disabled)")

		close(results)
		for err := range results {
			assert.NoError(t, err)
		}

		// Post-condition: all state cleared.
		s.asyncReplicationRWMux.RLock()
		assert.Nil(t, s.hashtree)
		assert.False(t, s.hashtreeFullyInitialized)
		s.asyncReplicationRWMux.RUnlock()
	})

	// ── Test 3: mayStopAsyncReplication unblocks waiters ─────────────────────
	// mayStopAsyncReplication is the shutdown path used during shard close.
	// It must also close the init channel so goroutines are not leaked.
	t.Run("StopUnblocksWaiters", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		const n = 5
		results := make(chan error, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for range n {
			go func() {
				defer wg.Done()
				results <- s.waitForMinimalHashTreeInitialization(ctx)
			}()
		}

		s.mayStopAsyncReplication()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		awaitDone(t, done, "goroutines did not unblock after mayStopAsyncReplication")

		close(results)
		for err := range results {
			assert.NoError(t, err)
		}
	})

	// ── Double-close protection: releaseInitialization races with disable ─────
	// Both the init goroutine (releaseInitialization) and the disable path
	// (SetAsyncReplicationState / mayStopAsyncReplication) can race to close
	// minimalHashtreeInitializationCh.  sync.Once must guarantee exactly one
	// close, preventing a panic.  The loop runs the race repeatedly so the
	// -race detector can observe any data races on the channel or the Once.
	t.Run("NoPanicOnConcurrentChannelCloseAndDisable", func(t *testing.T) {
		for range 20 {
			sl, _ := testShard(t, ctx, class)
			s := concreteShard(t, sl)
			cancel := simulateInitInProgress(t, s)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				// Simulate releaseInitialization inside initHashtree.
				s.minimalHashtreeInitializationOnce.Do(func() {
					close(s.minimalHashtreeInitializationCh)
				})
			}()
			go func() {
				defer wg.Done()
				_ = s.SetAsyncReplicationState(context.Background(), AsyncReplicationConfig{}, false)
			}()
			wg.Wait() // no panic == pass
			cancel()
		}
	})

	// ── Test 5: enable → disable → re-enable cycle ───────────────────────────
	// Verifies that the shard can be cycled through enable → disable → re-enable
	// without panicking or deadlocking.  In particular checks that
	// initAsyncReplication allocates a fresh channel and a fresh sync.Once on
	// each call, so the second enable does not reuse a spent Once or a closed
	// channel.
	t.Run("EnableDisableReenableCycle", func(t *testing.T) {
		config := AsyncReplicationConfig{hashtreeHeight: 1}
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)

		// First enable: spawns init goroutine; hashtree must be allocated.
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))
		s.asyncReplicationRWMux.RLock()
		assert.NotNil(t, s.hashtree, "hashtree must be allocated after first enable")
		s.asyncReplicationRWMux.RUnlock()

		// Disable: cancels the init goroutine and clears all state.
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, false))
		s.asyncReplicationRWMux.RLock()
		assert.Nil(t, s.hashtree, "hashtree must be nil after disable")
		s.asyncReplicationRWMux.RUnlock()

		// Re-enable: must allocate a fresh channel and Once; no panic or deadlock.
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))
		s.asyncReplicationRWMux.RLock()
		assert.NotNil(t, s.hashtree, "hashtree must be re-allocated after re-enable")
		s.asyncReplicationRWMux.RUnlock()

		// The init goroutine scans the (empty) bucket and completes quickly.
		// waitForMinimalHashTreeInitialization must unblock without timing out.
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer waitCancel()
		require.NoError(t, s.waitForMinimalHashTreeInitialization(waitCtx),
			"waitForMinimalHashTreeInitialization should return nil after re-enable on empty shard")

		// Idempotent enable: calling enable again when already enabled is a no-op.
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))

		// Final cleanup so the shard's background goroutine is stopped.
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, false))
	})

	// ── Regression: batchDeleteObject + concurrent disable must not deadlock ──
	//
	// Before the fix, batchDeleteObject held asyncReplicationRWMux.RLock() and
	// then called waitForMinimalHashTreeInitialization, which internally tries a
	// second RLock on the same mutex. With Go's writer-preference, if
	// SetAsyncReplicationState was waiting to acquire the write lock, the second
	// RLock was permanently blocked — and the write lock itself was blocked
	// waiting for the first RLock to be released. DEADLOCK.
	//
	// The fix moves waitForMinimalHashTreeInitialization before the RLock so
	// there is never a nested lock acquisition.
	t.Run("BatchDeleteDuringDisable", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)

		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))

		// Put the shard in "init in progress" state: hashtree allocated, channel
		// open, fully-initialized flag false.  batchDeleteObject will block in
		// waitForMinimalHashTreeInitialization until the channel is closed.
		cancel := simulateInitInProgress(t, s)
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Must not deadlock with the concurrent SetAsyncReplicationState below.
			_ = s.batchDeleteObject(ctx, id, time.Now())
		}()

		// Disable async replication: acquires the write lock, closes the channel,
		// and clears s.hashtree.  With the old lock order this would deadlock
		// because batchDeleteObject held RLock while waiting for the channel that
		// SetAsyncReplicationState needs the write lock to close.
		cfg := AsyncReplicationConfig{hashtreeHeight: 1}
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, false))

		awaitDone(t, done, "batchDeleteObject deadlocked with concurrent SetAsyncReplicationState(false)")
	})

	// ── Init scan populates the hashtree correctly ────────────────────────────
	//
	// Validates that the lock-free init callback (captured ht pointer +
	// direct AggregateLeafWith, no per-object RLock) correctly registers all
	// objects that were present in the shard before async replication was
	// enabled.  A non-zero root digest confirms objects were registered.
	t.Run("InitScanPopulatesHashtree", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)

		// Insert objects before enabling async replication.
		for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
			require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
		}

		cfg := AsyncReplicationConfig{hashtreeHeight: 1, initShieldCPUEveryN: 1}
		require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, true))

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer waitCancel()
		require.NoError(t, s.waitForMinimalHashTreeInitialization(waitCtx))

		s.asyncReplicationRWMux.RLock()
		require.NotNil(t, s.hashtree)
		require.True(t, s.hashtreeFullyInitialized)
		root := s.hashtree.Root()
		s.asyncReplicationRWMux.RUnlock()

		var zero hashtree.Digest
		require.NotEqual(t, zero, root,
			"hashtree root must be non-zero: objects inserted before init must have been registered")

		require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, false))
	})
}

// ─── propagateObjects ─────────────────────────────────────────────────────────

// overwriteRecordingClient records every VObject batch sent via OverwriteObjects.
// All other methods delegate to FakeReplicationClient (no-ops / zero values).
type overwriteRecordingClient struct {
	FakeReplicationClient
	mu      sync.Mutex
	batches [][]*objects.VObject
}

func (c *overwriteRecordingClient) OverwriteObjects(_ context.Context, _, _, _ string, objs []*objects.VObject) ([]routerTypes.RepairResponse, error) {
	batch := make([]*objects.VObject, len(objs))
	copy(batch, objs)
	c.mu.Lock()
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return nil, nil
}

// panicOverwriteClient panics on every OverwriteObjects call to simulate a
// worker-level panic inside propagateObjects.
type panicOverwriteClient struct{ FakeReplicationClient }

func (*panicOverwriteClient) OverwriteObjects(_ context.Context, _, _, _ string, _ []*objects.VObject) ([]routerTypes.RepairResponse, error) {
	panic("injected test panic")
}

// blockingOverwriteClient blocks until the context is cancelled, letting tests
// verify that propagateObjects terminates promptly on context cancellation.
type blockingOverwriteClient struct{ FakeReplicationClient }

func (*blockingOverwriteClient) OverwriteObjects(ctx context.Context, _, _, _ string, _ []*objects.VObject) ([]routerTypes.RepairResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// propagationTestConfig returns an AsyncReplicationConfig configured for
// propagateObjects unit tests with the given worker count and batch size.
func propagationTestConfig(concurrency, batchSize int) AsyncReplicationConfig {
	return AsyncReplicationConfig{
		propagationConcurrency: concurrency,
		propagationBatchSize:   batchSize,
	}
}

// TestPropagateObjects verifies three important properties of propagateObjects:
//
//  1. HappyPath – all objects are collected and forwarded to the replication
//     client without error.
//
//  2. WorkerPanicSurfacedAsError – a panic inside a worker goroutine is
//     surfaced as a non-nil error and the function returns without deadlocking.
//     Before the fix, the drain-loop defer would steal batches from healthy
//     workers, GoWrapper would silently swallow the panic, and propagateObjects
//     could return nil while objects were never propagated.
//
//  3. ContextCancellationNoDeadlock – when the parent context is cancelled
//     mid-flight, propagateObjects returns promptly; wg accounting via the
//     workerCtx drain path keeps channels and the WaitGroup consistent.
func TestPropagateObjects(t *testing.T) {
	ctx := context.Background()
	const class = "PropagateObjectsTest"
	const deadline = 5 * time.Second

	// awaitDone runs f in a goroutine and fails the test if it does not return
	// within deadline.  It is the deadlock detector for all sub-tests.
	awaitDone := func(t *testing.T, f func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { f(); close(done) }()
		select {
		case <-done:
		case <-time.After(deadline):
			t.Fatal("propagateObjects did not return within the deadline (possible deadlock)")
		}
	}

	concreteShard := func(t *testing.T, sl ShardLike) *Shard {
		t.Helper()
		s, ok := sl.(*Shard)
		require.True(t, ok, "expected *Shard from testShard (EnableLazyLoadShards=true)")
		return s
	}

	// insertObjects inserts n objects into the shard and returns their UUIDs.
	insertObjects := func(t *testing.T, sl ShardLike, n int) []strfmt.UUID {
		t.Helper()
		uuids := make([]strfmt.UUID, n)
		for i := range n {
			id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", i+1))
			uuids[i] = id
			require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
		}
		return uuids
	}

	t.Run("HappyPath", func(t *testing.T) {
		client := &overwriteRecordingClient{}
		sl, _ := testShard(t, ctx, class, withReplicationClient(t, client))
		s := concreteShard(t, sl)

		const n = 6
		uuids := insertObjects(t, sl, n)
		cfg := propagationTestConfig(2, 2) // 2 workers, batches of 2 → 3 batches

		var err error
		awaitDone(t, func() {
			_, err = s.propagateObjects(ctx, cfg, "http://fake-host", uuids, nil)
		})

		require.NoError(t, err)
		client.mu.Lock()
		totalSent := 0
		for _, batch := range client.batches {
			totalSent += len(batch)
		}
		client.mu.Unlock()
		assert.Equal(t, n, totalSent, "all objects must have been forwarded to the replication client")
	})

	t.Run("WorkerPanicSurfacedAsError", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class, withReplicationClient(t, &panicOverwriteClient{}))
		s := concreteShard(t, sl)

		uuids := insertObjects(t, sl, 6)
		cfg := propagationTestConfig(2, 2)

		var err error
		awaitDone(t, func() {
			_, err = s.propagateObjects(ctx, cfg, "http://fake-host", uuids, nil)
		})

		require.Error(t, err, "a worker panic must be surfaced as a non-nil error")
		assert.Contains(t, err.Error(), "panic")
	})

	t.Run("ContextCancellationNoDeadlock", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class, withReplicationClient(t, &blockingOverwriteClient{}))
		s := concreteShard(t, sl)

		uuids := insertObjects(t, sl, 4)
		cfg := propagationTestConfig(2, 2)

		cancelCtx, cancel := context.WithCancel(ctx)
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		awaitDone(t, func() {
			_, _ = s.propagateObjects(cancelCtx, cfg, "http://fake-host", uuids, nil)
		})
	})
}
