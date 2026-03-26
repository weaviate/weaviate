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
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// concreteShard unwraps the *Shard from a ShardLike returned by testShard.
// testShard passes EnableLazyLoadShards=true which causes initShard to create
// a real *Shard directly (not a LazyLoadShard wrapper).
func concreteShard(t *testing.T, sl ShardLike) *Shard {
	t.Helper()
	s, ok := sl.(*Shard)
	require.True(t, ok, "expected *Shard from testShard (EnableLazyLoadShards=true)")
	return s
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

// minAsyncReplicationConfig returns a valid AsyncReplicationConfig suitable
// for unit tests that enable async replication.  All ticker durations are
// non-zero (time.NewTicker panics on zero) but large enough that background
// goroutines don't fire during the test lifetime.
func minAsyncReplicationConfig() AsyncReplicationConfig {
	return AsyncReplicationConfig{
		hashtreeHeight:              1,
		frequency:                   10 * time.Minute,
		frequencyWhilePropagating:   10 * time.Minute,
		aliveNodesCheckingFrequency: 10 * time.Minute,
		diffBatchSize:               100,
		propagationLimit:            100,
		propagationConcurrency:      1,
		propagationBatchSize:        10,
		diffPerNodeTimeout:          5 * time.Second,
		prePropagationTimeout:       5 * time.Second,
		propagationTimeout:          5 * time.Second,
		initShieldCPUEveryN:         1,
	}
}

// awaitHashtreeInitialized polls hashtreeFullyInitialized until it is true or
// the deadline fires.  Since writes no longer update the hashtree, the init
// goroutine is the only writer; polling under RLock is safe and race-free.
func awaitHashtreeInitialized(t *testing.T, s *Shard) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()
		return s.hashtreeFullyInitialized
	}, 10*time.Second, 10*time.Millisecond, "hashtree did not become fully initialized within timeout")
}

// TestAsyncReplicationEnableDisableCycle verifies the shard can be cycled
// through enable → disable → re-enable without panicking or deadlocking, and
// that hashtree state is correctly managed across transitions.
func TestAsyncReplicationEnableDisableCycle(t *testing.T) {
	ctx := context.Background()
	const class = "AsyncReplicationLifecycleTest"

	config := minAsyncReplicationConfig()
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
	assert.False(t, s.hashtreeFullyInitialized)
	s.asyncReplicationRWMux.RUnlock()

	// Re-enable: must work without panic or deadlock.
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))
	s.asyncReplicationRWMux.RLock()
	assert.NotNil(t, s.hashtree, "hashtree must be re-allocated after re-enable")
	s.asyncReplicationRWMux.RUnlock()

	// Wait for the init goroutine on the (empty) shard to finish.
	awaitHashtreeInitialized(t, s)

	// Idempotent enable: no-op when already enabled.
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, true))

	// Final cleanup.
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), config, false))
}

// TestInitScanPopulatesHashtree validates that objects on disk when async
// replication is enabled are correctly registered in the hashtree by the init
// scan (ApplyToOnDiskObjectDigests), producing a non-zero root digest.
func TestInitScanPopulatesHashtree(t *testing.T) {
	ctx := context.Background()
	const class = "InitScanHashtreeTest"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)

	// Objects must be flushed to disk before enabling async replication:
	// initHashtree only scans on-disk segments (no memtables).
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, true))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	require.NotNil(t, s.hashtree)
	root := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	require.NotEqual(t, hashtree.Digest{}, root,
		"hashtree root must be non-zero: on-disk objects must have been registered by init scan")

	require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, false))
}

// TestHashtreeUpdateOnFlush verifies that objects are NOT in the hashtree while
// they are in the memtable, and ARE registered after the memtable is flushed to
// disk.  This is the core invariant of the flush-time hashtree update: the
// hashtree always reflects durable (on-disk) data only.
//
// The test does not assume a specific initial root value (the shard may have
// residual on-disk data from initialization).  Instead it verifies the relative
// change: inserts do not change the root, but a flush does.
func TestHashtreeUpdateOnFlush(t *testing.T) {
	ctx := context.Background()
	const class = "HashtreeFlushTest"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)

	// Enable on the shard; init scan of on-disk segments completes quickly.
	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, true))
	awaitHashtreeInitialized(t, s)

	// Record baseline root after init (captures whatever is already on disk).
	s.asyncReplicationRWMux.RLock()
	rootAfterInit := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	// Insert objects — they land in the memtable, not on disk yet.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}

	// Root must be unchanged: writes do not update the hashtree.
	s.asyncReplicationRWMux.RLock()
	rootAfterInsert := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.Equal(t, rootAfterInit, rootAfterInsert,
		"hashtree root must not change on write: objects are only registered at flush time")

	// Flush to disk — updateHashtreeOnFlush fires synchronously inside FlushAndSwitch.
	require.NoError(t, s.store.FlushMemtables(ctx))

	// Root must have changed: the flushed objects are now registered.
	s.asyncReplicationRWMux.RLock()
	rootAfterFlush := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.NotEqual(t, rootAfterInsert, rootAfterFlush,
		"hashtree root must change after flush: updateHashtreeOnFlush must have registered the objects")

	require.NoError(t, s.SetAsyncReplicationState(context.Background(), cfg, false))
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

// ─── Shutdown hashtree persistence ───────────────────────────────────────────

// htFilesInDir returns all .ht files found directly inside dir.
func htFilesInDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var ht []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".ht" {
			ht = append(ht, e)
		}
	}
	return ht
}

// TestShutdownHashtreePersistsMemtableObjects is the core regression test for
// the bug fixed in shard_shutdown.go: Bucket.Shutdown flushes the active
// memtable via b.active.flush() which does NOT fire objectFlushCallback, so
// objects that were still in the memtable at shutdown time were missing from
// the saved .ht file.
//
// The fix calls bucket.FlushAndSwitch() explicitly before store.Shutdown so
// that updateHashtreeOnFlush fires and the hashtree includes every object that
// lands on disk during the shutdown flush.
func TestShutdownHashtreePersistsMemtableObjects(t *testing.T) {
	ctx := context.Background()
	const class = "ShutdownHashtreeMemtableTest"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.SetAsyncReplicationState(ctx, cfg, true))
	awaitHashtreeInitialized(t, s)

	// Capture baseline root after init (the shard may have pre-existing state).
	s.asyncReplicationRWMux.RLock()
	rootAfterInit := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	// Insert objects — they land in the memtable only, not yet on disk.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}

	// Hashtree must still equal the baseline: writes go to memtable only; the
	// hashtree reflects durable (flushed) data exclusively.
	s.asyncReplicationRWMux.RLock()
	rootAfterInsert := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.Equal(t, rootAfterInit, rootAfterInsert,
		"hashtree must not change while objects are only in the memtable")

	// Capture the hashtree directory path before shutdown destroys the shard.
	htPath := s.pathHashTree()

	// Shutdown: performShutdown calls FlushAndSwitch (our fix) so that
	// updateHashtreeOnFlush fires, then mayStopAsyncReplication saves the .ht.
	require.NoError(t, sl.Shutdown(ctx))

	// Exactly one .ht file must have been written.
	htFiles := htFilesInDir(t, htPath)
	require.Len(t, htFiles, 1, "exactly one .ht file must be written on shutdown")

	// The persisted hashtree must differ from the pre-shutdown baseline: the
	// memtable objects must have been registered via updateHashtreeOnFlush
	// during the shutdown FlushAndSwitch call.
	f, err := os.Open(filepath.Join(htPath, htFiles[0].Name()))
	require.NoError(t, err)
	defer f.Close()
	ht, err := hashtree.DeserializeHashTree(bufio.NewReader(f))
	require.NoError(t, err)
	require.NotEqual(t, rootAfterInit, ht.Root(),
		"persisted hashtree must include the objects that were in the memtable at shutdown")
}

// TestMayStopAsyncReplicationSkipsDumpOnFlushFailure verifies the guard
// introduced to prevent a stale .ht file from being saved when the pre-dump
// FlushAndSwitch fails.
//
// When hashtreeFlushFailed is true, mayStopAsyncReplication must:
//   - NOT write a .ht file (a stale file is worse than no file; on restart
//     initHashtree will re-scan from disk and produce a correct hashtree).
//   - Reset hashtreeFlushFailed to false.
//   - Still clean up all other async replication state (hashtree → nil, etc.).
func TestMayStopAsyncReplicationSkipsDumpOnFlushFailure(t *testing.T) {
	ctx := context.Background()
	const class = "SkipDumpOnFlushFailureTest"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.SetAsyncReplicationState(ctx, cfg, true))
	awaitHashtreeInitialized(t, s)

	htPath := s.pathHashTree()

	// Simulate a FlushAndSwitch failure during performShutdown.
	// hashtreeFlushFailed is written outside asyncReplicationRWMux in
	// production (single-goroutine shutdown path); it is safe to set here
	// because the test is the only goroutine touching the shard at this point.
	s.hashtreeFlushFailed = true

	s.mayStopAsyncReplication()

	// No .ht file must have been written.
	require.Empty(t, htFilesInDir(t, htPath),
		"no .ht file must be written when hashtreeFlushFailed is set")

	// The flag must be reset so that a subsequent re-enable can dump correctly.
	require.False(t, s.hashtreeFlushFailed,
		"hashtreeFlushFailed must be reset to false by mayStopAsyncReplication")

	// All other async replication state must be cleaned up.
	require.Nil(t, s.hashtree, "hashtree must be nil after mayStopAsyncReplication")
	require.False(t, s.hashtreeFullyInitialized,
		"hashtreeFullyInitialized must be false after mayStopAsyncReplication")
}

// TestMayStopAsyncReplicationDumpsHashtreeOnSuccess verifies the happy path:
// when hashtreeFlushFailed is false (the default), mayStopAsyncReplication
// writes a .ht file that can be loaded on the next startup.
func TestMayStopAsyncReplicationDumpsHashtreeOnSuccess(t *testing.T) {
	ctx := context.Background()
	const class = "DumpHashtreeOnSuccessTest"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	// Put objects and flush to disk so the hashtree has non-zero state after
	// the init scan.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	flushShard(t, ctx, sl)

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.SetAsyncReplicationState(ctx, cfg, true))
	awaitHashtreeInitialized(t, s)

	htPath := s.pathHashTree()

	// hashtreeFlushFailed is false by default — dump should proceed.
	require.False(t, s.hashtreeFlushFailed)
	s.mayStopAsyncReplication()

	htFiles := htFilesInDir(t, htPath)
	require.Len(t, htFiles, 1,
		".ht file must be written when hashtreeFullyInitialized is true and hashtreeFlushFailed is false")

	// The persisted root must be non-zero: on-disk objects were registered by
	// the init scan.
	f, err := os.Open(filepath.Join(htPath, htFiles[0].Name()))
	require.NoError(t, err)
	defer f.Close()
	ht, err := hashtree.DeserializeHashTree(bufio.NewReader(f))
	require.NoError(t, err)
	require.NotEqual(t, hashtree.Digest{}, ht.Root(),
		"persisted hashtree root must be non-zero: on-disk objects must have been registered")
}
