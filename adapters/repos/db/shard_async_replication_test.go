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
	"errors"
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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	entreplication "github.com/weaviate/weaviate/entities/replication"
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

// fixedDigestsClient is a test double whose DigestObjectsInRange always
// returns a pre-configured slice of digests. It also mirrors that "remote"
// state through CompareDigests by returning every source digest absent from
// `digests` as missing (UpdateTime=0) and every source digest with a strictly
// newer source UpdateTime as stale (UpdateTime=remote's value). This lets
// pre-existing tests retain their "remote-knowledge" semantics under the new
// CompareDigests protocol.
type fixedDigestsClient struct {
	FakeReplicationClient
	digests          []routerTypes.RepairResponse
	compareDigestErr error // if non-nil, CompareDigests returns this error instead of comparing
}

func (c *fixedDigestsClient) DigestObjectsInRange(
	_ context.Context, _, _, _ string, _, _ strfmt.UUID, _ int,
) ([]routerTypes.RepairResponse, error) {
	return c.digests, nil
}

func (c *fixedDigestsClient) CompareDigests(
	_ context.Context, _, _, _ string, source []routerTypes.RepairResponse,
) ([]routerTypes.RepairResponse, error) {
	if c.compareDigestErr != nil {
		return nil, c.compareDigestErr
	}
	remote := make(map[string]int64, len(c.digests))
	for _, d := range c.digests {
		remote[d.ID] = d.UpdateTime
	}
	out := make([]routerTypes.RepairResponse, 0, len(source))
	for _, s := range source {
		rt, ok := remote[s.ID]
		if !ok {
			// Missing on remote.
			out = append(out, routerTypes.RepairResponse{ID: s.ID, UpdateTime: 0})
			continue
		}
		if s.UpdateTime > rt {
			// Source is strictly newer than remote — stale on target.
			out = append(out, routerTypes.RepairResponse{ID: s.ID, UpdateTime: rt})
		}
		// Equal or remote-newer → no action needed.
	}
	return out, nil
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

// withAsyncScheduler returns a testShard index option that installs a started
// AsyncReplicationScheduler on the index. Tests that call enableAsyncReplication
// directly need this because the function guards against a nil scheduler.
func withAsyncScheduler(t *testing.T) func(*Index) {
	t.Helper()
	return func(idx *Index) {
		sched := newStartedTestScheduler(t, 1)
		idx.asyncReplicationScheduler = sched
	}
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

// flushShard forces all pending memtable data to disk.
func flushShard(t *testing.T, ctx context.Context, shard ShardLike) {
	t.Helper()
	s, ok := shard.(*Shard)
	require.True(t, ok, "flushShard: expected *Shard, got %T", shard)
	require.NoError(t, s.store.FlushMemtables(ctx))
}

func (s *Shard) propagateWithinRangeForTest(t *testing.T, ctx context.Context,
	cfg AsyncReplicationConfig, addr, node string, initialLeaf, finalLeaf uint64,
	limit int, overrides additional.AsyncReplicationTargetNodeOverrides,
) (int, []objectToPropagate, error) {
	t.Helper()
	cursor := s.store.Bucket(helpers.ObjectsBucketLSM).CursorReplaceReusable()
	defer cursor.Close()
	scratch := newPropagationScratch(cfg.diffBatchSize)
	return s.objectsToPropagateWithinRange(ctx, cfg, cursor, scratch, addr, node, initialLeaf, finalLeaf, limit, overrides)
}

// ─── objectsToPropagateWithinRange ───────────────────────────────────────────

// TestObjectsToPropagateWithinRange covers the scanning and filtering logic
// inside objectsToPropagateWithinRange. Tests use well-known UUIDs so that
// batch ordering is deterministic. Flushing is not required for visibility:
// ObjectDigestsInRange uses bucket.Cursor(), which includes memtables.
func TestObjectsToPropagateWithinRange(t *testing.T) {
	ctx := context.Background()
	const class = "PropagateRangeTest"

	t.Run("EmptyShard", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class)
		s := concreteShard(t, sl)
		cfg := fullRangeConfig(100)

		local, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, local)
		assert.Empty(t, objs)
		_ = idx
	})

	// MemtableObjectsArePropagated verifies that objects still in the memtable
	// (not yet flushed to disk) are visible to DigestObjectsInRange because the
	// bucket cursor includes both memtable and on-disk segments.
	t.Run("MemtableObjectsArePropagated", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{}))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		cfg := fullRangeConfig(100)

		local, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 2, local, "memtable objects must be visible to the merged bucket cursor")
		assert.Len(t, objs, 2, "both memtable objects must be queued for propagation")
		_ = idx
	})

	// SourceBehindRemote verifies the no-op path: remote already has the same
	// objects with equal or newer timestamps, so nothing should be propagated.
	t.Run("SourceBehindRemote", func(t *testing.T) {
		// Pre-configure the remote to return the same digests as local.
		remoteDigests := []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: tsFarPast},
			{ID: string(uuidHigh), UpdateTime: tsFarPast},
		}
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{digests: remoteDigests}))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		local, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 2, local)
		assert.Empty(t, objs, "remote is up-to-date → nothing to propagate")
		_ = idx
	})

	// LimitEnforcement verifies that the propagation limit caps the number of
	// objects queued. The default FakeReplicationClient returns empty digests for
	// DigestObjectsInRange (remote has nothing), so every local object appears
	// missing and is queued for propagation.
	t.Run("LimitEnforcement", func(t *testing.T) {
		const limit = 1
		// fixedDigestsClient with no preconfigured digests → CompareDigests
		// reports every source UUID as missing on remote, so each local
		// object is queued for propagation (subject to the limit).
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{}))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		_, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, limit, nil,
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(objs), limit,
			"number of queued propagations must not exceed the limit")
		_ = idx
	})

	// PropagationDelayExcludesRecentObjects verifies that objects whose UpdateTime
	// falls within [now - propagationDelay, now] are excluded from the propagation
	// batch. This exercises the getHashBeatMaxUpdateTime filter in
	// objectsToPropagateWithinRange. Without the filter a regression (e.g.
	// reversed comparison) would silently propagate objects before the delay
	// elapses, breaking the propagation-delay contract.
	t.Run("PropagationDelayExcludesRecentObjects", func(t *testing.T) {
		// Remote has no objects, so every local object would normally appear as
		// missing and be queued for propagation.
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{}))

		// Use a timestamp very close to now so that it is definitely within the
		// propagation delay window for any realistic clock skew.
		recentTS := time.Now().UnixMilli()
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, recentTS)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))

		cfg := AsyncReplicationConfig{
			hashtreeHeight:   1,
			diffBatchSize:    100,
			propagationDelay: 30 * time.Second,
		}

		local, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 0, local,
			"recently updated object must be excluded by the propagation delay filter")
		assert.Empty(t, objs,
			"no objects must be queued when all are within the propagation delay window")
		_ = idx
	})

	// PropagationDelayCursorAdvancesFastBatch verifies that when an entire batch
	// is filtered out by the propagation delay, the cursor advances past it and
	// continues scanning the remaining UUID space. batchSize=1 forces each UUID
	// into its own batch, so the cursor-advance branch in the filtered path is
	// exercised and uuidHigh (which is old) must still be found and queued.
	t.Run("PropagationDelayCursorAdvancesPastFilteredBatch", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{}))

		recentTS := time.Now().UnixMilli()
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, recentTS)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))

		cfg := AsyncReplicationConfig{
			hashtreeHeight:   1,
			diffBatchSize:    1, // one UUID per batch; forces cursor advance when uuidLow is filtered
			propagationDelay: 30 * time.Second,
		}

		local, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 1, local,
			"only uuidHigh (old) must count; uuidLow (recent) must be excluded")
		require.Len(t, objs, 1,
			"cursor must advance past the all-filtered uuidLow batch and find uuidHigh")
		assert.Equal(t, uuidHigh, objs[0].uuid,
			"the queued object must be uuidHigh, not the recent uuidLow")
		_ = idx
	})

	// CompareDigestsTransportError verifies that a wire failure on CompareDigests
	// is wrapped, surfaces no propagated objects, and does not corrupt the local
	// scan counters (local count reflects what was scanned before the wire call).
	t.Run("CompareDigestsTransportError", func(t *testing.T) {
		client := &fixedDigestsClient{compareDigestErr: errors.New("simulated rpc unavailable")}
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, client))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		_, objs, err := s.propagateWithinRangeForTest(
			t, ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "comparing digests with remote",
			"transport error must be wrapped with the scheduler's prefix")
		assert.Contains(t, err.Error(), "simulated rpc unavailable",
			"original error must be preserved via %w wrapping")
		assert.Empty(t, objs, "no objects must be queued when CompareDigests fails")
		_ = idx
	})

	// ScratchReusedAcrossRanges guards the per-beat buffer reuse: a second call on the same scratch must not leak the first's queued objects.
	t.Run("ScratchReusedAcrossRanges", func(t *testing.T) {
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &fixedDigestsClient{}))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		cursor := s.store.Bucket(helpers.ObjectsBucketLSM).CursorReplaceReusable()
		defer cursor.Close()
		scratch := newPropagationScratch(cfg.diffBatchSize)

		local1, objs1, err := s.objectsToPropagateWithinRange(ctx, cfg, cursor, scratch, "http://fake", "node2", 0, 1, 100, nil)
		require.NoError(t, err)
		require.Len(t, objs1, 2)
		want := []strfmt.UUID{objs1[0].uuid, objs1[1].uuid}

		local2, objs2, err := s.objectsToPropagateWithinRange(ctx, cfg, cursor, scratch, "http://fake", "node2", 0, 1, 100, nil)
		require.NoError(t, err)
		require.Len(t, objs2, 2, "reused scratch must not leak the prior range's queued objects")
		assert.Equal(t, local1, local2)
		assert.Equal(t, want, []strfmt.UUID{objs2[0].uuid, objs2[1].uuid})
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
		hashtreeHeight:            1,
		frequency:                 10 * time.Minute,
		frequencyWhilePropagating: 10 * time.Minute,
		diffBatchSize:             100,
		propagationLimit:          100,
		propagationConcurrency:    1,
		propagationBatchSize:      10,
		diffPerNodeTimeout:        5 * time.Second,
		prePropagationTimeout:     5 * time.Second,
		propagationTimeout:        5 * time.Second,
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
	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)

	// First enable: spawns init goroutine; hashtree must be allocated.
	require.NoError(t, s.enableAsyncReplication(context.Background(), config))
	s.asyncReplicationRWMux.RLock()
	assert.NotNil(t, s.hashtree, "hashtree must be allocated after first enable")
	s.asyncReplicationRWMux.RUnlock()

	// Disable: cancels the init goroutine and clears all state.
	require.NoError(t, s.disableAsyncReplication(context.Background()))
	s.asyncReplicationRWMux.RLock()
	assert.Nil(t, s.hashtree, "hashtree must be nil after disable")
	assert.False(t, s.hashtreeFullyInitialized)
	s.asyncReplicationRWMux.RUnlock()

	// Re-enable: must work without panic or deadlock.
	require.NoError(t, s.enableAsyncReplication(context.Background(), config))
	s.asyncReplicationRWMux.RLock()
	assert.NotNil(t, s.hashtree, "hashtree must be re-allocated after re-enable")
	s.asyncReplicationRWMux.RUnlock()

	// Wait for the init goroutine on the (empty) shard to finish.
	awaitHashtreeInitialized(t, s)

	// Idempotent enable: no-op when already enabled.
	require.NoError(t, s.enableAsyncReplication(context.Background(), config))
	require.NoError(t, s.enableAsyncReplication(context.Background(), config))

	// Config update while running: enableAsyncReplication with a different
	// config must update s.asyncReplicationConfig even though the shard is
	// already running. This covers the case where per-class overrides are
	// cleared (cfg.AsyncConfig transitions from non-nil to nil): the caller
	// does not call disableAsyncReplication first, so enableAsyncReplication
	// must update the stored config itself.
	updatedConfig := config
	newFreq := 99 * time.Minute
	updatedConfig.frequency = newFreq
	require.NoError(t, s.enableAsyncReplication(context.Background(), updatedConfig))
	s.asyncReplicationRWMux.RLock()
	assert.NotNil(t, s.hashtree, "hashtree must still be running after config update")
	assert.Equal(t, newFreq, s.asyncReplicationConfig.frequency,
		"asyncReplicationConfig must be updated even when shard is already running")
	s.asyncReplicationRWMux.RUnlock()

	// Final cleanup.
	require.NoError(t, s.disableAsyncReplication(context.Background()))
}

// TestInitScanPopulatesHashtree validates that objects on disk when async
// replication is enabled are correctly registered in the hashtree by the init
// scan (ApplyToOnDiskObjectDigests), producing a non-zero root digest.
func TestInitScanPopulatesHashtree(t *testing.T) {
	ctx := context.Background()
	const class = "InitScanHashtreeTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)

	// Objects must be flushed to disk before enabling async replication:
	// initHashtree only scans on-disk segments (no memtables).
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.enableAsyncReplication(context.Background(), cfg))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	require.NotNil(t, s.hashtree)
	root := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	require.NotEqual(t, hashtree.Digest{}, root,
		"hashtree root must be non-zero: on-disk objects must have been registered by init scan")

	require.NoError(t, s.disableAsyncReplication(context.Background()))
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
			time.Sleep(50 * time.Millisecond)
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

// TestMayStopAsyncReplicationDumpsHashtreeOnSuccess verifies the happy path:
// mayStopAsyncReplication writes a .ht file that can be loaded on the next startup.
func TestMayStopAsyncReplicationDumpsHashtreeOnSuccess(t *testing.T) {
	ctx := context.Background()
	const class = "DumpHashtreeOnSuccessTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	// Put objects and flush to disk so the hashtree has non-zero state after
	// the init scan.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	flushShard(t, ctx, sl)

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	htPath := s.pathHashTree()

	s.mayStopAsyncReplication()

	htFiles := htFilesInDir(t, htPath)
	require.Len(t, htFiles, 1,
		".ht file must be written when hashtreeFullyInitialized is true")

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

// ─── Fix: disk I/O outside the write lock ────────────────────────────────────
//
// The following tests cover the fix that moved tryLoadHashtreeFromDisk outside
// the asyncReplicationRWMux write lock in enableAsyncReplication (and the
// shard startup path in initNonVector). Before the fix, the write lock was
// acquired at the top of enableAsyncReplication and held for the entire
// tryLoadHashtreeFromDisk call — blocking all concurrent RLock callers for the
// full duration of disk I/O (ReadDir, OpenFile, Read, Remove, Fsync).

// TestEnableAsyncReplication_LoadsHashtreeFromDisk verifies the new disk-load
// code path: when a .ht file is present, enableAsyncReplication loads it
// and sets hashtreeFullyInitialized = true without waiting for a background
// init scan. It also verifies the file is consumed (deleted) on load.
func TestEnableAsyncReplication_LoadsHashtreeFromDisk(t *testing.T) {
	ctx := context.Background()
	const class = "LoadHashtreeFromDiskTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	// First enable: builds hashtree from scratch via background init scan.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	// Persist some objects so the hashtree has a non-trivial root that we can
	// compare after re-enable.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))

	// Capture root before disable so we can verify it survives the round-trip.
	s.asyncReplicationRWMux.RLock()
	rootBeforeDisable := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	htPath := s.pathHashTree()

	// Disable: mayStopAsyncReplication serialises the hashtree to a .ht file.
	require.NoError(t, s.disableAsyncReplication(ctx))
	require.Len(t, htFilesInDir(t, htPath), 1,
		"pre-condition: disableAsyncReplication must have written a .ht file")

	// Re-enable: tryLoadHashtreeFromDisk should load the .ht file.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))

	// hashtreeFullyInitialized is set synchronously inside initAsyncReplication
	// when a cached tree is found (before enableAsyncReplication returns), so
	// it must be true by the time enableAsyncReplication returns — no polling.
	s.asyncReplicationRWMux.RLock()
	require.True(t, s.hashtreeFullyInitialized,
		"hashtree must be fully initialized from disk cache, not via a background scan")
	s.asyncReplicationRWMux.RUnlock()

	// Root must match what was saved: the persisted tree must have been loaded.
	s.asyncReplicationRWMux.RLock()
	rootAfterReEnable := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.Equal(t, rootBeforeDisable, rootAfterReEnable,
		"hashtree root must survive the disable/re-enable round-trip via disk")

	// tryLoadHashtreeFromDisk must delete the .ht file after loading it.
	require.Empty(t, htFilesInDir(t, htPath),
		".ht file must be consumed by enableAsyncReplication on successful load")

	require.NoError(t, s.disableAsyncReplication(ctx))
}

// TestEnableAsyncReplication_ConcurrentCallsAreSafe verifies that multiple
// goroutines racing to call enableAsyncReplication simultaneously do not cause
// data races, panics, or double-initialisation. Run with -race.
func TestEnableAsyncReplication_ConcurrentCallsAreSafe(t *testing.T) {
	ctx := context.Background()
	const class = "ConcurrentEnableTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Release all goroutines as close to simultaneously as possible.
	ready := make(chan struct{})
	for range numGoroutines {
		go func() {
			defer wg.Done()
			<-ready
			_ = s.enableAsyncReplication(ctx, cfg)
		}()
	}
	close(ready)
	wg.Wait()

	// Exactly one initialisation must have taken place; the hashtree must not
	// be nil regardless of which goroutine "won" the write lock.
	s.asyncReplicationRWMux.RLock()
	require.NotNil(t, s.hashtree,
		"hashtree must be initialised after concurrent enableAsyncReplication calls")
	s.asyncReplicationRWMux.RUnlock()

	require.NoError(t, s.disableAsyncReplication(ctx))
}

// TestEnableAsyncReplication_DiskIONotBlockedByRLock is the regression test for
// the write-lock starvation bug described in the code review.
//
// Before the fix, enableAsyncReplication acquired asyncReplicationRWMux.Lock()
// at the very top, then called tryLoadHashtreeFromDisk inside the critical
// section.  Any concurrent RLock holder (e.g. a hashbeat reader or a commit
// handler) was therefore blocked for the full disk I/O duration — observed as
// 1–3 minute goroutine blocks in production profiles.
//
// After the fix, tryLoadHashtreeFromDisk runs BEFORE the write lock is acquired.
// This test verifies the invariant directly:
//
//  1. A .ht file is planted by running a full enable → disable cycle.
//  2. asyncReplicationRWMux.RLock() is held in a goroutine, preventing any
//     write lock from being acquired.
//  3. enableAsyncReplication is started in a second goroutine.
//  4. While the RLock is still held we assert that the .ht file is consumed.
//     This can only happen if tryLoadHashtreeFromDisk ran without the write
//     lock — proving disk I/O is outside the critical section.
//
// With the old code step 4 would time out because the goroutine in step 3
// would immediately block on Lock() and never reach tryLoadHashtreeFromDisk.
func TestEnableAsyncReplication_DiskIONotBlockedByRLock(t *testing.T) {
	ctx := context.Background()
	const class = "DiskIONotBlockedTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	// Enable and wait for a full init so that disable will produce a .ht file.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	htPath := s.pathHashTree()

	// Disable: persists the hashtree to disk.
	require.NoError(t, s.disableAsyncReplication(ctx))
	require.Len(t, htFilesInDir(t, htPath), 1,
		"pre-condition: one .ht file must exist before the RLock test")

	// Hold asyncReplicationRWMux.RLock() — this prevents any write lock from
	// being acquired.  We release it only after the file-consumed check passes.
	rLockHeld := make(chan struct{})
	rLockRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(rLockRelease) }) }
	t.Cleanup(release) // safety: always release on test teardown

	go func() {
		s.asyncReplicationRWMux.RLock()
		close(rLockHeld)
		<-rLockRelease
		s.asyncReplicationRWMux.RUnlock()
	}()
	<-rLockHeld // wait until RLock is held before starting enableAsyncReplication

	// Start enableAsyncReplication; it must be able to run tryLoadHashtreeFromDisk
	// (and consume the .ht file) even while our goroutine holds asyncReplicationRWMux.RLock().
	enableErr := make(chan error, 1)
	go func() {
		enableErr <- s.enableAsyncReplication(ctx, cfg)
	}()

	// Assert the .ht file is consumed while RLock is still held.
	// With old code (Lock() before disk I/O): the enableAsyncReplication goroutine
	// blocks immediately on Lock() → tryLoadHashtreeFromDisk never runs → file not
	// consumed → this assertion times out.
	// With new code (disk I/O before Lock()): tryLoadHashtreeFromDisk runs freely →
	// file consumed → assertion passes.
	require.Eventually(t, func() bool {
		return len(htFilesInDir(t, htPath)) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"tryLoadHashtreeFromDisk must run while RLock is held: disk I/O must be outside the write lock")

	// Release the RLock so the write lock can be acquired and init can complete.
	release()

	require.NoError(t, <-enableErr)

	// Hashtree must be loaded from the cached file (fully initialized immediately).
	s.asyncReplicationRWMux.RLock()
	require.True(t, s.hashtreeFullyInitialized,
		"hashtree must be fully initialized from the cached .ht file")
	s.asyncReplicationRWMux.RUnlock()

	require.NoError(t, s.disableAsyncReplication(ctx))
}

// TestConcurrentEnableDisable verifies that rapid concurrent enable calls do
// not deadlock or panic. Each goroutine may win or lose the write-lock race;
// the important invariant is that the shard settles to a consistent state
// (hashtree allocated or nil) without any goroutine leaking.
func TestConcurrentEnableDisable(t *testing.T) {
	ctx := context.Background()
	const class = "ConcEnableDisable"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	cfg := minAsyncReplicationConfig()

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.enableAsyncReplication(ctx, cfg)
		}()
	}
	wg.Wait()

	_ = s.disableAsyncReplication(ctx)
}

// TestDisableRacingInitLeavesNoRegistration is a targeted test for the TOCTOU
// window between the init goroutine's WLock release and Register() returning.
//
// Without the guard the dangerous sequence is:
//  1. Init goroutine: WLock → hashtree≠nil → WUnlock
//  2. disableAsyncReplication: WLock → hashtree=nil → WUnlock →
//     Deregister (no-op: shard not yet in the scheduler's registry)
//  3. Init goroutine: Register(s) → shard added to scheduler registry
//  4. BUG: hashtree is nil but shard is permanently registered
//
// The TOCTOU guard closes this by RLocking after Register returns and calling
// Deregister if hashtree is nil. After asyncRepWg.Wait() serialises the
// goroutine's completion, no shard must remain in the registry.
//
// The test uses the cached-tree path (pre-written .ht file loaded on each
// enable) so the init goroutine is as short as possible (no disk scan),
// maximising the probability of the race window being exercised. Run with
// -race to catch any synchronisation gap the guard does not cover.
func TestDisableRacingInitLeavesNoRegistration(t *testing.T) {
	ctx := context.Background()
	const class = "DisableRacesInit"
	const iterations = 50

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	sched := s.index.asyncReplicationScheduler

	cfg := minAsyncReplicationConfig()

	// Bootstrap: enable → full init scan → disable. disableAsyncReplication
	// persists the initialised tree to a .ht file. Every subsequent enable will
	// hit the cached-tree path (goroutine = lock-check + Register + TOCTOU guard).
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	require.NoError(t, s.disableAsyncReplication(ctx))
	s.asyncRepWg.Wait()

	// awaitWg blocks until asyncRepWg reaches zero or the deadline fires. It
	// covers both the init goroutine and any hashbeat cycle dispatched between
	// Register and Deregister.
	awaitWg := func() {
		t.Helper()
		done := make(chan struct{})
		go func() { s.asyncRepWg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("asyncRepWg did not drain within timeout — goroutine leak or deadlock")
		}
	}

	for range iterations {
		// Call enable then immediately disable. Depending on goroutine scheduling,
		// one of three outcomes is possible:
		//   (A) Init goroutine's first WLock check sees nil → returns early (no register).
		//   (B) Goroutine registers; TOCTOU guard fires → self-deregisters.
		//   (C) Goroutine registers; disableAsyncReplication's Deregister removes it.
		// In every case the invariant must hold: shard absent from registry after Wait().
		require.NoError(t, s.enableAsyncReplication(ctx, cfg))
		require.NoError(t, s.disableAsyncReplication(ctx))

		// Serialise: wait until the init goroutine (and any dispatched cycle) has
		// fully exited, including the TOCTOU guard's Deregister call if it fired.
		awaitWg()

		sched.mu.Lock()
		_, inRegistry := sched.entries[s]
		sched.mu.Unlock()
		require.False(t, inRegistry,
			"shard must not remain registered after disableAsyncReplication raced the init goroutine")
	}
}

// TestEffectiveClampsZeroFields verifies that Effective() clamps integer fields
// that have a minimum of 1 to that minimum when a zero-value config is passed.
// This prevents zero-value AsyncReplicationConfig (e.g. from direct struct
// construction) from reaching propagateObjects with invalid concurrency.
func TestEffectiveClampsZeroFields(t *testing.T) {
	// Zero-value config: all numeric fields are 0.
	cfg := AsyncReplicationConfig{}
	result := cfg.Effective(entreplication.GlobalConfig{})

	assert.Equal(t, minDiffBatchSize, result.diffBatchSize,
		"diffBatchSize must be clamped to minDiffBatchSize")
	assert.Equal(t, minPropagationLimit, result.propagationLimit,
		"propagationLimit must be clamped to minPropagationLimit")
	assert.Equal(t, minPropagationConcurrency, result.propagationConcurrency,
		"propagationConcurrency must be clamped to minPropagationConcurrency")
	assert.Equal(t, minPropagationBatchSize, result.propagationBatchSize,
		"propagationBatchSize must be clamped to minPropagationBatchSize")
}

// TestMayStopAsyncReplicationWaitsForInflightCycle verifies that
// mayStopAsyncReplication does not return while a hashbeat cycle is in-flight.
//
// This is the regression test for the asyncRepWg.Wait() fix: without it,
// mayStopAsyncReplication returns as soon as the scheduler confirms deregistration
// (Deregister ack), leaving an in-flight cycle still running against shard
// resources (store cursors, hashtree) that are about to be torn down.
//
// The test simulates a running cycle by calling asyncRepWg.Add(1) directly.
// With the fix, mayStopAsyncReplication blocks on asyncRepWg.Wait() until Done()
// is called. Without the fix, it returns immediately after Deregister, and the
// timed assertion at the "still held" check fails.
func TestMayStopAsyncReplicationWaitsForInflightCycle(t *testing.T) {
	ctx := context.Background()
	const class = "MayStopWaitsForInflightTest"
	// blockDuration gives the goroutine time to cancel asyncRepCtx, call
	// Deregister, and reach asyncRepWg.Wait(). 100 ms is generous for a few
	// lock operations; if the machine is pathologically slow, the test may
	// produce a false negative (not a false positive).
	const blockDuration = 100 * time.Millisecond

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))

	// Hold asyncRepWg to simulate an in-flight hashbeat cycle.
	s.asyncRepWg.Add(1)

	stopDone := make(chan struct{})
	go func() {
		s.mayStopAsyncReplication()
		close(stopDone)
	}()

	// After blockDuration, mayStopAsyncReplication must still be blocked.
	time.Sleep(blockDuration)
	select {
	case <-stopDone:
		t.Fatal("mayStopAsyncReplication returned while asyncRepWg was still held: " +
			"asyncRepWg.Wait() is missing or ineffective")
	default:
		// Correctly blocked — release the simulated cycle below.
	}

	// Releasing the WG must unblock mayStopAsyncReplication promptly.
	s.asyncRepWg.Done()
	select {
	case <-stopDone:
		// Correctly unblocked after the in-flight cycle finished.
	case <-time.After(5 * time.Second):
		t.Fatal("mayStopAsyncReplication did not complete within 5 s after asyncRepWg.Done()")
	}
}

// TestLoadHashtreeIgnoresTmpFile verifies that tryLoadHashtreeFromDisk ignores
// a stale .ht.tmp file left by a previous crash mid-write. Only committed .ht
// files (produced by a complete write-rename-fsync sequence) may be loaded.
func TestLoadHashtreeIgnoresTmpFile(t *testing.T) {
	ctx := context.Background()
	const class = "LoadHashtreeIgnoresTmpTest"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	// Enable and wait for a full init so that disableAsyncReplication produces a
	// well-formed .ht file we can rename in the next step.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	require.NoError(t, s.disableAsyncReplication(ctx))

	htPath := s.pathHashTree()

	htFiles := htFilesInDir(t, htPath)
	require.Len(t, htFiles, 1, "pre-condition: exactly one .ht file must exist after disable")

	// Simulate a crash mid-rename: rename the committed .ht file to .ht.tmp so
	// the directory contains a well-formed hashtree payload but with the wrong
	// extension. tryLoadHashtreeFromDisk must skip it (only .ht is valid).
	htFile := filepath.Join(htPath, htFiles[0].Name())
	tmpPath := htFile + ".tmp"
	require.NoError(t, os.Rename(htFile, tmpPath))

	// Re-enable: tryLoadHashtreeFromDisk must ignore the .ht.tmp and fall back
	// to a background init scan.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	// The .ht.tmp file must remain on disk: tryLoadHashtreeFromDisk only
	// consumes files it actually loads (extension == ".ht"). If it had
	// incorrectly loaded and consumed this file it would no longer exist.
	_, statErr := os.Stat(tmpPath)
	assert.NoError(t, statErr, ".ht.tmp file must remain untouched (ignored, not consumed)")

	require.NoError(t, s.disableAsyncReplication(ctx))
}
