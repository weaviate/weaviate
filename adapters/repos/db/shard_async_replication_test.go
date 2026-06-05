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

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	entreplication "github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
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

		local, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		local, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		local, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		_, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, limit, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		local, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		local, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
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

		_, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
			0, // asyncCheckpointCutoff: no active checkpoint
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "comparing digests with remote",
			"transport error must be wrapped with the scheduler's prefix")
		assert.Contains(t, err.Error(), "simulated rpc unavailable",
			"original error must be preserved via %w wrapping")
		assert.Empty(t, objs, "no objects must be queued when CompareDigests fails")
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

// TestReconcileAsyncReplicationOnFlagToggle verifies that
// Index.reconcileAsyncReplication starts async replication on a shard that
// skipped initialisation because the global AsyncReplicationDisabled flag was
// set at load time, and stops it again when the flag is re-disabled.
func TestReconcileAsyncReplicationOnFlagToggle(t *testing.T) {
	ctx := context.Background()
	const class = "ReconcileAsyncReplicationTest"

	logger, _ := test.NewNullLogger()

	// Index-level flag the test toggles.
	disabled := configRuntime.NewDynamicValue(true)
	globalConfig := &entreplication.GlobalConfig{AsyncReplicationDisabled: disabled}

	sl, idx := testShard(t, ctx, class, func(i *Index) {
		i.Config.ReplicationFactor = 3
		i.globalreplicationConfig = globalConfig

		// A scheduler whose own disabled flag is permanently set: it accepts
		// Register/Deregister but never dispatches a hashbeat cycle. This keeps
		// the test focused on reconcile's effect on shard state (hashtree +
		// registration) without needing to mock the per-cycle router calls.
		sched, err := NewAsyncReplicationScheduler(ctx,
			entreplication.GlobalConfig{
				AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
				AsyncReplicationDisabled:         configRuntime.NewDynamicValue(true),
			}, nil, logger)
		require.NoError(t, err)
		t.Cleanup(sched.Close)
		i.asyncReplicationScheduler = sched
	})
	s := concreteShard(t, sl)

	// The shard loaded while async replication was globally disabled, so
	// shard_init_lsm skipped init: no hashtree, not registered with the scheduler.
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree, "async replication must be off when loaded while globally disabled")
	s.asyncReplicationRWMux.RUnlock()

	// Re-enable the flag at runtime and reconcile: the already-loaded shard must
	// now start async replication.
	disabled.SetValue(false)
	require.NoError(t, idx.reconcileAsyncReplication(ctx))
	s.asyncReplicationRWMux.RLock()
	require.NotNil(t, s.hashtree, "reconcile must start async replication after the flag is re-enabled")
	s.asyncReplicationRWMux.RUnlock()
	awaitHashtreeInitialized(t, s)

	// Reconcile again with the flag still enabled: idempotent no-op.
	require.NoError(t, idx.reconcileAsyncReplication(ctx))
	s.asyncReplicationRWMux.RLock()
	require.NotNil(t, s.hashtree, "reconcile must be idempotent while the flag stays enabled")
	s.asyncReplicationRWMux.RUnlock()

	// Disable the flag again and reconcile: async replication must stop.
	disabled.SetValue(true)
	require.NoError(t, idx.reconcileAsyncReplication(ctx))
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree, "reconcile must stop async replication after the flag is disabled")
	s.asyncReplicationRWMux.RUnlock()
}

// asyncSchedulerOption returns a testShard index option installing a started
// scheduler whose own disabled flag is permanently set — it accepts
// Register/Deregister but never dispatches a hashbeat cycle.
func asyncSchedulerOption(t *testing.T, ctx context.Context) func(*Index) {
	t.Helper()
	return func(i *Index) {
		logger, _ := test.NewNullLogger()
		sched, err := NewAsyncReplicationScheduler(ctx,
			entreplication.GlobalConfig{
				AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
				AsyncReplicationDisabled:         configRuntime.NewDynamicValue(true),
			}, nil, logger)
		require.NoError(t, err)
		t.Cleanup(sched.Close)
		i.asyncReplicationScheduler = sched
	}
}

func setShardReplicas(t *testing.T, idx *Index, nodes ...string) {
	t.Helper()
	m, ok := idx.schemaReader.(*schemaUC.MockSchemaReader)
	require.True(t, ok, "schemaReader is not a *MockSchemaReader")
	for _, c := range m.ExpectedCalls {
		if c.Method == "ShardReplicas" {
			c.ReturnArguments = mock.Arguments{nodes, error(nil)}
			return
		}
	}
	m.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return(nodes, nil).Maybe()
}

// TestRunHashbeatCycle_SkipsWhileNonTerminalOpForShard pins the cycle's
// in-flight short-circuit: while any non-terminal replication op exists for
// the shard, the cycle must return without invoking the replicator so the CCL
// stays the exclusive catch-up channel. A nil FSM reader is treated as "no
// in-flight ops" — the fall-through path for tests that don't plumb one.
func TestRunHashbeatCycle_SkipsWhileNonTerminalOpForShard(t *testing.T) {
	ctx := context.Background()
	const class = "HashbeatInflightCheck"

	sh, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	shardName := sh.Name()
	setShardReplicas(t, idx, "node1", "node2")

	// Swap in a fresh FSM reader so we control the predicate this cycle reads.
	fsmMock := replicationTypes.NewMockReplicationFSMReader(t)
	saved := idx.replicationFSMReader
	idx.replicationFSMReader = fsmMock
	defer func() { idx.replicationFSMReader = saved }()

	// Resolve the concrete *Shard to invoke runHashbeatCycle directly.
	concrete, ok := sh.(*Shard)
	require.True(t, ok, "expected a concrete *Shard from testShard")

	cfg := idx.AsyncReplicationConfig()

	t.Run("non-terminal op short-circuits the cycle", func(t *testing.T) {
		// Cycle must consult the FSM, see the in-flight op, and return without
		// calling the replicator (asserted implicitly via NewMockReplicationFSMReader's
		// Cleanup: any unexpected call would fail the mock).
		call := fsmMock.EXPECT().HasOngoingReplication(class, shardName).Return(true).Once()
		defer call.Unset()
		propagated, err := concrete.runHashbeatCycle(ctx, cfg)
		require.NoError(t, err)
		require.False(t, propagated, "no objects must be propagated while an op is in flight")
	})

	t.Run("nil FSM reader is treated as no in-flight ops", func(t *testing.T) {
		// Tests that never plumb the reader should not have their hashbeat
		// gated by an absent FSM — they fall through to the normal gate.
		idx.replicationFSMReader = nil
		defer func() { idx.replicationFSMReader = fsmMock }()

		// We don't need to drive a full diff; we just want to confirm we got
		// past the FSM short-circuit. With no peers configured, the cycle
		// exits cleanly via the existing "no diff found" path. The mock would
		// catch any FSM call here (none allowed; we restored the nil reader).
		_, _ = concrete.runHashbeatCycle(ctx, cfg)
	})
}

func TestReconcileDoesNotForceLoadUnloadedShard(t *testing.T) {
	ctx := context.Background()
	const class = "ReconcileNoForceLoad"

	_, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	setShardReplicas(t, idx, "node1", "node2")

	// Register an unloaded lazy shard under a fresh name (disableLazyLoad=false).
	const lazyName = "lazy-cold-shard"
	sl, err := idx.initShard(ctx, lazyName, &models.Class{Class: class}, nil, false, false)
	require.NoError(t, err)
	lazy, ok := sl.(*LazyLoadShard)
	require.True(t, ok, "expected a *LazyLoadShard")
	require.False(t, lazy.isLoaded(), "precondition: shard must start unloaded")
	idx.shards.Store(lazyName, sl)

	require.NoError(t, idx.ReconcileAsyncReplicationForShard(ctx, lazyName))
	require.False(t, lazy.isLoaded(), "reconcile must not force-load an unloaded shard")
}

// TestDBReconcileAsyncReplicationAggregatesErrors verifies that
// DB.ReconcileAsyncReplication walks every index, and that a reconcile failure
// on one index (here: a misconfigured index with no scheduler) is aggregated
// into the returned error without preventing the other index from reconciling.
func TestDBReconcileAsyncReplicationAggregatesErrors(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	// Index-level flag shared by both indices; both load with it disabled.
	disabled := configRuntime.NewDynamicValue(true)
	globalConfig := &entreplication.GlobalConfig{AsyncReplicationDisabled: disabled}

	// Healthy index: RF > 1 with a scheduler installed → reconcile succeeds.
	healthySL, healthy := testShard(t, ctx, "ReconcileHealthy", func(i *Index) {
		i.Config.ReplicationFactor = 3
		i.globalreplicationConfig = globalConfig
		asyncSchedulerOption(t, ctx)(i)
	})
	// Broken index: RF > 1 but no scheduler → enableAsyncReplication errors.
	_, broken := testShard(t, ctx, "ReconcileBroken", func(i *Index) {
		i.Config.ReplicationFactor = 3
		i.globalreplicationConfig = globalConfig
	})

	db := &DB{
		logger:  logger,
		indices: map[string]*Index{healthy.ID(): healthy, broken.ID(): broken},
	}

	disabled.SetValue(false)
	err := db.ReconcileAsyncReplication(ctx)
	require.Error(t, err, "a per-index reconcile failure must surface as an error")
	require.Contains(t, err.Error(), "1 of 2",
		"the error must report how many indices failed")

	// The broken index must not have blocked the healthy one.
	healthyShard := concreteShard(t, healthySL)
	healthyShard.asyncReplicationRWMux.RLock()
	require.NotNil(t, healthyShard.hashtree,
		"the healthy index must still be reconciled despite the other index failing")
	healthyShard.asyncReplicationRWMux.RUnlock()
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

	// mayStopAsyncReplication is the legitimate .ht producer (shutdown path);
	// disableAsyncReplication is the runtime path and does not dump.
	s.mayStopAsyncReplication()
	require.Len(t, htFilesInDir(t, htPath), 1,
		"pre-condition: mayStopAsyncReplication must have written a .ht file")

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

// TestEnableAsyncReplicationRescansAfterRuntimeDisable pins the silent
// replica-inconsistency bug from PR #11214: disableAsyncReplication is the
// runtime path (shard stays live and serves writes), so any hashtree it
// persists goes stale immediately. Before the fix it dumped a .ht and
// enableAsyncReplication loaded that snapshot verbatim — CollectShardDifferences
// then returned ErrNoDiffFound forever and async repair silently abandoned
// the disabled-window divergence. (mayStopAsyncReplication remains the only
// safe .ht producer; covered by TestMayStopAsyncReplicationDumpsHashtreeOnSuccess.)
//
// Falsifiable assertion: after enable → disable → write → re-enable, the
// hashtree root must differ from its pre-disable value. With the bug the
// cached .ht restores the pre-write root and the assertion fires; with the
// fix re-enable rebuilds from a full scan and the new object shifts the root.
func TestEnableAsyncReplicationRescansAfterRuntimeDisable(t *testing.T) {
	ctx := context.Background()
	const class = "AsyncRescanAfterRuntimeDisable"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	// Seed: three flushed objects so the first init has non-trivial state.
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))

	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootBeforeDisable := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.NotEqual(t, hashtree.Digest{}, rootBeforeDisable,
		"sanity: seeded hashtree must have a non-zero root")

	// Runtime kill-switch flipped to disabled. Shard stays live.
	require.NoError(t, s.disableAsyncReplication(ctx))

	// disableAsyncReplication must not leave a .ht behind — the shard will
	// serve writes from here on, so any snapshot is stale.
	require.Empty(t, htFilesInDir(t, s.pathHashTree()),
		"runtime disableAsyncReplication must not persist the hashtree; only "+
			"mayStopAsyncReplication (shutdown) may write a .ht")

	// Write a fourth object while async replication is disabled. With the
	// bug, this write is invisible to any future hashtree comparison.
	const uuidWhileDisabled = strfmt.UUID("44444444-4444-4444-4444-444444444444")
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidWhileDisabled, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))

	// Flip the kill-switch back to false. The shard must rebuild the hashtree
	// from the current object store (a full scan) — not trust a leftover .ht.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootAfterReEnable := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	require.NotEqual(t, rootBeforeDisable, rootAfterReEnable,
		"hashtree must reflect the disabled-window write; if it does not, a "+
			"stale snapshot was trusted and divergence is invisible to repair")

	require.NoError(t, s.disableAsyncReplication(ctx))
}

// TestShardLoadDiscardsStaleHashtreeWhenAsyncDisabled pins the second arm of
// the stale-cache bug: a node booted with ASYNC_REPLICATION_DISABLED=true
// after a previous async-enabled shutdown must not let the leftover .ht
// survive into a later runtime re-enable — the shard will serve writes while
// async is off, invalidating the snapshot. shard_init_lsm.go discards the
// .ht in this branch; this test exercises the helper it relies on.
func TestShardLoadDiscardsStaleHashtreeWhenAsyncDisabled(t *testing.T) {
	ctx := context.Background()
	const class = "DiscardStaleHashtreeOnDisabledLoad"

	sl, _ := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })

	cfg := minAsyncReplicationConfig()

	// Produce a legitimate .ht the way a graceful shutdown would: enable async,
	// let the init scan complete, then mayStopAsyncReplication serialises the
	// in-memory tree to disk.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	s.mayStopAsyncReplication()
	require.Len(t, htFilesInDir(t, s.pathHashTree()), 1,
		"pre-condition: mayStopAsyncReplication must have produced a .ht")

	// Simulate the shard-init path running with async replication disabled,
	// i.e. the else-branch of shard_init_lsm.go. The .ht must be discarded
	// so a later runtime enable cannot load a stale snapshot.
	require.NoError(t, s.removePersistedHashtree())

	require.Empty(t, htFilesInDir(t, s.pathHashTree()),
		"shard-load with async replication disabled must discard the "+
			"persisted .ht; otherwise a later runtime enable would load a "+
			"snapshot that the shard's disabled-window writes have invalidated")
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

	// Enable and wait for a full init so that mayStopAsyncReplication will
	// produce a .ht file we can later load.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	htPath := s.pathHashTree()

	// mayStopAsyncReplication is the legitimate .ht producer; disable is the
	// runtime path and does not dump.
	s.mayStopAsyncReplication()
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

	// Bootstrap: enable → full init scan → mayStop persists a .ht. The first
	// loop iteration's enable hits the cached-tree path (short goroutine =
	// lock-check + Register + TOCTOU guard); iterations 2..N go through
	// disableAsyncReplication, which does not dump, so they exercise the same
	// TOCTOU race against the longer full-scan path. Both are worth covering.
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	s.mayStopAsyncReplication()
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

	// Enable, await full init, then mayStop persists a well-formed .ht we
	// can rename in the next step. (disableAsyncReplication does not dump.)
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	s.mayStopAsyncReplication()

	htPath := s.pathHashTree()

	htFiles := htFilesInDir(t, htPath)
	require.Len(t, htFiles, 1, "pre-condition: exactly one .ht file must exist after mayStop")

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
