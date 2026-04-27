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

// fixedDigestsClient is a test double whose DigestObjectsInRange always
// returns a pre-configured slice of digests, letting tests control what the
// "remote" shard appears to hold.
type fixedDigestsClient struct {
	FakeReplicationClient
	digests []routerTypes.RepairResponse
}

func (c *fixedDigestsClient) DigestObjectsInRange(
	_ context.Context, _, _, _ string, _, _ strfmt.UUID, _ int,
) ([]routerTypes.RepairResponse, error) {
	return c.digests, nil
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
		assert.Equal(t, 0, remote, "remote must not be queried when no on-disk objects found")
		assert.Empty(t, objs)
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

		local, remote, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, 100, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, 2, local)
		assert.Equal(t, 2, remote, "remote must return the same number of digests")
		assert.Empty(t, objs, "remote is up-to-date → nothing to propagate")
		_ = idx
	})

	// LimitEnforcement verifies that the propagation limit caps the number of
	// objects queued. The default FakeReplicationClient returns empty digests for
	// DigestObjectsInRange (remote has nothing), so every local object appears
	// missing and is queued for propagation.
	t.Run("LimitEnforcement", func(t *testing.T) {
		const limit = 1
		// Default FakeReplicationClient.DigestObjectsInRange returns nil → remote empty.
		sl, idx := testShard(t, ctx, class, withReplicationClient(t, &FakeReplicationClient{}))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, tsFarPast)))

		s := concreteShard(t, sl)
		require.NoError(t, s.store.FlushMemtables(ctx))
		cfg := fullRangeConfig(100)

		_, _, objs, err := s.objectsToPropagateWithinRange(
			ctx, cfg, "http://fake", "node2", 0, 1, limit, nil,
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(objs), limit,
			"number of queued propagations must not exceed the limit")
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
		initShieldCPUEveryN:       1,
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

	sl, _ := testShard(t, ctx, class)
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
	require.NoError(t, s.enableAsyncReplication(context.Background(), cfg))
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
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
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
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
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
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
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

	sl, _ := testShard(t, ctx, class)
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

	sl, _ := testShard(t, ctx, class)
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

	sl, _ := testShard(t, ctx, class)
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

// TestConcurrentEnableAndFlushCallback verifies that concurrent calls to
// enableAsyncReplication and updateHashtreeOnFlush do not produce data races.
// Both functions acquire asyncReplicationRWMux exclusively; running them
// together under the race detector validates correct lock usage end-to-end.
func TestConcurrentEnableAndFlushCallback(t *testing.T) {
	ctx := context.Background()
	const class = "ConcEnableFlush"

	sl, _ := testShard(t, ctx, class)
	s := concreteShard(t, sl)
	cfg := minAsyncReplicationConfig()

	// No-op callbacks: we care about lock correctness, not hashtree content.
	noopForEach := func(_ func(key, value []byte, tombstone bool)) {}
	noopLookup := func(_ []byte) ([]byte, bool) { return nil, false }

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.enableAsyncReplication(ctx, cfg)
		}()
	}
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.updateHashtreeOnFlush(noopForEach, noopLookup)
		}()
	}
	wg.Wait()

	_ = s.disableAsyncReplication(ctx)
}
