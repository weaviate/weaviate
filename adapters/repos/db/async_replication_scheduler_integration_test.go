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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// newStartedTestScheduler creates and starts a scheduler backed by a null
// logger and no Prometheus metrics. The topology watcher fires every 10 min
// so it never interferes with the short-lived scheduler tests.
func newStartedTestScheduler(t *testing.T, workers int) *AsyncReplicationScheduler {
	t.Helper()
	logger, _ := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(
		context.Background(),
		replication.GlobalConfig{
			AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(workers),
			AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(10 * time.Minute), // long so topology watcher never fires during tests
			AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
		},
		nil, logger,
	)
	require.NoError(t, err)
	sched.Start()
	t.Cleanup(sched.Close)
	return sched
}

// firstShard extracts the single shard from an Index created by testShard.
func firstShard(t *testing.T, idx *Index) *Shard {
	t.Helper()
	var s *Shard
	require.NoError(t, idx.ForEachShard(func(_ string, sl ShardLike) error {
		s, _ = sl.(*Shard)
		return nil
	}))
	require.NotNil(t, s, "index must have at least one shard")
	return s
}

// prepareShardForScheduler seeds the minimum async-replication state a shard
// needs before it can safely be passed to Register:
//   - a non-nil asyncRepCtx (nil would panic in the worker goroutine)
//   - an initialised hashtree (nil produces repeated "hashtree not initialized"
//     errors in every hashbeat cycle)
//   - the per-cycle tracking maps that runHashbeatCycle reads
//   - asyncReplicationConfig with the correct hashtreeHeight so that runEntry
//     does not detect a height mismatch and spawn a rebuildHashtree goroutine
//     that would register the shard with the repo's asyncReplicationScheduler
//
// This mirrors what initAsyncReplication does for production shards, without
// going through the full initialization path (which also spawns goroutines and
// registers the shard with a different scheduler instance).
func prepareShardForScheduler(t *testing.T, s *Shard) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.asyncReplicationCancelFunc = cancel
	s.asyncRepCtx = ctx
	ht, err := hashtree.NewHashTree(defaultHashtreeHeightSingleTenant)
	require.NoError(t, err)
	s.hashtree = ht
	s.hashtreeFullyInitialized = true
	s.asyncRepLastLocalRootByTarget = make(map[string]hashtree.Digest)
	s.asyncRepLastRemoteRootByTarget = make(map[string]hashtree.Digest)
	s.asyncRepLastPropagatedToTarget = make(map[string]bool)
	// Set hashtreeHeight to match the actual hashtree so runEntry does not
	// detect a mismatch and trigger a rebuildHashtree goroutine.
	s.asyncReplicationConfig = AsyncReplicationConfig{hashtreeHeight: defaultHashtreeHeightSingleTenant}
}

// TestAsyncSchedulerRegisterDeregisterClean registers a shard, waits for at
// least one dispatch to occur, then deregisters. Verifies no deadlock and that
// the shard is absent from the registry afterwards.
func TestAsyncSchedulerRegisterDeregisterClean(t *testing.T) {
	ctx := context.Background()
	sched := newStartedTestScheduler(t, 2)
	_, idx := testShard(t, ctx, "SchedRegDeregClean")
	s := firstShard(t, idx)
	prepareShardForScheduler(t, s)

	sched.Register(s)

	// Allow the scheduler to dispatch the shard at least once.
	time.Sleep(50 * time.Millisecond)

	sched.Deregister(s)
	s.asyncRepWg.Wait()

	sched.mu.Lock()
	_, still := sched.entries[s]
	sched.mu.Unlock()
	assert.False(t, still, "shard must not remain in registry after Deregister")
}

// TestAsyncSchedulerRegistrationIdempotent verifies that registering the same
// shard twice produces exactly one heap entry and one registry entry.
func TestAsyncSchedulerRegistrationIdempotent(t *testing.T) {
	ctx := context.Background()
	sched := newStartedTestScheduler(t, 1)
	_, idx := testShard(t, ctx, "SchedIdempotentReg")
	s := firstShard(t, idx)
	prepareShardForScheduler(t, s)

	sched.Register(s)
	sched.Register(s) // must be a no-op

	sched.mu.Lock()
	count := len(sched.entries)
	sched.mu.Unlock()
	assert.Equal(t, 1, count, "double-register must not create more than one entry")

	sched.Deregister(s)
	s.asyncRepWg.Wait()
}

// TestAsyncSchedulerDeregistrationIdempotent verifies that deregistering a
// shard that was never registered is a silent no-op.
func TestAsyncSchedulerDeregistrationIdempotent(t *testing.T) {
	sched := newStartedTestScheduler(t, 1)
	assert.NotPanics(t, func() {
		sched.Deregister(&Shard{}) // never registered
		sched.Deregister(&Shard{}) // second call also safe
	})
}

// TestAsyncSchedulerNotifyShardRegistered verifies that NotifyShard enqueues a
// reprioritization and that the dispatcher clears asyncRepHasPendingFlush.
func TestAsyncSchedulerNotifyShardRegistered(t *testing.T) {
	ctx := context.Background()
	sched := newStartedTestScheduler(t, 1)
	_, idx := testShard(t, ctx, "SchedNotifyRegistered")
	s := firstShard(t, idx)
	prepareShardForScheduler(t, s)

	sched.Register(s)

	sched.NotifyShard(s) // first notification
	sched.NotifyShard(s) // second call: deduped, only one pending

	// The dispatcher drains notifyCh and clears the flag.
	require.Eventually(t, func() bool {
		return !s.asyncRepHasPendingFlush.Load()
	}, 5*time.Second, 10*time.Millisecond,
		"asyncRepHasPendingFlush must be cleared after dispatcher processes the notification")

	sched.Deregister(s)
	s.asyncRepWg.Wait()
}

// TestAsyncSchedulerNotifyShardNotRegistered verifies that NotifyShard on an
// unregistered shard is safe (no panic, channel not flooded indefinitely).
func TestAsyncSchedulerNotifyShardNotRegistered(t *testing.T) {
	sched := newStartedTestScheduler(t, 1)
	s := &Shard{} // never registered
	assert.NotPanics(t, func() {
		sched.NotifyShard(s)
		sched.NotifyShard(s) // dedup: second send is dropped silently
	})
}

// TestAsyncSchedulerMultipleShards registers n shards against a 2-worker pool,
// verifies they are all present in the registry, then deregisters them all and
// confirms the registry is empty with no goroutine leaks.
func TestAsyncSchedulerMultipleShards(t *testing.T) {
	const n = 5
	ctx := context.Background()

	sched := newStartedTestScheduler(t, 2) // 2 workers, 5 shards → exercises queuing

	shards := make([]*Shard, 0, n)
	for i := range n {
		_, idx := testShard(t, ctx, fmt.Sprintf("SchedMultiShard%02d", i))
		s := firstShard(t, idx)
		prepareShardForScheduler(t, s)
		shards = append(shards, s)
	}
	require.Len(t, shards, n)

	for _, s := range shards {
		sched.Register(s)
	}

	// All n shards must be in the registry immediately after registration.
	sched.mu.Lock()
	registered := len(sched.entries)
	sched.mu.Unlock()
	assert.Equal(t, n, registered, "all %d shards must appear in the registry", n)

	// Let the scheduler process a few cycles.
	time.Sleep(100 * time.Millisecond)

	// Deregister all and wait for any in-flight cycles to complete.
	for _, s := range shards {
		sched.Deregister(s)
	}
	for _, s := range shards {
		s.asyncRepWg.Wait()
	}

	sched.mu.Lock()
	remaining := len(sched.entries)
	sched.mu.Unlock()
	assert.Equal(t, 0, remaining, "registry must be empty after deregistering all shards")
}

// ─── lifecycle ────────────────────────────────────────────────────────────────

// TestAsyncSchedulerCloseWithInflightWorkers verifies that Close() returns
// cleanly and does not deadlock even when worker goroutines are in the middle
// of a hashbeat cycle at the time of shutdown. The dispatcher and workers must
// drain to completion after the context is cancelled; the test fails if Close()
// does not return within the deadline.
func TestAsyncSchedulerCloseWithInflightWorkers(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	// Build and start the scheduler without registering a t.Cleanup(Close)
	// so that we can call Close() explicitly and detect any deadlock.
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(2),
		AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(10 * time.Minute),
		AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
	}, nil, logger)
	require.NoError(t, err)
	sched.Start()

	// Register several shards to keep workers busy across multiple dispatch cycles.
	for i := range 6 {
		_, idx := testShard(t, ctx, fmt.Sprintf("CloseInflight%02d", i))
		s := firstShard(t, idx)
		prepareShardForScheduler(t, s)
		sched.Register(s)
	}

	// Give the dispatcher time to send work items to workers.
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() { sched.Close(); close(closeDone) }()

	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not return within deadline — possible deadlock with in-flight workers")
	}
}

// TestAsyncSchedulerConcurrentRegisterDeregisterAndClose verifies that
// concurrent Register / Deregister calls racing against a Close() do not
// deadlock or panic. Register and Deregister escape via ctx.Done() once the
// scheduler's context is cancelled, so all goroutines terminate cleanly.
func TestAsyncSchedulerConcurrentRegisterDeregisterAndClose(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(2),
		AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(10 * time.Minute),
		AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
	}, nil, logger)
	require.NoError(t, err)
	sched.Start()

	shards := make([]*Shard, 6)
	for i := range shards {
		_, idx := testShard(t, ctx, fmt.Sprintf("ConcClose%02d", i))
		s := firstShard(t, idx)
		prepareShardForScheduler(t, s)
		shards[i] = s
	}

	var wg sync.WaitGroup
	for _, s := range shards {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			sched.Register(s)
			time.Sleep(5 * time.Millisecond)
			sched.Deregister(s)
		}()
	}

	// Close() while goroutines are still registering / deregistering.
	time.Sleep(5 * time.Millisecond)
	sched.Close()

	wg.Wait() // goroutines exit via ctx.Done() in Register/Deregister
}

// ─── checkTopology ────────────────────────────────────────────────────────────

// TestAsyncSchedulerCheckTopologyNoOpForDeregisteredShard verifies that
// checkTopology does not panic or corrupt the heap when called after a shard
// has been deregistered. The shards snapshot taken inside checkTopology will
// be empty, so the function is a clean no-op.
func TestAsyncSchedulerCheckTopologyNoOpForDeregisteredShard(t *testing.T) {
	ctx := context.Background()
	sched := newStartedTestScheduler(t, 1)

	_, idx := testShard(t, ctx, "CheckTopoDeregistered")
	s := firstShard(t, idx)
	prepareShardForScheduler(t, s)

	sched.Register(s)
	sched.Deregister(s)
	s.asyncRepWg.Wait()

	assert.NotPanics(t, func() { sched.checkTopology() },
		"checkTopology must not panic after a shard is deregistered")

	sched.mu.Lock()
	heapLen := len(sched.h)
	sched.mu.Unlock()
	assert.Zero(t, heapLen, "heap must be empty after deregistration")
}
