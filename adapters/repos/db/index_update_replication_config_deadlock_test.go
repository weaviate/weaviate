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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Like newAddPropertyLazyFixture but without the Shutdown cleanup: a reproduced
// deadlock would make a deferred Shutdown hang the test binary forever.
func newReplConfigDeadlockFixture(t *testing.T, className string) (*DB, *Index) {
	t.Helper()
	ctx := testCtx()
	repo, migrator, schemaGetter := newLazyLoadRepo(t, singleShardState())

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
	}
	require.NoError(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)
	return repo, index
}

func soleColdShard(t *testing.T, index *Index) *LazyLoadShard {
	t.Helper()
	var lazy *LazyLoadShard
	index.shards.Range(func(name string, s ShardLike) error {
		ls, ok := s.(*LazyLoadShard)
		require.True(t, ok, "shard %q should be a LazyLoadShard", name)
		lazy = ls
		return nil
	})
	require.NotNil(t, lazy)
	require.False(t, lazy.isLoaded())
	return lazy
}

// deadlockStacks filters the dump to the cycle's goroutines; full dump if none match.
func deadlockStacks(full string) string {
	var kept []string
	for _, g := range strings.Split(full, "\n\n") {
		if strings.Contains(g, "updateReplicationConfig") ||
			strings.Contains(g, "reconcileAsyncReplication") ||
			strings.Contains(g, "LazyLoadShard") ||
			strings.Contains(g, "initNonVector") {
			kept = append(kept, g)
		}
	}
	if len(kept) == 0 {
		return full
	}
	return strings.Join(kept, "\n\n")
}

// Both operations succeed when they don't overlap — pins any deadlock-test
// failure on the interleaving rather than the fixture.
func TestUpdateReplicationConfig_SequentialWithLazyShard(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigSequential")
	lazy := soleColdShard(t, index)

	require.NoError(t, lazy.Load(ctx))
	require.NoError(t, index.updateReplicationConfig(ctx, &models.ReplicationConfig{
		Factor: 1,
	}))
	require.NoError(t, repo.Shutdown(context.Background()))
}

// gateAllocChecker parks Load at CheckMappingAndReserve — inside Load's
// critical section (shard mutex held, before its config read) — giving the
// test a deterministic sync point.
type gateAllocChecker struct {
	entered chan struct{} // closed when Load reaches the gate
	release chan struct{} // closed by the test to let Load continue
}

func (g gateAllocChecker) CheckAlloc(int64) error { return nil }

func (g gateAllocChecker) CheckMappingAndReserve(int64, int) error {
	close(g.entered)
	<-g.release
	return nil
}

func (g gateAllocChecker) Refresh(bool) {}

const (
	deadlockSyncTimeout = 10 * time.Second
	deadlockTimeout     = 15 * time.Second
)

// startGatedLoad parks lazy's Load inside its critical section (shard mutex held, before its config reads) and returns the gate plus Load's done channel.
func startGatedLoad(t *testing.T, ctx context.Context, lazy *LazyLoadShard) (gateAllocChecker, chan error) {
	t.Helper()
	gate := gateAllocChecker{entered: make(chan struct{}), release: make(chan struct{})}
	lazy.memMonitor = gate

	loadDone := make(chan error, 1)
	go func() { loadDone <- lazy.Load(ctx) }()

	select {
	case <-gate.entered:
	case <-time.After(deadlockSyncTimeout):
		t.Fatal("Load never reached the gate inside its critical section — the fixture no longer exercises the interleaving")
	}
	return gate, loadDone
}

// requireBothComplete fails with filtered goroutine stacks when either channel stays blocked past the deadlock timeout.
func requireBothComplete(t *testing.T, what string, loadDone, opDone chan error) {
	t.Helper()
	loadOK, opOK := false, false
	timeout := time.After(deadlockTimeout)
	for !loadOK || !opOK {
		select {
		case err := <-loadDone:
			require.NoError(t, err)
			loadOK = true
		case err := <-opDone:
			require.NoError(t, err)
			opOK = true
		case <-timeout:
			buf := make([]byte, 1<<22)
			stacks := string(buf[:runtime.Stack(buf, true)])
			t.Fatalf("deadlock: %s and LazyLoadShard.Load wedged for %s (load done: %v, op done: %v).\n\ninvolved goroutines:\n\n%s",
				what, deadlockTimeout, loadOK, opOK, deadlockStacks(stacks))
		}
	}
}

// Pins the ABBA deadlock that wedged the RAFT FSM in prod (the UpdateClass
// apply never returns, so raft.Shutdown hangs on runFSM):
//
//	updateReplicationConfig: holds replicationConfigLock (W) -> wants LazyLoadShard.mutex (isLoaded)
//	LazyLoadShard.Load:      holds LazyLoadShard.mutex      -> wants replicationConfigLock (R via initNonVector)
//
// The interleaving is forced deterministically: Load is parked at the gate
// with the shard mutex held; a test-held read lock queues the updater's write
// (observable — a pending writer fails TryRLock); releasing both lets Load's
// config read collide with the fan-out. Every sync point fails the test loudly
// if it is not reached, so the test cannot pass without exercising the cycle.
func TestUpdateReplicationConfig_DeadlocksAgainstLazyShardLoad(t *testing.T) {
	ctx := context.Background()

	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigDeadlock")
	lazy := soleColdShard(t, index)

	gate, loadDone := startGatedLoad(t, ctx, lazy)

	// Queue the updater behind a test-held read lock so its write-lock request
	// is observably pending before Load is released.
	index.replicationConfigLock.RLock()

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{
			Factor: 1,
		})
	}()

	writerQueued := false
	for deadline := time.Now().Add(deadlockSyncTimeout); time.Now().Before(deadline); {
		if !index.replicationConfigLock.TryRLock() {
			writerQueued = true // a pending writer blocks new readers
			break
		}
		index.replicationConfigLock.RUnlock()
		runtime.Gosched()
	}
	if !writerQueued {
		index.replicationConfigLock.RUnlock()
		t.Fatal("updateReplicationConfig never queued for the config write lock")
	}
	index.replicationConfigLock.RUnlock() // writer acquires the lock now
	close(gate.release)                   // Load proceeds into its config read

	requireBothComplete(t, "updateReplicationConfig", loadDone, updateDone)
	require.NoError(t, repo.Shutdown(context.Background()))
}

// TestReconcileAsyncReplication_NoDeadlockAgainstLazyShardLoad pins the updater deadlock's sibling: reconcile must not hold replicationConfigLock across the fan-out, whose isLoaded() blocks on a mid-load shard's mutex.
func TestReconcileAsyncReplication_NoDeadlockAgainstLazyShardLoad(t *testing.T) {
	ctx := context.Background()

	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigReconcileDeadlock")
	lazy := soleColdShard(t, index)

	gate, loadDone := startGatedLoad(t, ctx, lazy)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- index.reconcileAsyncReplication(ctx) }()

	// Completing here would mean the fan-out no longer blocks on the parked shard's mutex.
	select {
	case <-reconcileDone:
		t.Fatal("reconcile completed while Load held the shard mutex — the fan-out no longer touches it")
	case <-time.After(200 * time.Millisecond):
	}

	close(gate.release) // Load proceeds into its config read

	requireBothComplete(t, "reconcileAsyncReplication", loadDone, reconcileDone)
	require.NoError(t, repo.Shutdown(context.Background()))
}

// resumeLockScopeProbe records whether replicationConfigLock was free and the apply lock held while rebuildAsyncReplicationFromScratch ran.
type resumeLockScopeProbe struct {
	ShardLike
	asyncReplicationController
	idx       *Index
	rebuilt   bool
	lockFree  bool
	applyHeld bool
}

func (p *resumeLockScopeProbe) resumeMaintenanceCycles(context.Context, string) error { return nil }

func (p *resumeLockScopeProbe) rebuildAsyncReplicationFromScratch(context.Context, bool, AsyncReplicationConfig) error {
	p.rebuilt = true
	if p.idx.replicationConfigLock.TryLock() {
		p.idx.replicationConfigLock.Unlock()
		p.lockFree = true
	}
	if p.idx.asyncReplicationApplyLock.TryLock() {
		p.idx.asyncReplicationApplyLock.Unlock()
	} else {
		p.applyHeld = true
	}
	return nil
}

// TestResumeAfterAbortedOffload_ConfigLockNotHeldAcrossRebuild: holding the config lock across the wrapper call recreates the lazy-load deadlock cycle.
func TestResumeAfterAbortedOffload_ConfigLockNotHeldAcrossRebuild(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ResumeLockScope")

	probe := &resumeLockScopeProbe{idx: index}
	index.shards.Store("probe-shard", probe)

	require.NoError(t, index.resumeAfterAbortedOffload(ctx, "probe-shard"))
	require.True(t, probe.rebuilt, "the rebuild must run for a loaded shard")
	require.True(t, probe.lockFree, "replicationConfigLock must not be held across rebuildAsyncReplicationFromScratch")
	require.True(t, probe.applyHeld, "asyncReplicationApplyLock must be held across rebuildAsyncReplicationFromScratch")

	_, _ = index.shards.LoadAndDelete("probe-shard")
	require.NoError(t, repo.Shutdown(context.Background()))
}

// gatedApplyProbe parks the first enable before recording, modelling the in-place config assignment a real shard performs under its mutex.
type gatedApplyProbe struct {
	ShardLike
	asyncReplicationController
	entered  chan struct{}
	release  chan struct{}
	parkOnce sync.Once
	mu       sync.Mutex
	ops      []string
}

func (p *gatedApplyProbe) enableAsyncReplication(_ context.Context, _ AsyncReplicationConfig) error {
	p.parkOnce.Do(func() { close(p.entered); <-p.release })
	p.mu.Lock()
	p.ops = append(p.ops, "enable")
	p.mu.Unlock()
	return nil
}

func (p *gatedApplyProbe) disableAsyncReplication(context.Context) error {
	p.mu.Lock()
	p.ops = append(p.ops, "disable")
	p.mu.Unlock()
	return nil
}

func (p *gatedApplyProbe) hasActiveAsyncReplicationTargetOverrides() bool { return false }

// TestStaleFanOutCannotClobberNewerConfig: a fan-out parked mid-apply must finish before a newer config's fan-out, so the newest decision is always the last applied.
func TestStaleFanOutCannotClobberNewerConfig(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigStaleClobber")

	index.asyncReplicationScheduler = newSchedulerForUnitTest(t)
	index.replicationConfigLock.Lock()
	index.Config.ReplicationFactor = 3
	index.replicationConfigLock.Unlock()

	probe := &gatedApplyProbe{entered: make(chan struct{}), release: make(chan struct{})}
	index.shards.Store("probe-shard", probe)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- index.reconcileAsyncReplication(ctx) }()
	select {
	case <-probe.entered:
	case <-time.After(deadlockSyncTimeout):
		t.Fatal("the reconcile fan-out never reached the probe shard")
	}

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{Factor: 1})
	}()
	// Wait for the updater to publish the new factor: its fan-out is then at or
	// behind the apply lock (serialized) or already past it (the regression this
	// test pins) — no wall-clock guessing.
	require.Eventually(t, func() bool {
		index.replicationConfigLock.RLock()
		defer index.replicationConfigLock.RUnlock()
		return index.Config.ReplicationFactor == 1
	}, deadlockSyncTimeout, time.Millisecond, "updateReplicationConfig never published the new factor")

	close(probe.release)
	require.NoError(t, <-reconcileDone)
	require.NoError(t, <-updateDone)

	probe.mu.Lock()
	ops := append([]string(nil), probe.ops...)
	probe.mu.Unlock()
	require.NotEmpty(t, ops)
	require.Equal(t, "disable", ops[len(ops)-1],
		"the newest config (factor 1 ⇒ disable) must be the last applied — a stale fan-out may not clobber it")

	_, _ = index.shards.LoadAndDelete("probe-shard")
	require.NoError(t, repo.Shutdown(context.Background()))
}

// TestReconcileForShard_NoDeadlockAgainstLazyLoadAndConfigWriter pins the 3-way ABBA: the per-shard reconcile holding the config read lock across the apply wedges against a shard re-loading after an unload and a queued config writer (writer-priority RWMutex blocks the loader's read).
func TestReconcileForShard_NoDeadlockAgainstLazyLoadAndConfigWriter(t *testing.T) {
	ctx := context.Background()

	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigReconcileShardABBA")
	lazy := soleColdShard(t, index)
	var shardName string
	require.NoError(t, index.shards.Range(func(name string, _ ShardLike) error { shardName = name; return nil }))

	// Load fully so Loaded() passes, then swap the shard to mid-load underneath the parked reconcile.
	require.NoError(t, lazy.Load(ctx))

	index.replicationConfigLock.Lock()

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- index.ReconcileAsyncReplicationForShard(ctx, shardName) }()

	select {
	case <-reconcileDone:
		index.replicationConfigLock.Unlock()
		t.Fatal("reconcile completed without touching the config lock — the interleaving is no longer exercised")
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, lazy.Shutdown(ctx))
	require.False(t, lazy.isLoaded())

	gate, loadDone := startGatedLoad(t, ctx, lazy)

	index.replicationConfigLock.Unlock()

	// The reconcile now blocks on the parked shard's mutex.
	select {
	case <-reconcileDone:
		t.Fatal("reconcile completed while Load held the shard mutex — the apply no longer touches it")
	case <-time.After(200 * time.Millisecond):
	}

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{Factor: 1})
	}()

	// Qualifying states: updater queued on the write lock, or the new factor already published.
	require.Eventually(t, func() bool {
		if !index.replicationConfigLock.TryRLock() {
			return true
		}
		factor := index.Config.ReplicationFactor
		index.replicationConfigLock.RUnlock()
		return factor == 1
	}, deadlockSyncTimeout, time.Millisecond, "updateReplicationConfig neither queued nor published")

	close(gate.release)

	requireBothComplete(t, "ReconcileAsyncReplicationForShard", loadDone, reconcileDone)
	select {
	case err := <-updateDone:
		require.NoError(t, err)
	case <-time.After(deadlockTimeout):
		t.Fatal("updateReplicationConfig wedged behind the per-shard reconcile")
	}
	require.NoError(t, repo.Shutdown(context.Background()))
}

// applyLockScopeProbe records, for every apply routed at it, whether the config lock was free and the apply lock held.
type applyLockScopeProbe struct {
	ShardLike
	asyncReplicationController
	idx        *Index
	mu         sync.Mutex
	applies    []string
	configFree bool
	applyHeld  bool
}

func newApplyLockScopeProbe(idx *Index) *applyLockScopeProbe {
	return &applyLockScopeProbe{idx: idx, configFree: true, applyHeld: true}
}

func (p *applyLockScopeProbe) record(op string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applies = append(p.applies, op)
	if p.idx.replicationConfigLock.TryLock() {
		p.idx.replicationConfigLock.Unlock()
	} else {
		p.configFree = false
	}
	if p.idx.asyncReplicationApplyLock.TryLock() {
		p.idx.asyncReplicationApplyLock.Unlock()
		p.applyHeld = false
	}
}

func (p *applyLockScopeProbe) enableAsyncReplication(context.Context, AsyncReplicationConfig) error {
	p.record("enable")
	return nil
}

func (p *applyLockScopeProbe) disableAsyncReplication(context.Context) error {
	p.record("disable")
	return nil
}

func (p *applyLockScopeProbe) hasActiveAsyncReplicationTargetOverrides() bool { return false }

func (p *applyLockScopeProbe) preventShutdown() (func(), error) { return func() {}, nil }

// TestPerShardAsyncReplicationAppliersLockScope: every single-shard applier must apply without the config lock (ABBA vector) and under the apply lock (stale fan-out / resurrection guard).
func TestPerShardAsyncReplicationAppliersLockScope(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		invoke    func(*Index) error
		wantApply string
	}{
		{
			name:      "ReconcileAsyncReplicationForShard",
			invoke:    func(i *Index) error { return i.ReconcileAsyncReplicationForShard(ctx, "probe-shard") },
			wantApply: "disable",
		},
		{
			name:      "InitAsyncReplicationOnShard",
			invoke:    func(i *Index) error { return i.InitAsyncReplicationOnShard(ctx, "probe-shard") },
			wantApply: "enable",
		},
		{
			name:      "RevertAsyncReplicationOnShard",
			invoke:    func(i *Index) error { return i.RevertAsyncReplicationOnShard(ctx, "probe-shard") },
			wantApply: "disable",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, index := newReplConfigDeadlockFixture(t, "ApplierLockScope"+tc.wantApply+tc.name[:4])
			probe := newApplyLockScopeProbe(index)
			index.shards.Store("probe-shard", probe)

			require.NoError(t, tc.invoke(index))
			require.Equal(t, []string{tc.wantApply}, probe.applies)
			require.True(t, probe.configFree, "replicationConfigLock must not be held across the shard apply")
			require.True(t, probe.applyHeld, "asyncReplicationApplyLock must be held across the shard apply")

			_, _ = index.shards.LoadAndDelete("probe-shard")
			require.NoError(t, repo.Shutdown(context.Background()))
		})
	}
}

// TestConcurrentUpdateAndReconcileNoRace: the fan-out's per-shard decision must not read index config unsynchronized (run with -race).
func TestConcurrentUpdateAndReconcileNoRace(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigApplyNoRace")

	index.asyncReplicationScheduler = newSchedulerForUnitTest(t)

	probe := &gatedApplyProbe{entered: make(chan struct{}), release: make(chan struct{})}
	close(probe.release)
	index.shards.Store("probe-shard", probe)

	for i := 0; i < 25; i++ {
		factor := int64(1 + i%3)
		errs := make(chan error, 2)
		go func() { errs <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{Factor: factor}) }()
		go func() { errs <- index.reconcileAsyncReplication(ctx) }()
		require.NoError(t, <-errs)
		require.NoError(t, <-errs)
	}

	_, _ = index.shards.LoadAndDelete("probe-shard")
	require.NoError(t, repo.Shutdown(context.Background()))
}
