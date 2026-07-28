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
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestShardStillAlive pins the lifecycle-fence predicate: a shard whose
// Shutdown failed before the shut mark is fully alive and must be restored to
// the shard map (an orphaned live instance lets a reactivation double-open
// the same directory); a shard that got past the mark must not be re-served.
func TestShardStillAlive(t *testing.T) {
	live := &Shard{}
	require.True(t, shardStillAlive(live), "not marked shut => alive")

	shut := &Shard{}
	shut.shut.Store(true)
	require.False(t, shardStillAlive(shut), "marked shut => not restored")

	require.False(t, shardStillAlive(&LazyLoadShard{}),
		"an unloaded lazy shard holds nothing to orphan")

	loaded := &LazyLoadShard{loaded: true, shard: live}
	require.True(t, shardStillAlive(loaded))

	loadedShut := &LazyLoadShard{loaded: true, shard: shut}
	require.False(t, shardStillAlive(loadedShut))
}

// TestPollUntilEmpty_ShardGoneFailsFast pins the decoupling's fast-fail: a
// pending-read error on a shard that is no longer locally loaded is a tenant
// lifecycle event, not a blip — the unit must fail on the FIRST errored read
// instead of burning the 3-tick tolerance (≥60s of dead wait per shard).
func TestPollUntilEmpty_ShardGoneFailsFast(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	task := dropTask(distributedtask.TaskStatusStarted, nil)

	reads := 0
	bucket := &fakeEditOpBucket{pendingFn: func(string) ([]string, error) {
		reads++
		return nil, errors.New("database not open")
	}}
	err := p.pollUntilEmpty(context.Background(), bucket, task, "u1", "op1",
		func() bool { return true })
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard no longer locally loaded")
	require.Equal(t, 1, reads, "must fail on the first read, no blip tolerance")

	// Control: with the shard still loaded, the same error is treated as a
	// blip and tolerated up to the bounded retry budget.
	reads2 := 0
	bucket2 := &fakeEditOpBucket{pendingFn: func(string) ([]string, error) {
		reads2++
		return nil, errors.New("transient")
	}}
	err = p.pollUntilEmpty(context.Background(), bucket2, task, "u1", "op1",
		func() bool { return false })
	require.Error(t, err)
	require.Contains(t, err.Error(), "consecutive errors")
	require.Equal(t, maxConsecutivePollErrors, reads2)
}

// TestShutdown_RequestFlagAfterFailure pins the two failure contracts of the
// request flag:
//   - still-in-use: the flag STAYS armed — pending refs exist by definition,
//     and the last release completing the shutdown is the designed
//     eventual-shutdown contract (integration-pinned by
//     TestShardShutdownWhenIdleEventually); guarded ops refuse meanwhile.
//   - any other abort (e.g. a teardown that already failed): the flag is
//     cleared — there may be no pending release left to complete it, and a
//     restored shard would otherwise be a zombie behind a trusted map hit.
func TestShutdown_RequestFlagAfterFailure(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("still in use keeps the deferred shutdown armed", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1) // an in-flight guarded op

		err := s.Shutdown(context.Background())
		require.ErrorIs(t, err, errShardStillInUse)
		require.True(t, s.shutdownRequested.Load(),
			"pending refs complete the shutdown on release; the flag must stay armed")
		_, err = s.preventShutdown()
		require.ErrorIs(t, err, errShutdownInProgress,
			"new guarded ops are refused while the drain is pending")
	})

	t.Run("non-in-use abort clears the flag", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.shut.Store(true)
		s.teardownErr = errors.New("bucket close failed")
		s.shutdownRequested.Store(true)

		err := s.Shutdown(context.Background())
		require.Error(t, err)
		require.NotErrorIs(t, err, errShardStillInUse)
		require.False(t, s.shutdownRequested.Load(),
			"an abort with no pending release must disarm the deferred shutdown")
	})
}

// TestRestoreShardIfStillAlive pins the shared restore helper used by every
// failed-shutdown site (UpdateTenants cold path, ShutdownShard,
// UnloadLocalShard, IncomingReinitShard).
func TestRestoreShardIfStillAlive(t *testing.T) {
	var m shardMap

	live := &Shard{}
	require.True(t, restoreShardIfStillAlive(&m, "s1", live))
	got := m.Load("s1")
	require.NotNil(t, got)

	shut := &Shard{}
	shut.shut.Store(true)
	require.False(t, restoreShardIfStillAlive(&m, "s2", shut),
		"a shard past the shut mark must not be re-served")
	require.Nil(t, m.Load("s2"))
}

// TestVerifiedStillDroppedMemoIsBounded pins the leak fix: DeleteClass during
// an active drop cascade-deletes the task, OnTaskCompleted never fires, and
// without the cap each such cycle would leak a memo entry for the process
// lifetime.
func TestVerifiedStillDroppedMemoIsBounded(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	for i := 0; i < maxVerifiedStillDroppedEntries+50; i++ {
		task := dropTask(distributedtask.TaskStatusFinished, nil)
		task.ID = fmt.Sprintf("t-%d", i)
		require.NoError(t, p.OnGroupCompleted(task, "tenant1", []string{"u1"}))
	}
	p.verifiedMu.Lock()
	defer p.verifiedMu.Unlock()
	require.LessOrEqual(t, len(p.verifiedStillDropped), maxVerifiedStillDroppedEntries)
}

// TestPerformShutdown_TeardownErrorSticks pins that a teardown failure after
// the shut mark stays visible: without the sticky error, the idempotent
// short-circuit converts the retry into a silent nil and callers treat the
// partially-torn shard (open buckets, uncleared registry entries) as cleanly
// closed.
func TestPerformShutdown_TeardownErrorSticks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	s.shut.Store(true)
	s.teardownErr = errors.New("boom: bucket close failed")

	err := s.Shutdown(context.Background())
	require.Error(t, err, "a swallowed teardown failure must resurface, not read as success")
	require.Contains(t, err.Error(), "boom")

	// A cleanly shut shard keeps the idempotent nil.
	clean := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	clean.shut.Store(true)
	require.NoError(t, clean.Shutdown(context.Background()))
}

// TestShardKnownShut pins the reactivation-eviction predicate: ONLY a shard
// that completed a shutdown may be evicted from the map. An unloaded
// LazyLoadShard is the normal steady state of a not-yet-loaded shard —
// evicting it would race a concurrent Load() on the old wrapper into a second
// instance over the same directory.
func TestShardKnownShut(t *testing.T) {
	require.False(t, shardKnownShut(&Shard{}))
	require.False(t, shardKnownShut(&LazyLoadShard{}),
		"never-loaded lazy shard must NOT be treated as shut")

	shut := &Shard{}
	shut.shut.Store(true)
	require.True(t, shardKnownShut(shut))
	require.True(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: shut}))
	require.False(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: &Shard{}}))

	// A deep-teardown failure (shut=true, sticky teardownErr) reads the same
	// as a clean shutdown: known-shut, so the reactivation belt evicts it and
	// restore predicates refuse it. Re-init then either succeeds (teardown got
	// far enough to release the buckets) or is refused loudly by the bucket
	// registry until the leaked handles clear — pinned in lsmkv's
	// TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose. It is never
	// silently double-opened and never restored as if healthy.
	torn := &Shard{teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	require.True(t, shardKnownShut(torn))
	require.False(t, shardStillAlive(torn))
}
