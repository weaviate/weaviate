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

// TestShutdown_FailedInUseResetsRequestFlag pins the restore-composability
// fix: a Shutdown that fails "still in use" must clear shutdownRequested —
// otherwise the shard the caller restores to the map is a zombie (every
// guarded op returns errShutdownInProgress) and refCountSub self-completes
// the shutdown in the background the moment refs drain, leaving a silently
// dead shard behind a trusted map hit.
func TestShutdown_FailedInUseResetsRequestFlag(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	s.inUseCounter.Add(1) // an in-flight guarded op

	err := s.Shutdown(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "still in use")

	require.False(t, s.shutdownRequested.Load(),
		"a failed shutdown must clear the request flag or the restored shard is unusable")
	release, err := s.preventShutdown()
	require.NoError(t, err, "guarded ops must work on a restored shard")
	release()

	// And the deferred ref-drain shutdown is disarmed: draining the ref must
	// NOT self-complete the aborted shutdown.
	s.refCountSub()
	require.False(t, s.shut.Load(), "refCountSub must not self-shut a shard whose shutdown was aborted")
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
