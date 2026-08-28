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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Unit pins for the shard shutdown/restore lifecycle (the integrationTest-
// tagged journeys live in shard_shutdown_test.go; these stay untagged so the
// plain unit gate runs them).

// TestShardStillAlive pins the lifecycle-fence predicate: a shard whose
// Shutdown failed before the shut mark is fully alive and must be restored to
// the shard map (an orphaned live instance lets a reactivation double-open
// the same directory); a shard that got past the mark must not be re-served.
func TestShardStillAlive(t *testing.T) {
	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, shardStillAlive(live), "not marked shut => alive")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, shardStillAlive(shut), "marked shut => not restored")

	require.False(t, shardStillAlive(&LazyLoadShard{}),
		"an unloaded lazy shard holds nothing to orphan")

	loaded := &LazyLoadShard{loaded: true, shard: live}
	require.True(t, shardStillAlive(loaded))

	loadedShut := &LazyLoadShard{loaded: true, shard: shut}
	require.False(t, shardStillAlive(loadedShut))
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

	t.Run("ctx cancellation while in use keeps the flag armed", func(t *testing.T) {
		// backoff returns ctx.Err() and swallows the still-in-use attempt
		// error; the pending refs still exist, so the deferred completion
		// must stay armed.
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := s.Shutdown(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, s.shutdownRequested.Load(),
			"a cancelled wait on an in-use shard is still the in-use case; disarming it strands the deferred shutdown")
	})

	t.Run("non-in-use abort clears the flag", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.shut.Store(true)
		s.teardownErr = errors.New("bucket close failed")
		s.shutdownRequested.Store(true)

		start := time.Now()
		err := s.Shutdown(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, errTeardownFailed)
		require.NotErrorIs(t, err, errShardStillInUse)
		require.False(t, s.shutdownRequested.Load(),
			"an abort with no pending release must disarm the deferred shutdown")
		require.Less(t, time.Since(start), time.Second,
			"a sticky teardown error must fail fast, not burn the retry backoff under shardCreateLocks")
	})
}

// TestRestoreShardIfStillAlive pins the shared restore helper used by every
// failed-shutdown site (UpdateTenants cold path, ShutdownShard,
// UnloadLocalShard, IncomingReinitShard).
func TestRestoreShardIfStillAlive(t *testing.T) {
	var m shardMap

	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, restoreShardIfStillAlive(&m, "s1", live))
	got := m.Load("s1")
	require.NotNil(t, got)

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, restoreShardIfStillAlive(&m, "s2", shut),
		"a cleanly-shut shard must not be re-served")
	require.Nil(t, m.Load("s2"))
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
	require.False(t, shardKnownShut(&Shard{shutdownLock: new(sync.RWMutex)}))
	require.False(t, shardKnownShut(&LazyLoadShard{}),
		"never-loaded lazy shard must NOT be treated as shut")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.True(t, shardKnownShut(shut))
	require.True(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: shut}))
	require.False(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: &Shard{shutdownLock: new(sync.RWMutex)}}))

	// A deep-teardown failure (shut=true, sticky teardownErr) is NOT
	// evictable: the map entry is the last reference to its possibly-leaked
	// handles, so the restore predicate KEEPS it and the belts serve the
	// sticky error instead of re-initializing into a bucket-registry
	// collision (that refusal is pinned in lsmkv's
	// TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose). It is
	// never silently double-opened and never treated as healthy.
	torn := &Shard{shutdownLock: new(sync.RWMutex), teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	require.False(t, shardKnownShut(torn), "a torn shard must not be evicted for re-init")
	require.False(t, shardStillAlive(torn))
	require.ErrorContains(t, shardTeardownError(torn), "bucket close failed")

	var m shardMap
	require.True(t, restoreShardIfStillAlive(&m, "torn", torn),
		"a torn shard is retained as the last reference to its leaked handles")
	require.NotNil(t, m.Load("torn"))
}

// TestShardUnloadOutcomeStrings pins each constant's exact string, because a rename
// compiles and passes every other test. The table and shardUnloadOutcomes are
// checked against each other both ways.
func TestShardUnloadOutcomeStrings(t *testing.T) {
	want := map[ShardUnloadOutcome]string{
		ShardUnloadOutcomeUnloaded:     "unloaded",
		ShardUnloadOutcomeRefusedInUse: "refused_in_use",
		ShardUnloadOutcomeTorn:         "torn",
		ShardUnloadOutcomeIndexClosing: "index_closing",
		ShardUnloadOutcomeFailed:       "failed",
	}

	for outcome, str := range want {
		require.Equal(t, str, string(outcome))
		require.Contains(t, shardUnloadOutcomes, outcome,
			"an outcome this table pins has to be registered")
	}
	for outcome := range shardUnloadOutcomes {
		require.Contains(t, want, outcome, "a registered outcome has to be pinned here")
	}
}

// TestShutdownOrRestoreShardOutcome drives the classification directly. A mock takes
// shardTeardownError's and shardStillAlive's default arms, so it is always restored
// and never torn, and these rows cover the error-shaped arms only.
func TestShutdownOrRestoreShardOutcome(t *testing.T) {
	tests := []struct {
		name        string
		shutdownErr error
		want        ShardUnloadOutcome
	}{
		{
			name: "a clean shutdown",
			want: ShardUnloadOutcomeUnloaded,
		},
		{
			name:        "a shard that was already shut",
			shutdownErr: errAlreadyShutdown,
			want:        ShardUnloadOutcomeUnloaded,
		},
		{
			// performShutdown wraps the sentinel, so a bare one would leave the
			// unwrapping this classification depends on unexercised.
			name:        "a shard still serving requests",
			shutdownErr: fmt.Errorf("shard %q: %w", "s1", errShardStillInUse),
			want:        ShardUnloadOutcomeRefusedInUse,
		},
		{
			// A cancelled backoff carries the context error rather than the
			// refusal, and the shard is held either way.
			name:        "a backoff that ran out of time",
			shutdownErr: context.DeadlineExceeded,
			want:        ShardUnloadOutcomeRefusedInUse,
		},
		{
			name:        "a backoff ended by a close request",
			shutdownErr: context.Canceled,
			want:        ShardUnloadOutcomeRefusedInUse,
		},
		{
			// The residual arm, which nothing else in the package reaches.
			name:        "a teardown that failed for its own reason",
			shutdownErr: errors.New("bucket close failed"),
			want:        ShardUnloadOutcomeFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shard := NewMockShardLike(t)
			shard.On("Shutdown", mock.Anything).Return(tc.shutdownErr)
			idx, _ := indexForShutdownOutcomeTest(t)

			got, err := shutdownOrRestoreShard(context.Background(), idx, "s1", shard)

			require.Equal(t, tc.want, got)
			if tc.shutdownErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.shutdownErr)
		})
	}
}

// TestShutdownOrRestoreShardOutcomeTorn covers the arm a mock cannot reach, since
// shardTeardownError's default arm means a mock is never torn.
func TestShutdownOrRestoreShardOutcomeTorn(t *testing.T) {
	torn := &Shard{shutdownLock: new(sync.RWMutex), teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	idx, hook := indexForShutdownOutcomeTest(t)

	got, err := shutdownOrRestoreShard(context.Background(), idx, "s1", torn)

	require.Equal(t, ShardUnloadOutcomeTorn, got)
	require.ErrorIs(t, err, errTeardownFailed)
	require.NotNil(t, idx.shards.Load("s1"), "a torn shard is retained in the map")

	// Shutdown wraps the sticky error here, so this cannot tell terr from err. It
	// catches the line losing the cause altogether.
	entry := hook.LastEntry()
	require.NotNil(t, entry)
	require.Contains(t, entry.Message, "bucket close failed")
}

// indexForShutdownOutcomeTest builds the minimum Index shutdownOrRestoreShard
// reads: the shard map it restores into, the logger it reports a failed close on,
// and the metrics it stops counting an unloaded shard with. Passing nil for prom
// produces the same *Metrics a node without monitoring has, one whose baseMetrics
// is nil.
func indexForShutdownOutcomeTest(t *testing.T) (*Index, *test.Hook) {
	t.Helper()

	logger, hook := test.NewNullLogger()
	metrics, err := NewMetrics(logger, nil, "Abc", "n/a")
	require.NoError(t, err)

	return &Index{logger: logger, metrics: metrics, shards: shardMap{}}, hook
}
