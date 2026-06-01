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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// shardWithHashtree constructs a bare *Shard wired with just enough state
// (hashtree + RWMux + fully-initialised flag) for the async-checkpoint
// methods to exercise their logic without touching the rest of the shard
// machinery. Height 8 yields a small in-memory tree (≈256 leaves) that
// keeps tests fast.
func shardWithHashtree(t *testing.T) *Shard {
	t.Helper()
	ht, err := hashtree.NewHashTree(8)
	require.NoError(t, err)
	return &Shard{
		hashtree:                 ht,
		hashtreeFullyInitialized: true,
	}
}

func TestShard_CreateAsyncCheckpoint_Basic(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	createdAt := time.Now().UTC()

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 1_000, createdAt))

	root, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok, "expected active checkpoint")
	assert.Equal(t, int64(1_000), cutoff)
	assert.Equal(t, createdAt, ca)
	// Root mirrors the unbounded hashtree at clone time.
	assert.Equal(t, s.hashtree.Root(), root)
}

func TestShard_CreateAsyncCheckpoint_StaleCreatedAtIsRejected(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	older := time.Now().UTC()
	newer := older.Add(time.Second)

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 2_000, newer))

	// An older createdAt loses the tie-breaker; the active checkpoint stays.
	err := s.CreateAsyncCheckpoint(ctx, 3_000, older)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errAsyncCheckpointStale), "expected stale error, got %v", err)

	_, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok)
	assert.Equal(t, int64(2_000), cutoff, "active checkpoint must not be overwritten by stale createdAt")
	assert.Equal(t, newer, ca)
}

func TestShard_CreateAsyncCheckpoint_NewerCreatedAtReplaces(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	first := time.Now().UTC()
	second := first.Add(time.Second)

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 2_000, first))
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 3_000, second))

	_, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok)
	assert.Equal(t, int64(3_000), cutoff)
	assert.Equal(t, second, ca)
}

func TestShard_CreateAsyncCheckpoint_EqualCreatedAtIsStale(t *testing.T) {
	// The tie-breaker is strict greater-than; equal createdAt is rejected so
	// concurrent replicas converge deterministically without coordination.
	ctx := context.Background()
	s := shardWithHashtree(t)
	at := time.Now().UTC()

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 2_000, at))
	err := s.CreateAsyncCheckpoint(ctx, 3_000, at)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errAsyncCheckpointStale))
}

func TestShard_CreateAsyncCheckpoint_RequiresActiveHashtree(t *testing.T) {
	ctx := context.Background()
	s := &Shard{} // no hashtree, not initialised
	err := s.CreateAsyncCheckpoint(ctx, 1_000, time.Now())
	require.Error(t, err)
	assert.True(t, errors.Is(err, errAsyncReplicationNotActive))

	// A hashtree that exists but has not finished initialising is treated the
	// same: callers must wait for the init scan before pinning a checkpoint.
	ht, err := hashtree.NewHashTree(8)
	require.NoError(t, err)
	s = &Shard{hashtree: ht, hashtreeFullyInitialized: false}
	err = s.CreateAsyncCheckpoint(ctx, 1_000, time.Now())
	require.Error(t, err)
	assert.True(t, errors.Is(err, errAsyncReplicationNotActive))
}

func TestShard_CreateAsyncCheckpoint_HonoursContextCancel(t *testing.T) {
	// Pre-cancelled context must short-circuit before any state changes
	// happen, even when the hashtree is fully initialised.
	s := shardWithHashtree(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.CreateAsyncCheckpoint(ctx, 1_000, time.Now().UTC())
	require.ErrorIs(t, err, context.Canceled)
	_, _, _, ok := s.AsyncCheckpointRoot(context.Background())
	assert.False(t, ok, "no checkpoint must have been created after a cancelled create")
}

func TestShard_DeleteAsyncCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 1_000, time.Now().UTC()))

	require.NoError(t, s.DeleteAsyncCheckpoint(ctx))

	_, cutoff, _, ok := s.AsyncCheckpointRoot(ctx)
	assert.False(t, ok)
	assert.Equal(t, int64(0), cutoff)
}

func TestShard_DeleteAsyncCheckpoint_IdempotentWhenInactive(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	// Calling Delete without a prior Create must not panic and must leave
	// the shard in the "no active checkpoint" state.
	require.NoError(t, s.DeleteAsyncCheckpoint(ctx))
	_, _, _, ok := s.AsyncCheckpointRoot(ctx)
	assert.False(t, ok)
}

func TestShard_DeleteAsyncCheckpoint_HonoursContextCancel(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 1_000, time.Now().UTC()))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.DeleteAsyncCheckpoint(cancelled), context.Canceled)
	// The earlier checkpoint must still be active — cancellation cannot
	// produce a partial clear.
	_, _, _, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok)
}

func TestShard_AsyncCheckpointRoot_NoActiveCheckpoint(t *testing.T) {
	s := shardWithHashtree(t)
	root, cutoff, ca, ok := s.AsyncCheckpointRoot(context.Background())
	assert.False(t, ok)
	assert.Equal(t, hashtree.Digest{}, root)
	assert.Equal(t, int64(0), cutoff)
	assert.True(t, ca.IsZero())
}

func TestShard_AsyncCheckpoint_ClearedOnStateReset(t *testing.T) {
	// clearAsyncCheckpointLocked is the common cleanup path used by both
	// mayStopAsyncReplication and disableAsyncReplication. Verifying it
	// resets every field guards against a future refactor that adds new
	// state but forgets to wire it in.
	ctx := context.Background()
	s := shardWithHashtree(t)
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 1_000, time.Now().UTC()))

	s.asyncReplicationRWMux.Lock()
	s.clearAsyncCheckpointLocked()
	s.asyncReplicationRWMux.Unlock()

	assert.Nil(t, s.asyncCheckpointHashtree)
	assert.Equal(t, int64(0), s.asyncCheckpointCutoff)
	assert.True(t, s.asyncCheckpointCreatedAt.IsZero())
	assert.True(t, s.asyncCheckpointActivatedAt.IsZero())
}

func TestShard_CreateAsyncCheckpoint_ActivatedAtUsesLocalClock(t *testing.T) {
	// The lifetime histogram must be measured from a local timestamp, not
	// the initiator-supplied createdAt: createdAt may legitimately be in the
	// local future (within the accepted skew tolerance), which would make
	// time.Since negative. Activating with a future createdAt must still
	// stamp asyncCheckpointActivatedAt with the local clock.
	ctx := context.Background()
	s := shardWithHashtree(t)

	before := time.Now()
	futureCreatedAt := before.Add(2 * time.Minute).UTC()
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, 1_000, futureCreatedAt))
	after := time.Now()

	s.asyncReplicationRWMux.RLock()
	activatedAt := s.asyncCheckpointActivatedAt
	s.asyncReplicationRWMux.RUnlock()

	assert.False(t, activatedAt.Before(before), "activatedAt must be local clock, not before the call")
	assert.False(t, activatedAt.After(after), "activatedAt must be local clock, not the future createdAt")
	assert.GreaterOrEqual(t, time.Since(activatedAt), time.Duration(0),
		"lifetime basis must never yield a negative duration")
}

func TestShard_CreateAsyncCheckpoint_ConcurrentRaceIsSerialised(t *testing.T) {
	// Two goroutines racing on Create against the same shard must serialise
	// via asyncReplicationRWMux. The strict-greater-than tie-breaker then
	// ensures at most one createdAt "wins": with N distinct createdAt
	// values, the highest one is the final state. Verifies the mutex
	// discipline + the tie-breaker work together under -race.
	ctx := context.Background()
	s := shardWithHashtree(t)

	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	base := time.Now().UTC()
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = s.CreateAsyncCheckpoint(ctx, int64(i+1), base.Add(time.Duration(i)*time.Millisecond))
		}()
	}
	wg.Wait()

	_, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	require.True(t, ok)
	// The final state must be exactly one of the proposals, not a torn
	// combination. createdAt must be monotonically increasing so the final
	// one is at or after the earliest proposal.
	assert.GreaterOrEqual(t, ca.UnixMilli(), base.UnixMilli())
	assert.Greater(t, cutoff, int64(0))
}
