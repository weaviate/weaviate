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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
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

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(1_000), createdAt))

	root, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok, "expected active checkpoint")
	assert.Equal(t, ckAbs(1_000), cutoff)
	assert.Equal(t, createdAt, ca)
	// Root mirrors the unbounded hashtree at clone time.
	assert.Equal(t, s.hashtree.Root(), root)
}

func TestShard_CreateAsyncCheckpoint_StaleCreatedAtIsRejected(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	older := time.Now().UTC()
	newer := older.Add(time.Second)

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(2_000), newer))

	// An older createdAt loses the tie-breaker; the active checkpoint stays.
	err := s.CreateAsyncCheckpoint(ctx, ckAbs(3_000), older)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errAsyncCheckpointStale), "expected stale error, got %v", err)

	_, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok)
	assert.Equal(t, ckAbs(2_000), cutoff, "active checkpoint must not be overwritten by stale createdAt")
	assert.Equal(t, newer, ca)
}

func TestShard_CreateAsyncCheckpoint_NewerCreatedAtReplaces(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	first := time.Now().UTC()
	second := first.Add(time.Second)

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(2_000), first))
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(3_000), second))

	_, cutoff, ca, ok := s.AsyncCheckpointRoot(ctx)
	assert.True(t, ok)
	assert.Equal(t, ckAbs(3_000), cutoff)
	assert.Equal(t, second, ca)
}

func TestShard_CreateAsyncCheckpoint_EqualCreatedAtIsStale(t *testing.T) {
	// The tie-breaker is strict greater-than; equal createdAt is rejected so
	// concurrent replicas converge deterministically without coordination.
	ctx := context.Background()
	s := shardWithHashtree(t)
	at := time.Now().UTC()

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(2_000), at))
	err := s.CreateAsyncCheckpoint(ctx, ckAbs(3_000), at)
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
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(1_000), time.Now().UTC()))

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
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(1_000), time.Now().UTC()))

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
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(1_000), time.Now().UTC()))

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
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, ckAbs(1_000), futureCreatedAt))
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
	futureCutoff := base.Add(time.Hour).UnixMilli()
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = s.CreateAsyncCheckpoint(ctx, futureCutoff+int64(i+1), base.Add(time.Duration(i)*time.Millisecond))
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

func TestShard_CreateAsyncCheckpoint_RejectsPastCutoff(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)

	require.ErrorIs(t, s.CreateAsyncCheckpoint(ctx, time.Now().Add(-time.Hour).UnixMilli(), time.Now()), errAsyncCheckpointCutoffInPast)
	if _, _, _, ok := s.AsyncCheckpointRoot(ctx); ok {
		t.Fatal("a rejected create must leave no active checkpoint")
	}

	require.NoError(t, s.CreateAsyncCheckpoint(ctx, time.Now().Add(time.Hour).UnixMilli(), time.Now()))
	if _, _, _, ok := s.AsyncCheckpointRoot(ctx); !ok {
		t.Fatal("a future cutoff must create the checkpoint")
	}
}

const (
	ckA = strfmt.UUID("00000000-0000-0000-0000-0000000000a1")
	ckB = strfmt.UUID("00000000-0000-0000-0000-0000000000b2")
	ckC = strfmt.UUID("00000000-0000-0000-0000-0000000000c3")
)

type ckOp struct {
	del      bool
	id       strfmt.UUID
	ts       int64
	old      int64
	delEvent int64
}

// ckBase shifts test timestamps into the future so the cutoff-in-past guard passes; 0 stays 0 (no-version sentinel).
var ckBase = time.Now().Add(time.Hour).UnixMilli()

func ckAbs(v int64) int64 {
	if v == 0 {
		return 0
	}
	return ckBase + v
}

func ckApply(t *testing.T, s *Shard, op ckOp) {
	t.Helper()
	idBytes, err := uuid.MustParse(op.id.String()).MarshalBinary()
	require.NoError(t, err)
	if op.del {
		require.NoError(t, s.deleteObjectHashTree(idBytes, ckAbs(op.ts), ckAbs(op.delEvent)))
		return
	}
	obj := &storobj.Object{Object: models.Object{ID: op.id, LastUpdateTimeUnix: ckAbs(op.ts)}}
	require.NoError(t, s.upsertObjectHashTree(obj, idBytes, objectInsertStatus{oldUpdateTime: ckAbs(op.old)}))
}

// ckRoot builds a fresh shard, applies before-ops, creates a checkpoint at cutoff, applies after-ops, returns the checkpoint root.
func ckRoot(t *testing.T, cutoff int64, before, after []ckOp) hashtree.Digest {
	t.Helper()
	s := shardWithHashtree(t)
	for _, op := range before {
		ckApply(t, s, op)
	}
	require.NoError(t, s.CreateAsyncCheckpoint(context.Background(), ckAbs(cutoff), time.UnixMilli(ckAbs(cutoff))))
	for _, op := range after {
		ckApply(t, s, op)
	}
	root, _, _, ok := s.AsyncCheckpointRoot(context.Background())
	require.True(t, ok)
	return root
}

func TestShard_AsyncCheckpoint_AbsorbsLatePreCutoffObjects(t *testing.T) {
	const cutoff = 200
	full := ckRoot(t, cutoff, []ckOp{{id: ckA, ts: 100}, {id: ckB, ts: 110}, {id: ckC, ts: 120}}, nil)
	behind := ckRoot(t, cutoff, []ckOp{{id: ckA, ts: 100}, {id: ckB, ts: 110}}, nil)
	require.NotEqual(t, full, behind, "a shard missing a pre-cutoff object must not already match")

	caughtUp := ckRoot(t, cutoff,
		[]ckOp{{id: ckA, ts: 100}, {id: ckB, ts: 110}},
		[]ckOp{{id: ckC, ts: 120}})
	require.Equal(t, full, caughtUp, "a pre-cutoff object arriving after creation must converge the root")
}

func TestShard_AsyncCheckpoint_FoldGating(t *testing.T) {
	const cutoff = 200
	tests := []struct {
		name                             string
		beforeA, afterA, beforeB, afterB []ckOp
	}{
		{
			name:    "pre-cutoff create absorbed",
			beforeA: []ckOp{{id: ckA, ts: 100}, {id: ckB, ts: 120}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{id: ckB, ts: 120}},
		},
		{
			name:    "post-cutoff create excluded",
			beforeA: []ckOp{{id: ckA, ts: 100}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{id: ckB, ts: 250}},
		},
		{
			name:    "pre-cutoff update absorbed",
			beforeA: []ckOp{{id: ckA, ts: 150}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{id: ckA, ts: 150, old: 100}},
		},
		{
			name:    "post-cutoff update keeps pre-cutoff version",
			beforeA: []ckOp{{id: ckA, ts: 100}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{id: ckA, ts: 250, old: 100}},
		},
		{
			name:    "pre-cutoff delete absorbed",
			beforeA: []ckOp{{id: ckB, ts: 110}},
			beforeB: []ckOp{{id: ckA, ts: 100}, {id: ckB, ts: 110}}, afterB: []ckOp{{del: true, id: ckA, ts: 100, delEvent: 150}},
		},
		{
			name:    "post-cutoff delete keeps object",
			beforeA: []ckOp{{id: ckA, ts: 100}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{del: true, id: ckA, ts: 100, delEvent: 250}},
		},
		{
			name:    "resurrection converges to recreated state",
			beforeA: []ckOp{{id: ckA, ts: 150}},
			beforeB: []ckOp{{id: ckA, ts: 100}}, afterB: []ckOp{{del: true, id: ckA, ts: 100, delEvent: 120}, {id: ckA, ts: 150}},
		},
		{
			name:    "post-cutoff-then-pre-cutoff update injects no phantom",
			beforeA: []ckOp{{id: ckB, ts: 100}}, afterA: []ckOp{{id: ckA, ts: 150}},
			beforeB: []ckOp{{id: ckB, ts: 100}}, afterB: []ckOp{{id: ckA, ts: 300}, {id: ckA, ts: 150, old: 300}},
		},
		{
			name:    "post-cutoff object with pre-cutoff deletion event stays absent",
			beforeA: []ckOp{{id: ckB, ts: 100}},
			beforeB: []ckOp{{id: ckB, ts: 100}}, afterB: []ckOp{{id: ckA, ts: 300}, {del: true, id: ckA, ts: 300, delEvent: 150}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reference := ckRoot(t, cutoff, tt.beforeA, tt.afterA)
			journey := ckRoot(t, cutoff, tt.beforeB, tt.afterB)
			require.Equal(t, reference, journey)
		})
	}
}

func TestShard_AsyncCheckpoint_ConcurrentFoldIsRaceFree(t *testing.T) {
	ctx := context.Background()
	s := shardWithHashtree(t)
	cutoff := time.Now().Add(time.Hour).UnixMilli()
	require.NoError(t, s.CreateAsyncCheckpoint(ctx, cutoff, time.UnixMilli(cutoff)))

	const writers = 8
	var wg sync.WaitGroup
	wg.Add(writers + 1)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", w*1000+i))
				idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
				if err != nil {
					continue
				}
				obj := &storobj.Object{Object: models.Object{ID: id, LastUpdateTimeUnix: 500_000}}
				s.asyncReplicationRWMux.RLock()
				_ = s.upsertObjectHashTree(obj, idBytes, objectInsertStatus{})
				s.asyncReplicationRWMux.RUnlock()
			}
		}()
	}
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = s.DeleteAsyncCheckpoint(ctx)
			_ = s.CreateAsyncCheckpoint(ctx, cutoff+int64(i+1), time.UnixMilli(cutoff+int64(i+1)))
		}
	}()
	wg.Wait()

	require.NotPanics(t, func() { s.AsyncCheckpointRoot(ctx) })
}
