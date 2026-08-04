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

package shard_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	"github.com/weaviate/weaviate/entities/storobj"
)

// batchRecordingShard is a mock shard whose PutObjectBatch records each
// round's object IDs and blocks until the test releases it — one token per
// round via releaseOne, or all rounds via releaseAll (idempotent; MUST be
// registered as a cleanup: Stop waits for the apply worker's in-flight
// dispatch, so an unreleased round deadlocks teardown). started signals each
// round's ordinal as it enters the shard, so tests can hold the apply lane at
// a known point.
type batchRecordingShard struct {
	mock *mocks.Mockshard

	mu    sync.Mutex
	calls [][]strfmt.UUID

	started  chan int
	releases chan struct{}
	once     sync.Once
}

func newBatchRecordingShard(t *testing.T) *batchRecordingShard {
	s := &batchRecordingShard{
		mock:     mocks.NewMockshard(t),
		started:  make(chan int, 64),
		releases: make(chan struct{}, 64),
	}
	s.mock.EXPECT().ReadOnlyErr().Return(nil).Maybe()
	s.mock.EXPECT().ClassPresent().Return(true).Maybe()
	s.mock.EXPECT().PutObjectBatch(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, objs []*storobj.Object) []error {
			ids := make([]strfmt.UUID, len(objs))
			for i, o := range objs {
				ids[i] = o.Object.ID
			}
			s.mu.Lock()
			s.calls = append(s.calls, ids)
			n := len(s.calls)
			s.mu.Unlock()
			s.started <- n
			<-s.releases
			return make([]error, len(objs))
		},
	).Maybe()
	return s
}

func (s *batchRecordingShard) releaseOne() { s.releases <- struct{}{} }

func (s *batchRecordingShard) releaseAll() { s.once.Do(func() { close(s.releases) }) }

// callSnapshot returns a copy of the recorded rounds.
func (s *batchRecordingShard) callSnapshot() [][]strfmt.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]strfmt.UUID, len(s.calls))
	for i := range s.calls {
		out[i] = append([]strfmt.UUID(nil), s.calls[i]...)
	}
	return out
}

// waitStarted waits for round ordinal n to enter the shard.
func (s *batchRecordingShard) waitStarted(t *testing.T, n int) {
	t.Helper()
	select {
	case got := <-s.started:
		require.Equal(t, n, got, "shard rounds must start in order")
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for shard round %d to start", n)
	}
}

// coalesceUUID derives a deterministic UUID distinct per (batch, offset).
func coalesceUUID(batch, off int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("%08x-%04x-4000-8000-00000000c0a1", batch, off%0x10000))
}

// TestStore_ApplyCoalescing_DrainsQueuedBatches pins the apply lane's round
// coalescing: committed PUT_OBJECTS_BATCH entries that queue up while one
// dispatch is materializing must be drained and materialized in FEWER shard
// rounds than entries — the per-round LSM overhead (WAL walks, queue flushes,
// tracker rewrites, fan-out barriers) is amortized across the backlog. A lane
// that dispatches one entry = one shard round fails this test.
//
// Applied-watermark honesty is pinned alongside: while a round is inside the
// shard, the watermark must not cover any entry of that round, even though the
// entries are already drained off the queue; acks (Apply returns) still happen
// at commit, ahead of all of this.
func TestStore_ApplyCoalescing_DrainsQueuedBatches(t *testing.T) {
	const batches = 8

	rec := newBatchRecordingShard(t)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, rec.mock)
	startAndWaitForLeader(t, store)
	t.Cleanup(rec.releaseAll)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Batch #1 commits, acks, and its dispatch parks inside the shard.
	idx := make([]uint64, batches)
	var err error
	idx[0], err = store.Apply(ctx, buildPutObjectsBatchApplyRequest(t, testClassName, testShardName,
		[]*storobj.Object{makeTestObjectWithID(coalesceUUID(0, 0)), makeTestObjectWithID(coalesceUUID(0, 1))}))
	require.NoError(t, err)
	rec.waitStarted(t, 1)

	// Batches #2..M ack at commit and pile up behind the parked dispatch.
	for i := 1; i < batches; i++ {
		idx[i], err = store.Apply(ctx, buildPutObjectsBatchApplyRequest(t, testClassName, testShardName,
			[]*storobj.Object{makeTestObjectWithID(coalesceUUID(i, 0)), makeTestObjectWithID(coalesceUUID(i, 1))}))
		require.NoError(t, err, "ack-at-commit must not wait on the parked apply lane")
	}
	require.Less(t, store.LastAppliedIndex(), idx[0],
		"the applied watermark must not cover a dispatch still inside the shard")

	// Finish round #1; the next round begins with the queued backlog. While it
	// is inside the shard, the watermark must cover exactly the first entry.
	rec.releaseOne()
	rec.waitStarted(t, 2)
	require.Equal(t, idx[0], store.LastAppliedIndex(),
		"drained-but-unmaterialized entries must not advance the applied watermark")

	rec.releaseAll()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	require.NoError(t, store.WaitForAppliedIndex(waitCtx, idx[batches-1]))

	calls := rec.callSnapshot()
	t.Logf("%d committed batch entries materialized in %d shard rounds", batches, len(calls))
	require.Lessf(t, len(calls), batches,
		"%d queued batch entries took %d shard rounds — the apply lane is not coalescing (one round per entry)",
		batches, len(calls))

	// Log order must survive the coalescing: the flattened rounds equal the
	// per-batch object sequence in propose order.
	var got []strfmt.UUID
	for _, c := range calls {
		got = append(got, c...)
	}
	var want []strfmt.UUID
	for i := 0; i < batches; i++ {
		want = append(want, coalesceUUID(i, 0), coalesceUUID(i, 1))
	}
	require.Equal(t, want, got, "coalesced rounds must preserve log order")
}

// TestStore_ApplyCoalescing_DuplicateUUIDKeepsLogOrder pins the determinism
// boundary of round merging: two entries writing the SAME UUID must never
// share one shard round. The shard-side batcher resolves intra-round
// duplicates by keeping the LAST occurrence, while sequential log-order apply
// resolves cross-entry duplicates through the LWW replay guard — merging them
// into one round would make the outcome depend on how each replica happened to
// partition the backlog (replica divergence). Distinct-UUID entries may merge
// freely: any partition yields the same final state.
//
// Green before the coalescing change (every entry is its own round) and after
// (the window splits at the duplicate) — this is the safety pin for the merge
// rule, not a red test.
func TestStore_ApplyCoalescing_DuplicateUUIDKeepsLogOrder(t *testing.T) {
	dupID := coalesceUUID(100, 0)

	tests := []struct {
		name string
		// entryObjs[i] is the object-ID layout of queued entry i (applied
		// after a sacrificial parked entry so all of them queue together).
		entryObjs [][]strfmt.UUID
	}{
		{
			name: "duplicate across entries must split rounds",
			entryObjs: [][]strfmt.UUID{
				{dupID, coalesceUUID(101, 0)},
				{dupID, coalesceUUID(102, 0)},
			},
		},
		{
			name: "distinct entries may merge",
			entryObjs: [][]strfmt.UUID{
				{coalesceUUID(103, 0), coalesceUUID(103, 1)},
				{coalesceUUID(104, 0), coalesceUUID(104, 1)},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := newBatchRecordingShard(t)
			store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
				[]string{testNodeID}, rec.mock)
			startAndWaitForLeader(t, store)
			t.Cleanup(rec.releaseAll)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Sacrificial parked entry so the case's entries queue together.
			_, err := store.Apply(ctx, buildPutObjectsBatchApplyRequest(t, testClassName, testShardName,
				[]*storobj.Object{makeTestObjectWithID(coalesceUUID(99, 0))}))
			require.NoError(t, err)
			rec.waitStarted(t, 1)

			var lastIdx uint64
			var want []strfmt.UUID
			for _, ids := range tc.entryObjs {
				objs := make([]*storobj.Object, len(ids))
				for j, id := range ids {
					objs[j] = makeTestObjectWithID(id)
				}
				lastIdx, err = store.Apply(ctx, buildPutObjectsBatchApplyRequest(t, testClassName, testShardName, objs))
				require.NoError(t, err)
				want = append(want, ids...)
			}

			rec.releaseAll()
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer waitCancel()
			require.NoError(t, store.WaitForAppliedIndex(waitCtx, lastIdx))

			calls := rec.callSnapshot()
			require.NotEmpty(t, calls)
			// Rounds after the sacrificial first: no round may contain the
			// same UUID twice, and the flattened sequence must equal log
			// order.
			var got []strfmt.UUID
			for _, c := range calls[1:] {
				seen := map[strfmt.UUID]struct{}{}
				for _, id := range c {
					_, dup := seen[id]
					require.Falsef(t, dup,
						"shard round %v contains UUID %s twice — cross-entry duplicates must split the round (replica-divergence hazard)",
						c, id)
					seen[id] = struct{}{}
				}
				got = append(got, c...)
			}
			require.Equal(t, want, got, "rounds must preserve log order")
		})
	}
}
