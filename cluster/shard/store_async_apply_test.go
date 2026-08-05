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
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestMissedTicks pins the wall-clock tick-replay arithmetic.
func TestMissedTicks(t *testing.T) {
	base := time.Unix(1000, 0)
	interval := 20 * time.Millisecond

	tests := []struct {
		name        string
		elapsed     time.Duration
		interval    time.Duration
		maxTicks    int
		wantN       int
		wantLast    time.Duration // expected watermark advance from base
		wantClamped bool
	}{
		{name: "zero elapsed", elapsed: 0, interval: interval, maxTicks: 10, wantN: 0, wantLast: 0},
		{name: "sub-interval", elapsed: 19 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 0, wantLast: 0},
		{name: "exactly one interval", elapsed: 20 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 1, wantLast: 20 * time.Millisecond},
		{name: "remainder carried", elapsed: 50 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 2, wantLast: 40 * time.Millisecond},
		{name: "at cap", elapsed: 200 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 10, wantLast: 200 * time.Millisecond},
		{name: "over cap drops backlog", elapsed: 900 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 10, wantLast: 900 * time.Millisecond, wantClamped: true},
		{name: "non-positive interval", elapsed: 100 * time.Millisecond, interval: 0, maxTicks: 10, wantN: 0, wantLast: 100 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, last, clamped := shard.MissedTicks(base, base.Add(tc.elapsed), tc.interval, tc.maxTicks)
			require.Equal(t, tc.wantN, n)
			require.Equal(t, base.Add(tc.wantLast), last)
			require.Equal(t, tc.wantClamped, clamped)
		})
	}
}

// TestGatePlan pins the CheckQuorum crossing-gate arithmetic: a replay burst
// never ticks past a quorum evaluation (the crossing truncates the burst and
// the caller drops the leftover backlog), so consecutive evaluations are
// always separated by a full election timeout of newly-elapsed wall time,
// and pre-crossing ticks always flow (heartbeat generation never held).
func TestGatePlan(t *testing.T) {
	const el = 20

	tests := []struct {
		name        string
		n           int
		ttc         int // ticks to crossing before the call
		wantAllowed int
		wantTTC     int
		wantCrossed bool
	}{
		{name: "no crossing in range", n: 5, ttc: 9, wantAllowed: 5, wantTTC: 4},
		{name: "one short of crossing", n: 8, ttc: 9, wantAllowed: 8, wantTTC: 1},
		{name: "burst ends exactly at crossing", n: 5, ttc: 5, wantAllowed: 5, wantTTC: el, wantCrossed: true},
		{name: "burst truncated at crossing", n: el, ttc: 5, wantAllowed: 5, wantTTC: el, wantCrossed: true},
		{name: "full-period burst crosses at end", n: el, ttc: el, wantAllowed: el, wantTTC: el, wantCrossed: true},
		{name: "single tick is the crossing", n: 1, ttc: 1, wantAllowed: 1, wantTTC: el, wantCrossed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			allowed, ttc, crossed := shard.GatePlan(tc.n, tc.ttc, el)
			require.Equal(t, tc.wantAllowed, allowed)
			require.Equal(t, tc.wantTTC, ttc)
			require.Equal(t, tc.wantCrossed, crossed)
		})
	}
}

// recordingShard wraps a Mockshard configured to record the order in which
// PutObject dispatches reach the shard, with optional per-op latency jitter.
type recordingShard struct {
	mock *mocks.Mockshard

	// durableFloor feeds the DurableRaftFloor mock — the shard's durable
	// flush watermark as consulted by the snapshot cadence. Defaults to
	// MaxUint64 (no un-flushed writes, no compaction cap); tests exercising
	// the cap lower it.
	durableFloor atomic.Uint64

	mu    sync.Mutex
	order []strfmt.UUID
	seen  map[strfmt.UUID]int
}

func newRecordingShard(t *testing.T, maxJitter time.Duration) *recordingShard {
	r := &recordingShard{
		mock: mocks.NewMockshard(t),
		seen: map[strfmt.UUID]int{},
	}
	r.durableFloor.Store(math.MaxUint64)
	r.mock.EXPECT().DurableRaftFloor().RunAndReturn(r.durableFloor.Load).Maybe()
	r.mock.EXPECT().ReadOnlyErr().Return(nil).Maybe()
	r.mock.EXPECT().ClassPresent().Return(true).Maybe()
	r.mock.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, obj *storobj.Object) error {
			if maxJitter > 0 {
				time.Sleep(time.Duration(rand.Int63n(int64(maxJitter)))) //nolint:gosec // test jitter
			}
			r.mu.Lock()
			r.order = append(r.order, obj.Object.ID)
			r.seen[obj.Object.ID]++
			r.mu.Unlock()
			return nil
		},
	).Maybe()
	return r
}

func (r *recordingShard) dispatchOrder() []strfmt.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]strfmt.UUID(nil), r.order...)
}

func (r *recordingShard) has(id strfmt.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[id] > 0
}

// TestStore_AsyncApply_OrderAndAckContract pins two contracts across the
// loop→worker handoff, under concurrent writers and apply jitter:
//
//  1. dispatch order equals RAFT log order — entries are handed off and
//     applied strictly in index order, never reordered by the handoff;
//  2. Apply acks at quorum commit: the returned index is committed (the
//     committed-staged watermark covers it the moment Apply returns) and the
//     entry materializes locally once the FSM catches up to it — the
//     watermark/wait contract the read protocol relies on.
func TestStore_AsyncApply_OrderAndAckContract(t *testing.T) {
	const (
		writers        = 4
		appliesPerSeat = 25
	)

	rec := newRecordingShard(t, 3*time.Millisecond)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, rec.mock)
	startAndWaitForLeader(t, store)

	var (
		mu      sync.Mutex
		indexOf = map[strfmt.UUID]uint64{}
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < appliesPerSeat; seq++ {
				id := testUUID(w, seq)
				req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(id))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				idx, err := store.Apply(ctx, req)
				require.NoError(t, err)

				// Ack contract: the acked index is committed-staged, and
				// waiting for local apply to cover it makes the object
				// visible.
				require.GreaterOrEqualf(t, store.CommittedIndex(), idx,
					"Apply acked index %d before the committed watermark covered it", idx)
				require.NoError(t, store.WaitForAppliedIndex(ctx, idx))
				require.Truef(t, rec.has(id), "index %d applied without object %s reaching the shard", idx, id)
				cancel()

				mu.Lock()
				indexOf[id] = idx
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Order contract: the recorded dispatch sequence is strictly increasing
	// in log index.
	order := rec.dispatchOrder()
	require.Len(t, order, writers*appliesPerSeat)
	for i := 1; i < len(order); i++ {
		prev, cur := indexOf[order[i-1]], indexOf[order[i]]
		require.Lessf(t, prev, cur,
			"dispatch order violates log order at position %d: index %d then %d", i, prev, cur)
	}
}

// TestStore_Restart_RedeliversAbandonedApplies pins the Stop/restart
// contract: Stop abandons whatever the apply worker has not yet dispatched,
// and a restart over the same persisted state re-delivers every committed
// entry (from the last persisted snapshot) so no acknowledged write is ever
// lost. Re-application is idempotent, so duplicates are tolerated. The
// restart also replays the Bootstrap conf-change entries through the
// worker→loop round-trip while new applies are in flight.
func TestStore_Restart_RedeliversAbandonedApplies(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "shared-raft.db")
	snapRoot := t.TempDir()

	rec1 := newRecordingShard(t, 10*time.Millisecond)
	store1, closeInfra1 := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 1024, rec1.mock)
	startAndWaitForLeader(t, store1)

	// Writers pump applies; Stop lands mid-load so the worker abandons a
	// queued tail. Acked UUIDs are the ones that MUST survive restart.
	var (
		ackedMu  sync.Mutex
		acked    []strfmt.UUID
		ackedIdx uint64
	)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ; seq++ {
				id := testUUID(w, seq)
				req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(id))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				idx, err := store1.Apply(ctx, req)
				cancel()
				if err != nil {
					return // store stopping
				}
				ackedMu.Lock()
				acked = append(acked, id)
				if idx > ackedIdx {
					ackedIdx = idx
				}
				ackedMu.Unlock()
			}
		}()
	}

	time.Sleep(400 * time.Millisecond)
	require.NoError(t, store1.Stop())
	wg.Wait()
	require.NotEmpty(t, acked, "load phase produced no acknowledged applies")

	// Restart over the same persisted state.
	closeInfra1()
	rec2 := newRecordingShard(t, 0)
	store2, _ := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 1024, rec2.mock)
	startAndWaitForLeader(t, store2)

	// Recovery must reach at least the highest acked index (re-delivery of
	// the whole committed suffix, conf changes included).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, store2.WaitForAppliedIndex(ctx, ackedIdx),
		"restarted store never re-applied up to the highest acked index %d", ackedIdx)

	// No acknowledged write may be lost: an ack means quorum commit, so each
	// acked object was either dispatched before Stop (rec1) or re-delivered
	// from the persisted log during recovery (rec2) — Stop may abandon
	// committed-but-undispatched entries, but never their durability.
	for _, id := range acked {
		require.Truef(t, rec1.has(id) || rec2.has(id),
			"acked object %s missing after restart recovery", id)
	}

	// The store is fully functional after recovery.
	extra := testUUID(999, 0)
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(extra))
	idx, err := store2.Apply(ctx, req)
	require.NoError(t, err)
	require.Greater(t, idx, ackedIdx)
	require.NoError(t, store2.WaitForAppliedIndex(ctx, idx))
	require.True(t, rec2.has(extra))
}

// TestStore_SnapshotDuringAsyncApply_RestartFromSnapshot pins the outbound
// snapshot path under the async apply worker: snapshots trigger off the
// applied watermark while applies are in flight, the log is compacted, and a
// restart over the compacted state recovers and keeps serving writes. A
// compaction keyed to anything ahead of the applied watermark would break the
// restart (missing log entries after the snapshot index).
func TestStore_SnapshotDuringAsyncApply_RestartFromSnapshot(t *testing.T) {
	const applies = 100

	logPath := filepath.Join(t.TempDir(), "shared-raft.db")
	snapRoot := t.TempDir()

	rec1 := newRecordingShard(t, 2*time.Millisecond)
	// FlushForSnapshot is invoked by every snapshot job; requiring at least one
	// call proves snapshots really fired during the load below.
	rec1.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil)
	store1, closeInfra1 := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 8 /* snapshot every ~8 applied entries */, rec1.mock)
	startAndWaitForLeader(t, store1)

	var lastIdx uint64
	for seq := 0; seq < applies; seq++ {
		id := testUUID(1, seq)
		req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(id))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		idx, err := store1.Apply(ctx, req)
		cancel()
		require.NoError(t, err)
		lastIdx = idx
	}

	// Give the advisory snapshot pipeline a moment to complete a final
	// snapshot+compaction round, then stop cleanly.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, store1.Stop())
	closeInfra1()

	// Restart over the compacted log + persisted snapshot.
	rec2 := newRecordingShard(t, 0)
	rec2.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
	store2, _ := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 8, rec2.mock)
	startAndWaitForLeader(t, store2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, store2.WaitForAppliedIndex(ctx, lastIdx),
		"restart from snapshot-compacted state never caught up to index %d", lastIdx)

	extra := testUUID(999, 1)
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(extra))
	extraIdx, err := store2.Apply(ctx, req)
	require.NoError(t, err)
	require.NoError(t, store2.WaitForAppliedIndex(ctx, extraIdx))
	require.True(t, rec2.has(extra))
}

// TestStore_FollowerWatermark_CoversAckedWrites pins the read-protocol
// foundation on a real 3-node cluster: once a write is acked by the leader,
// the leader's committed-staged watermark covers it immediately (the
// watermark GetLastAppliedIndex reports), and any replica — leader included —
// that has waited for the acked index (the WaitForShardReady /
// WaitForLinearizableRead flow) observes the object locally.
func TestStore_FollowerWatermark_CoversAckedWrites(t *testing.T) {
	const applies = 20

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 2*time.Millisecond)
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 1024,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)

	for seq := 0; seq < applies; seq++ {
		id := testUUID(7, seq)
		req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(id))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		idx, err := stores[leader].Apply(ctx, req)
		require.NoError(t, err)

		// Ack ⇒ committed-staged on the leader: the watermark covers the
		// acked index right now, ahead of (or equal to) local apply.
		require.GreaterOrEqual(t, stores[leader].CommittedIndex(), idx)

		// Catch-up to the acked index ⇒ object visible, on every replica —
		// the leader included: its ack no longer implies local apply.
		for i, s := range stores {
			require.NoError(t, s.WaitForAppliedIndex(ctx, idx))
			require.Truef(t, recs[i].has(id), "replica %d applied index %d without object %s", i, idx, id)
		}
		cancel()
	}
}
