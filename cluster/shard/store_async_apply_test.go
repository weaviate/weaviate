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
	"math/rand"
	"path/filepath"
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

// TestMissedTicks pins the wall-clock tick-replay arithmetic.
func TestMissedTicks(t *testing.T) {
	base := time.Unix(1000, 0)
	interval := 20 * time.Millisecond

	tests := []struct {
		name     string
		elapsed  time.Duration
		interval time.Duration
		maxTicks int
		wantN    int
		wantLast time.Duration // expected watermark advance from base
	}{
		{name: "zero elapsed", elapsed: 0, interval: interval, maxTicks: 10, wantN: 0, wantLast: 0},
		{name: "sub-interval", elapsed: 19 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 0, wantLast: 0},
		{name: "exactly one interval", elapsed: 20 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 1, wantLast: 20 * time.Millisecond},
		{name: "remainder carried", elapsed: 50 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 2, wantLast: 40 * time.Millisecond},
		{name: "at cap", elapsed: 200 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 10, wantLast: 200 * time.Millisecond},
		{name: "over cap drops backlog", elapsed: 900 * time.Millisecond, interval: interval, maxTicks: 10, wantN: 10, wantLast: 900 * time.Millisecond},
		{name: "non-positive interval", elapsed: 100 * time.Millisecond, interval: 0, maxTicks: 10, wantN: 0, wantLast: 100 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, last := shard.MissedTicks(base, base.Add(tc.elapsed), tc.interval, tc.maxTicks)
			require.Equal(t, tc.wantN, n)
			require.Equal(t, base.Add(tc.wantLast), last)
		})
	}
}

// recordingShard wraps a Mockshard configured to record the order in which
// PutObject dispatches reach the shard, with optional per-op latency jitter.
type recordingShard struct {
	mock *mocks.Mockshard

	mu    sync.Mutex
	order []strfmt.UUID
	seen  map[strfmt.UUID]int
}

func newRecordingShard(t *testing.T, maxJitter time.Duration) *recordingShard {
	r := &recordingShard{
		mock: mocks.NewMockshard(t),
		seen: map[strfmt.UUID]int{},
	}
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
		}).Maybe()
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
//  2. Apply acks only after the entry is applied to the local FSM — the
//     object is visible in the shard and LastAppliedIndex covers its index
//     the moment Apply returns (the contract linearizable reads rely on).
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
				cancel()
				require.NoError(t, err)

				// Ack contract: applied locally before Apply returned.
				require.Truef(t, rec.has(id), "Apply acked %s before the object reached the shard", id)
				require.GreaterOrEqualf(t, store.LastAppliedIndex(), idx,
					"Apply acked index %d before LastAppliedIndex covered it", idx)

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

	// Every acked apply was locally applied before its ack.
	for _, id := range acked {
		require.Truef(t, rec1.has(id), "acked object %s missing before restart", id)
	}

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

	// The store is fully functional after recovery.
	extra := testUUID(999, 0)
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(extra))
	idx, err := store2.Apply(ctx, req)
	require.NoError(t, err)
	require.Greater(t, idx, ackedIdx)
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
	// FlushMemtables is invoked by every snapshot job; requiring at least one
	// call proves snapshots really fired during the load below.
	rec1.mock.EXPECT().FlushMemtables(mock.Anything).Return(nil)
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
	rec2.mock.EXPECT().FlushMemtables(mock.Anything).Return(nil).Maybe()
	store2, _ := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 8, rec2.mock)
	startAndWaitForLeader(t, store2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, store2.WaitForAppliedIndex(ctx, lastIdx),
		"restart from snapshot-compacted state never caught up to index %d", lastIdx)

	extra := testUUID(999, 1)
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(extra))
	_, err := store2.Apply(ctx, req)
	require.NoError(t, err)
	require.True(t, rec2.has(extra))
}

// TestStore_FollowerWatermark_CoversAckedWrites pins the read-protocol
// foundation on a real 3-node cluster: once a write is acked by the leader,
// the leader's applied watermark covers it immediately, and any follower
// that has waited for that index (the WaitForShardReady /
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

		// Ack ⇒ leader applied: watermark and object visible right now.
		require.GreaterOrEqual(t, stores[leader].LastAppliedIndex(), idx)
		require.True(t, recs[leader].has(id))

		// Follower catch-up to the acked index ⇒ object visible there.
		for i, s := range stores {
			if i == leader {
				continue
			}
			require.NoError(t, s.WaitForAppliedIndex(ctx, idx))
			require.Truef(t, recs[i].has(id), "follower %d applied index %d without object %s", i, idx, id)
		}
		cancel()
	}
}
