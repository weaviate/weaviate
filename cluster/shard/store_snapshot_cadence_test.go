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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"go.etcd.io/raft/v3/raftpb"
)

// makeBulkTestObject mirrors makeTestObjectWithID with a caller-sized vector,
// so a handful of applies carries bulk-import-scale bytes (~4·vecLen per
// object) while staying far below any entry-count snapshot threshold — the
// measured import shape (70–100 × 4KB objects per raft entry).
func makeBulkTestObject(id strfmt.UUID, vecLen int) *storobj.Object {
	vec := make([]float32, vecLen)
	for i := range vec {
		vec[i] = float32(i%7) * 0.5
	}
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              testClassName,
			CreationTimeUnix:   1000000,
			LastUpdateTimeUnix: 1000000,
		},
		Vector:    vec,
		VectorLen: vecLen,
	}
}

// applyBulkObjects pumps n large-vector puts through the leader and returns
// the last acked index.
func applyBulkObjects(t *testing.T, s *shard.Store, writer, n, vecLen int) uint64 {
	t.Helper()
	var last uint64
	for seq := 0; seq < n; seq++ {
		req := buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeBulkTestObject(testUUID(writer, seq), vecLen))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		idx, err := s.Apply(ctx, req)
		cancel()
		require.NoError(t, err)
		last = idx
	}
	return last
}

// peerAppDropperTransport drops entry-carrying MsgApp — and nothing else —
// sent to one peer while blocked: heartbeats, heartbeat responses, and empty
// MsgApp probes keep flowing, so the leader retains quorum, CheckQuorum stays
// satisfied, and the target follower's replication Match stays pinned at its
// bootstrap tail while the log grows past it.
type peerAppDropperTransport struct {
	inner   shard.Transport
	to      uint64
	blocked *atomic.Bool
}

func (d *peerAppDropperTransport) Send(groupID uint64, msgs []raftpb.Message) {
	if d.blocked.Load() {
		kept := make([]raftpb.Message, 0, len(msgs))
		for i := range msgs {
			if msgs[i].Type == raftpb.MsgApp && msgs[i].To == d.to && len(msgs[i].Entries) > 0 {
				continue
			}
			kept = append(kept, msgs[i])
		}
		if len(kept) == 0 {
			return
		}
		msgs = kept
	}
	d.inner.Send(groupID, msgs)
}

func (d *peerAppDropperTransport) Close() error { return d.inner.Close() }

// TestStore_SnapshotCadence_ByteTriggeredCompaction pins the byte trigger: a
// bulk-import-shaped stream — few entries, each carrying large payloads, far
// below the entry-count threshold — must still trigger snapshot+compaction
// and bound the log's retained bytes. Without the byte trigger the
// entry-count threshold never fires, every payload stays live in the shared
// bbolt log, and group-commit flush latency ages with the accumulating tree
// (15ms fresh → 321ms at ~320MB in the measured 3-node import runs).
func TestStore_SnapshotCadence_ByteTriggeredCompaction(t *testing.T) {
	const (
		applies        = 60
		vecLen         = 4096                // ≈16KB marshalled per entry
		bytesThreshold = 256 * 1024          // ~16 entries per snapshot period
		entryThreshold = uint64(1) << 20     // unreachable: bytes must fire
		retainedBound  = uint64(applies) / 2 // generous: ≈2 periods + in-flight slack
	)

	rec := newRecordingShard(t, 0)
	// Required (not Maybe): at least one snapshot job must run. RED without
	// the byte trigger — 60 entries never reach an entry-count threshold.
	rec.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil)

	store, _ := shard.BuildTestStoreAtWithOptions(t, testClassName, testShardName, testNodeID,
		t.TempDir()+"/shared-raft.db", t.TempDir(),
		shard.TestClusterOptions{
			TickInterval:           20 * time.Millisecond,
			HeartbeatTimeout:       40 * time.Millisecond,
			ElectionTimeout:        80 * time.Millisecond,
			SnapshotThreshold:      entryThreshold,
			SnapshotBytesThreshold: bytesThreshold,
		}, rec.mock)
	startAndWaitForLeader(t, store)

	applied := applyBulkObjects(t, store, 7, applies, vecLen)

	// Compaction must fire (FirstIndex advances past the bootstrap prefix)
	// and the retained tail must stay bounded near the byte threshold.
	require.Eventually(t, func() bool {
		fi := shard.LogFirstIndex(t, store)
		return fi > 1 && applied-(fi-1) <= retainedBound
	}, 10*time.Second, 20*time.Millisecond,
		"byte-triggered snapshot never compacted the log (FirstIndex=%d, applied=%d)",
		shard.LogFirstIndex(t, store), applied)
}

// TestStore_SnapshotCadence_CappedByDurableRaftFloor pins the explicit
// compaction durability gate: the snapshot index must never exceed the
// shard's durable flush watermark (DurableRaftFloor). Entries whose only
// materialization is in un-flushed memtables must stay in the log — a crash
// after compacting them would otherwise silently lose their writes. RED
// without the cap: the byte trigger compacts to the applied index regardless
// of the watermark. The cap must defer, not stall: once the watermark
// advances, the pending backlog compacts.
func TestStore_SnapshotCadence_CappedByDurableRaftFloor(t *testing.T) {
	const (
		applies        = 60
		vecLen         = 4096            // ≈16KB marshalled per entry
		bytesThreshold = 256 * 1024      // fires several times over the run
		entryThreshold = uint64(1) << 20 // unreachable: bytes must fire
		heldFloor      = uint64(5)
	)

	rec := newRecordingShard(t, 0)
	rec.durableFloor.Store(heldFloor)
	rec.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()

	store, _ := shard.BuildTestStoreAtWithOptions(t, testClassName, testShardName, testNodeID,
		t.TempDir()+"/shared-raft.db", t.TempDir(),
		shard.TestClusterOptions{
			TickInterval:           20 * time.Millisecond,
			HeartbeatTimeout:       40 * time.Millisecond,
			ElectionTimeout:        80 * time.Millisecond,
			SnapshotThreshold:      entryThreshold,
			SnapshotBytesThreshold: bytesThreshold,
		}, rec.mock)
	startAndWaitForLeader(t, store)

	applied := applyBulkObjects(t, store, 8, applies, vecLen)
	require.Greater(t, applied, heldFloor)

	// Phase 1: watermark held at heldFloor. The byte trigger fires, but
	// compaction (FirstIndex-1) must never cross the watermark. A snapshot AT
	// the floor is legal (FirstIndex == heldFloor+1).
	require.Never(t, func() bool {
		return shard.LogFirstIndex(t, store) > heldFloor+1
	}, 2*time.Second, 20*time.Millisecond,
		"log compacted past the durable flush watermark: entries whose only materialization is in un-flushed memtables were discarded")

	// Phase 2: the watermark advances (flush caught up) — the deferred
	// backlog must now compact. Cadence is evaluated on Ready rounds: a
	// multi-voter group re-evaluates on every heartbeat round, but this
	// single-voter harness produces no Ready without traffic, so one small
	// apply provides the round that re-runs the trigger.
	rec.durableFloor.Store(math.MaxUint64)
	applyBulkObjects(t, store, 9, 1, 8)
	require.Eventually(t, func() bool {
		return shard.LogFirstIndex(t, store) > heldFloor+1
	}, 10*time.Second, 20*time.Millisecond,
		"snapshot never resumed after the flush watermark advanced — the cap must defer compaction, not stall it")
}

// TestStore_SnapshotCadence_SmallGroupAgeFloor pins the small-group age
// floor: a group whose handful of entries never reaches the entry/byte
// thresholds must still have its log compacted once the retained tail
// outlives the (jittered) SnapshotMinInterval — on every voter, since each
// node compacts its own log. Without the floor, restart replay for small
// groups is unbounded in age: a measured 1000-tenant × 100-object import
// left every group's cadence thresholds unreached, and each voter restart
// replayed every group's log from entry 1. The idle leg pins the zero-cost
// contract: once compaction has caught up, an idle group submits no further
// snapshot jobs.
func TestStore_SnapshotCadence_SmallGroupAgeFloor(t *testing.T) {
	const (
		entryThreshold = uint64(1) << 20 // unreachable: only age may fire
		bytesThreshold = uint64(1) << 30 // unreachable: only age may fire
		minInterval    = 300 * time.Millisecond
		applies        = 3
	)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	var flushes atomic.Int64
	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		recs[i].mock.EXPECT().FlushForSnapshot(mock.Anything).RunAndReturn(
			func(context.Context) error {
				flushes.Add(1)
				return nil
			}).Maybe()
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:           20 * time.Millisecond,
			HeartbeatTimeout:       40 * time.Millisecond,
			ElectionTimeout:        200 * time.Millisecond,
			SnapshotThreshold:      entryThreshold,
			SnapshotBytesThreshold: bytesThreshold,
			SnapshotMinInterval:    minInterval,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)

	applied := applyBulkObjects(t, stores[leader], 6, applies, 8)

	// Every voter must compact its retained tail within the age bound
	// (heartbeat-driven Ready rounds keep the cadence evaluated with no
	// traffic). Compaction through the applied index means restart replay
	// for this group is now bounded.
	for i := range stores {
		require.Eventuallyf(t, func() bool {
			return shard.LogFirstIndex(t, stores[i]) > applied
		}, 15*time.Second, 20*time.Millisecond,
			"voter %d: small group never compacted through applied=%d (FirstIndex=%d) — age floor missing, restart replay unbounded in age",
			i, applied, shard.LogFirstIndex(t, stores[i]))
	}

	// Idle-group zero-cost contract: with nothing retained, the age trigger
	// must not fire again — no further snapshot jobs across several
	// intervals.
	settled := flushes.Load()
	time.Sleep(4 * minInterval)
	require.Equal(t, settled, flushes.Load(),
		"idle group kept submitting snapshot jobs — the age trigger must be free for groups with no retained entries")
}

// TestStore_SnapshotCadence_LeaderRetainsToFollowerMatch pins the snapshot
// floor: when the byte trigger fires while a live voter lags, the leader must
// take its snapshot at that voter's Match — not at the applied index — so the
// raft-visible compaction horizon (FirstIndex, derived from the persisted
// snapshot metadata in sharedlog) never crosses above Match+1 and the leader
// can keep serving plain MsgApp appends.
//
// This pins the meta-before-physical window specifically: the snapshot
// metadata lands in sharedlog one bbolt tx before (and independently of) the
// physical entry deletion, and groupStorage derives FirstIndex/Term/Entries
// bounds from that metadata alone — so the moment a snapshot at the applied
// index persists, a leader probing a follower below it hits ErrCompacted and
// demotes the follower to snapshot + out-of-band state transfer even while
// the entries it needs are still physically present in bbolt. RED before the
// floor: the leader compacts to the applied index unconditionally (the floor
// applies to both triggers — the low entry threshold here is what makes the
// pre-floor code compact at all, pinning the unconditional-compaction bug
// rather than the byte trigger's absence).
func TestStore_SnapshotCadence_LeaderRetainsToFollowerMatch(t *testing.T) {
	const (
		vecLen         = 4096      // ≈16KB marshalled per entry
		bytesThreshold = 64 * 1024 // trigger after ~4 entries
		entryThreshold = uint64(8) // low: both triggers arm, well under their 4× escapes
		applies        = 8         // ≈131KB: past the byte trigger, under the 4× escape
	)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	const laggard = 2
	laggardRaftID := shard.HashNodeID(nodeIDs[laggard])
	var blocked atomic.Bool // false during warm-up: the laggard's Match must reach a known index first

	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		recs[i].mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
		specs[i] = shard.TestStoreSpec{
			NodeID: id,
			Shard:  recs[i].mock,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				return &peerAppDropperTransport{inner: inner, to: laggardRaftID, blocked: &blocked}
			},
		}
	}
	// The laggard must never lead: give it a long campaign timer.
	specs[laggard].ElectionTimeout = 2 * time.Second

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:           20 * time.Millisecond,
			HeartbeatTimeout:       40 * time.Millisecond,
			ElectionTimeout:        200 * time.Millisecond,
			SnapshotThreshold:      entryThreshold,
			SnapshotBytesThreshold: bytesThreshold,
		})

	st := &gatedTransferer{} // fail=false: counts state-transfer attempts
	stores[laggard].SetStateTransferer(st)

	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)
	require.NotEqual(t, laggard, leader, "laggard won the election despite its long campaign timer")

	// Warm-up: one small write replicated to ALL voters pins the laggard's
	// Match at a known index (matchCeiling — the log's last entry, no
	// proposals follow until the drop window opens).
	warmReq := buildPutObjectApplyRequest(t, testClassName, testShardName,
		makeTestObjectWithID(testUUID(8, 0)))
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 10*time.Second)
	matchCeiling, err := stores[leader].Apply(warmCtx, warmReq)
	warmCancel()
	require.NoError(t, err)
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, stores[laggard].WaitForAppliedIndex(syncCtx, matchCeiling))
	syncCancel()

	blocked.Store(true)
	applied := applyBulkObjects(t, stores[leader], 8, applies, vecLen)

	// The triggers fire (committed bytes and entries past both thresholds),
	// so a snapshot lands — but AT the laggard's Match, not at the applied
	// index: the follower is then servable from FirstIndex = Match+1 with
	// Term(Match) answered by the snapshot metadata.
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, stores[leader]) > 1 },
		10*time.Second, 20*time.Millisecond, "no snapshot ever landed on the leader")

	// Let further trigger evaluations pass; none may cross the Match floor.
	time.Sleep(300 * time.Millisecond)
	fi := shard.LogFirstIndex(t, stores[leader])
	require.LessOrEqualf(t, fi, matchCeiling+1,
		"leader's raft-visible compaction horizon (FirstIndex=%d) crossed above the lagging voter's Match=%d — entries the follower needs are unreachable (ErrCompacted) and it will be demoted to snapshot+state transfer",
		fi, matchCeiling)

	// Unblock: the follower must catch up via retained appends — no state
	// transfer — and the released floor must then let compaction proceed.
	blocked.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, stores[laggard].WaitForAppliedIndex(ctx, applied),
		"lagging follower never caught up after the drop window ended")
	require.Zerof(t, st.attempts.Load(),
		"follower needed %d state transfer(s) although the leader should have retained its appends", st.attempts.Load())
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, stores[leader]) > matchCeiling+1 },
		10*time.Second, 20*time.Millisecond,
		"floor released (follower caught up) but the leader never compacted past it")
}

// TestStore_SnapshotCadence_WedgedFollowerEscape pins the floor's escape
// hatch: a voter whose Match stays pinned while the retained tail grows past
// snapshotFloorEscapeMultiplier× the byte threshold stops flooring the
// snapshot — the leader compacts at the applied index and the wedged voter
// legitimately falls back to snapshot + out-of-band state transfer once it
// returns. Without the hatch a single dead voter would pin the log (and the
// bbolt aging curve) forever.
func TestStore_SnapshotCadence_WedgedFollowerEscape(t *testing.T) {
	const (
		vecLen           = 4096
		bytesThreshold   = 64 * 1024
		entryThreshold   = uint64(1) << 20
		applies          = 40 // ≈656KB ≫ 4×64KB: escape must fire
		bootstrapCeiling = uint64(3)
	)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	const laggard = 2
	laggardRaftID := shard.HashNodeID(nodeIDs[laggard])
	var blocked atomic.Bool
	blocked.Store(true)

	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		recs[i].mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
		specs[i] = shard.TestStoreSpec{
			NodeID: id,
			Shard:  recs[i].mock,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				return &peerAppDropperTransport{inner: inner, to: laggardRaftID, blocked: &blocked}
			},
		}
	}
	specs[laggard].ElectionTimeout = 2 * time.Second

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:           20 * time.Millisecond,
			HeartbeatTimeout:       40 * time.Millisecond,
			ElectionTimeout:        200 * time.Millisecond,
			SnapshotThreshold:      entryThreshold,
			SnapshotBytesThreshold: bytesThreshold,
		})

	st := &gatedTransferer{}
	stores[laggard].SetStateTransferer(st)

	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)
	require.NotEqual(t, laggard, leader)

	applied := applyBulkObjects(t, stores[leader], 9, applies, vecLen)

	// Past the escape multiplier the floor yields: the leader compacts beyond
	// the pinned Match.
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, stores[leader]) > bootstrapCeiling+1 },
		15*time.Second, 20*time.Millisecond,
		"escape hatch never fired: leader FirstIndex=%d still pinned at the wedged voter's Match despite a %d-byte retained tail",
		shard.LogFirstIndex(t, stores[leader]), applies*vecLen*4)

	// The returning voter recovers through snapshot + state transfer.
	blocked.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, stores[laggard].WaitForAppliedIndex(ctx, applied),
		"wedged follower never recovered after the leader compacted past its Match")
	require.GreaterOrEqual(t, st.attempts.Load(), int64(1),
		"follower caught up without a state transfer although the leader compacted past its Match")
}

// TestStore_SnapshotCadence_RestartFromByteTriggeredSnapshot pins the restart
// path under the byte trigger: byte-triggered compaction makes
// snapshot+restart the NORMAL boot path, so a restart over the compacted log
// + persisted snapshot (including the InitialState ConfState-from-snapshot
// fallback — the bootstrap conf entries are compacted away) must recover to
// every acked write and keep serving.
func TestStore_SnapshotCadence_RestartFromByteTriggeredSnapshot(t *testing.T) {
	const (
		applies        = 60
		vecLen         = 4096
		bytesThreshold = 64 * 1024
		entryThreshold = uint64(1) << 20
	)

	logPath := t.TempDir() + "/shared-raft.db"
	snapRoot := t.TempDir()
	opts := shard.TestClusterOptions{
		TickInterval:           20 * time.Millisecond,
		HeartbeatTimeout:       40 * time.Millisecond,
		ElectionTimeout:        80 * time.Millisecond,
		SnapshotThreshold:      entryThreshold,
		SnapshotBytesThreshold: bytesThreshold,
	}

	rec1 := newRecordingShard(t, 0)
	// Required: snapshots must really have fired before the restart — RED
	// without the byte trigger.
	rec1.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil)
	store1, closeInfra1 := shard.BuildTestStoreAtWithOptions(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, opts, rec1.mock)
	startAndWaitForLeader(t, store1)

	lastIdx := applyBulkObjects(t, store1, 11, applies, vecLen)

	// Let a final snapshot+compaction round land, then stop cleanly.
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, store1) > 1 },
		10*time.Second, 20*time.Millisecond, "no byte-triggered compaction before restart")
	require.NoError(t, store1.Stop())
	closeInfra1()

	rec2 := newRecordingShard(t, 0)
	rec2.mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
	store2, _ := shard.BuildTestStoreAtWithOptions(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, opts, rec2.mock)
	startAndWaitForLeader(t, store2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, store2.WaitForAppliedIndex(ctx, lastIdx),
		"restart over byte-compacted state never caught up to index %d", lastIdx)

	extra := testUUID(999, 2)
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObjectWithID(extra))
	extraIdx, err := store2.Apply(ctx, req)
	require.NoError(t, err)
	require.NoError(t, store2.WaitForAppliedIndex(ctx, extraIdx))
	require.True(t, rec2.has(extra))
}
