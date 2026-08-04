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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
)

// gatedTransferer is a StateTransferer that fails while fail is set,
// counting attempts.
type gatedTransferer struct {
	fail     atomic.Bool
	attempts atomic.Int64
}

func (g *gatedTransferer) TransferState(context.Context, string, string) error {
	g.attempts.Add(1)
	if g.fail.Load() {
		return fmt.Errorf("simulated state-transfer failure")
	}
	return nil
}

// TestStore_SnapshotInstall_FailedRestorePausesApplies pins the
// receive-side snapshot failure contract: a follower whose FSM restore
// (out-of-band state transfer) is failing must not record the snapshot as
// installed in ANY way — no bookkeeping, no log compaction, no
// MsgStorageAppendResp back into raft, no MsgAppResp to the leader — and
// consequently must not apply any post-snapshot entries onto the hollow
// shard. Once the transfer succeeds, the install completes and the follower
// catches up.
//
// Before this contract, a failed restore was logged and then treated as
// success: the log prefix was compacted and subsequent entries were applied
// over a shard that never received the snapshot's data — a silent data hole.
//
// The import quiesces before the late follower joins so the leader cannot
// compact past the pending snapshot while the restore fails; the ack-after-
// stale-compaction wedge is pinned separately (see
// TestStore_SnapshotInstall_StaleAckAfterCompaction_LeaderResumes).
func TestStore_SnapshotInstall_FailedRestorePausesApplies(t *testing.T) {
	// The volume deliberately crosses the Match-floor escape (4× the entry
	// threshold of 8 = 32 applied entries) while node-c is down, so the
	// leader compacts past the absent voter and must serve it a snapshot.
	const applies = 40

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	const lateNode = 2 // node-c starts late and needs the snapshot

	recs := make([]*recordingShard, len(nodeIDs))
	var leaderFlushed atomic.Bool
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		recs[i].mock.EXPECT().FlushForSnapshot(mock.Anything).RunAndReturn(
			func(context.Context) error {
				leaderFlushed.Store(true)
				return nil
			},
		).Maybe()
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 8, // snapshot + compact early so node-c needs MsgSnap
		})

	st := &gatedTransferer{}
	st.fail.Store(true)
	stores[lateNode].SetStateTransferer(st)

	// Start a quorum without node-c and import enough to trigger a snapshot
	// and log compaction on the leader, then quiesce.
	for i, s := range stores {
		if i != lateNode {
			require.NoError(t, s.Start(context.Background()))
		}
	}
	leader := waitForClusterLeader(t, stores[:lateNode])
	for seq := 0; seq < applies; seq++ {
		req := buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeTestObjectWithID(testUUID(3, seq)))
		applyCtx, applyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := stores[leader].Apply(applyCtx, req)
		applyCancel()
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return leaderFlushed.Load() },
		10*time.Second, 20*time.Millisecond, "leader never took a snapshot")
	// Wait for the LEADER's compaction specifically (a follower's own
	// snapshot also sets leaderFlushed): FirstIndex > 1 means the escape
	// fired and the log node-c would need is gone.
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, stores[leader]) > 1 },
		10*time.Second, 20*time.Millisecond, "leader never compacted past the absent voter")
	target := stores[leader].LastAppliedIndex()

	// node-c joins: its log is empty and the leader's is compacted, so it is
	// served a snapshot whose restore (state transfer) fails.
	require.NoError(t, stores[lateNode].Start(context.Background()))
	require.Eventually(t, func() bool { return st.attempts.Load() >= 2 },
		15*time.Second, 20*time.Millisecond, "node-c never attempted the state transfer")

	// While the restore keeps failing, nothing snapshot-related may advance
	// on node-c. Its OWN Bootstrap conf-change entries (indexes 1..3,
	// self-committed by RawNode.Bootstrap) legitimately apply from its first
	// local Ready in a race with the MsgSnap arrival, so the watermark may
	// read up to 3. The bug this pins — a failed restore recorded as success
	// — moves the watermark to the snapshot's applied index (>= the
	// SnapshotThreshold of 8 here) or beyond via post-snapshot entries, so
	// anything above the bootstrap ceiling means the failed install leaked.
	const bootstrapCeiling = uint64(3)
	applied := stores[lateNode].LastAppliedIndex()
	require.LessOrEqualf(t, applied, bootstrapCeiling,
		"node-c advanced its applied watermark to %d (> bootstrap ceiling %d) although its snapshot restore never succeeded — failed install recorded as success",
		applied, bootstrapCeiling)
	require.Empty(t, recs[lateNode].dispatchOrder(),
		"node-c applied entries onto a shard whose snapshot restore never succeeded")

	// Let the transfer succeed: the install completes and node-c catches up
	// to everything the leader applied.
	st.fail.Store(false)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer waitCancel()
	require.NoError(t, stores[lateNode].WaitForAppliedIndex(waitCtx, target),
		"node-c never caught up after the state transfer recovered")
	require.Greaterf(t, st.attempts.Load(), int64(2),
		"transfer succeeded without the retry loop re-attempting (attempts=%d)", st.attempts.Load())
}

// TestStore_SnapshotInstall_StaleAckAfterCompaction_LeaderResumes pins the
// leader-side snapshot liveness contract: when a follower's snapshot ack
// arrives AFTER the leader has compacted past the snapshot it sent — which a
// slow FSM restore under a live import makes routine — the leader must still
// resume replication (fresh snapshot or appends) instead of parking the
// follower in StateSnapshot forever. etcd/raft only exits StateSnapshot on an
// ack when Match+1 >= firstIndex (raft.go:1523 in the vendored library); the
// application-side recovery is ReportSnapshot(SnapshotFinish) on MsgSnap
// transmission (processReady), which returns the peer to probing at the
// snapshot index so a stale ack merely triggers a re-probe.
func TestStore_SnapshotInstall_StaleAckAfterCompaction_LeaderResumes(t *testing.T) {
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	const lateNode = 2

	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		recs[i].mock.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 8,
		})

	// node-c's restore is slow (fails for a while), and the import KEEPS
	// RUNNING: by the time node-c's held ack reaches the leader, the leader
	// has snapshotted and compacted past the snapshot it sent.
	st := &gatedTransferer{}
	st.fail.Store(true)
	stores[lateNode].SetStateTransferer(st)

	for i, s := range stores {
		if i != lateNode {
			require.NoError(t, s.Start(context.Background()))
		}
	}
	leader := waitForClusterLeader(t, stores[:lateNode])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := 0; ctx.Err() == nil; seq++ {
			req := buildPutObjectApplyRequest(t, testClassName, testShardName,
				makeTestObjectWithID(testUUID(4, seq)))
			applyCtx, applyCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, _ = stores[leader].Apply(applyCtx, req)
			applyCancel()
		}
	}()

	// The continuous import must first cross the Match-floor escape (4× the
	// entry threshold of 8 = 32 applied entries) while node-c is down —
	// FirstIndex > 1 on the leader means the escape fired, the log node-c
	// would need is compacted, and it will be served a snapshot.
	require.Eventually(t, func() bool { return shard.LogFirstIndex(t, stores[leader]) > 1 },
		15*time.Second, 20*time.Millisecond, "leader never compacted past the absent voter")
	require.NoError(t, stores[lateNode].Start(context.Background()))
	require.Eventually(t, func() bool { return st.attempts.Load() >= 2 },
		15*time.Second, 20*time.Millisecond, "node-c never attempted the state transfer")

	// Restore succeeds ~2 snapshot generations later; the ack is stale.
	time.Sleep(time.Second)
	st.fail.Store(false)

	// The leader must eventually resume replication (fresh snapshot or
	// appends) and node-c must catch up. RED today: Progress[c] wedges in
	// StateSnapshot.
	require.Eventually(t, func() bool {
		return stores[lateNode].LastAppliedIndex() > 0
	}, 20*time.Second, 50*time.Millisecond,
		"leader never resumed replication to node-c after its stale snapshot ack (StateSnapshot wedge)")
	cancel()
	wg.Wait()
}
