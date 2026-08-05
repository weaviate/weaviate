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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"go.etcd.io/raft/v3/raftpb"
)

// fsyncGate blocks a sharedlog's flushes (each flush = one fsync) while
// closed. Wire hook as the node's BeforeFlush.
type fsyncGate struct {
	mu sync.Mutex
	ch chan struct{} // non-nil = gate closed; flushes block until it closes
}

func (g *fsyncGate) hook() {
	g.mu.Lock()
	ch := g.ch
	g.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

func (g *fsyncGate) close() {
	g.mu.Lock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *fsyncGate) open() {
	g.mu.Lock()
	if g.ch != nil {
		close(g.ch)
		g.ch = nil
	}
	g.mu.Unlock()
}

// electionTrafficCounter wraps a Transport and counts outbound election
// traffic (pre-votes and votes) — any of it during a leader-side fsync stall
// means the stall leaked into raft timing.
type electionTrafficCounter struct {
	inner    shard.Transport
	votes    atomic.Int64 // MsgPreVote + MsgVote
	appResps atomic.Int64 // MsgAppResp
	vtResps  atomic.Int64 // MsgVoteResp
}

func (c *electionTrafficCounter) Send(groupID uint64, msgs []raftpb.Message) {
	for i := range msgs {
		switch msgs[i].Type {
		case raftpb.MsgPreVote, raftpb.MsgVote:
			c.votes.Add(1)
		case raftpb.MsgAppResp:
			c.appResps.Add(1)
		case raftpb.MsgVoteResp:
			c.vtResps.Add(1)
		default: // only election traffic and acks are counted
		}
	}
	c.inner.Send(groupID, msgs)
}

func (c *electionTrafficCounter) Close() error { return c.inner.Close() }

// TestStore_SlowFsync_NoElections pins the fsync-convoy fix: a multi-second
// sharedlog fsync stall on the leader node (first big LSM flush landing on the
// same disk) must not park heartbeat TRANSMISSION beyond the election timeout.
// Before AsyncStorageWrites the Ready loop persisted synchronously in
// processReady step 1, so one slow batch commit stopped every outbound message
// for its duration: followers campaigned on time (honestly) and the leader
// stepped down on time (honestly) — one clean, but client-visible, transfer
// per convoy. With storage writes off the loop, a 5x-election-timeout fsync
// stall must produce zero election traffic and no leadership change, and every
// in-flight Apply must complete once the disk recovers.
func TestStore_SlowFsync_NoElections(t *testing.T) {
	const (
		electionTimeout = 200 * time.Millisecond
		stall           = time.Second // 5x election timeout
		writers         = 2
	)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	gates := make([]*fsyncGate, len(nodeIDs))
	counters := make([]*electionTrafficCounter, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		i := i
		gates[i] = &fsyncGate{}
		specs[i] = shard.TestStoreSpec{
			NodeID:      id,
			Shard:       newRecordingShard(t, 0).mock,
			BeforeFlush: gates[i].hook,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				counters[i] = &electionTrafficCounter{inner: inner}
				return counters[i]
			},
		}
	}

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   electionTimeout,
			SnapshotThreshold: 4096,
		})
	// Registered after the store cleanups so it runs BEFORE them (LIFO): a
	// failed assertion must not leave a closed gate deadlocking Store.Stop.
	for _, g := range gates {
		t.Cleanup(g.open)
	}
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)

	// Continuous import load so the leader has appends in flight when the
	// convoy hits. Every issued Apply must eventually succeed: the convoy may
	// delay acks (commit waits on local durability) but must not fail them.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var applyErrs atomic.Int64
	var applyOK atomic.Int64
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				req := buildPutObjectApplyRequest(t, testClassName, testShardName,
					makeTestObjectWithID(testUUID(w, seq)))
				applyCtx, applyCancel := context.WithTimeout(context.Background(), 20*time.Second)
				if _, err := stores[leader].Apply(applyCtx, req); err != nil {
					applyErrs.Add(1)
				} else {
					applyOK.Add(1)
				}
				applyCancel()
			}
		}()
	}

	// Let leadership and the import settle, then zero the election counters
	// and stall every fsync on the leader node for 5x the election timeout.
	time.Sleep(300 * time.Millisecond)
	for _, c := range counters {
		c.votes.Store(0)
	}
	gates[leader].close()
	time.Sleep(stall)
	gates[leader].open()

	// Give one election timeout of settle time so any campaign started at the
	// tail of the stall becomes visible before asserting.
	time.Sleep(electionTimeout)

	var votes int64
	for _, c := range counters {
		votes += c.votes.Load()
	}
	require.Zerof(t, votes,
		"a %v leader fsync stall caused %d pre-vote/vote messages — storage wait is still parking raft transmission", stall, votes)
	require.Equalf(t, shard.ShardStateLeader, stores[leader].State(),
		"leader lost leadership during a %v fsync stall", stall)

	// Drain the writers; everything issued must have completed error-free.
	cancel()
	wg.Wait()
	require.Zerof(t, applyErrs.Load(),
		"%d Applies failed across a %v fsync stall (%d succeeded) — the convoy must delay acks, never fail them",
		applyErrs.Load(), stall, applyOK.Load())
	require.NotZero(t, applyOK.Load(), "load phase produced no successful applies")
}

// TestStore_DurabilityGate_ResponsesWaitForFsync pins the durability invariant
// that AsyncStorageWrites must preserve: a node never acknowledges a log
// append (MsgAppResp) and never grants an election vote (MsgVoteResp) before
// the covering state is fsynced. Green before and after the async change —
// this is the regression guard for the appender's response gating.
func TestStore_DurabilityGate_ResponsesWaitForFsync(t *testing.T) {
	t.Run("append ack waits for follower fsync", func(t *testing.T) {
		nodeIDs := []string{"node-a", "node-b"}
		gates := make([]*fsyncGate, len(nodeIDs))
		counters := make([]*electionTrafficCounter, len(nodeIDs))
		specs := make([]shard.TestStoreSpec, len(nodeIDs))
		for i, id := range nodeIDs {
			i := i
			gates[i] = &fsyncGate{}
			specs[i] = shard.TestStoreSpec{
				NodeID:      id,
				Shard:       newRecordingShard(t, 0).mock,
				BeforeFlush: gates[i].hook,
				WrapTransport: func(inner shard.Transport) shard.Transport {
					counters[i] = &electionTrafficCounter{inner: inner}
					return counters[i]
				},
			}
		}
		stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
			shard.TestClusterOptions{
				TickInterval:      20 * time.Millisecond,
				HeartbeatTimeout:  40 * time.Millisecond,
				ElectionTimeout:   200 * time.Millisecond,
				SnapshotThreshold: 4096,
			})
		// LIFO: opens gates before Store.Stop so a failed assertion cannot
		// deadlock cleanup on a parked flush.
		for _, g := range gates {
			t.Cleanup(g.open)
		}
		for _, s := range stores {
			require.NoError(t, s.Start(context.Background()))
		}
		leader := waitForClusterLeader(t, stores)
		follower := 1 - leader
		time.Sleep(200 * time.Millisecond) // drain bootstrap appends

		// Gate the follower's fsyncs, then propose. In a 2-node group the
		// follower's ack is required for commit, so the Apply must hang, and
		// no MsgAppResp may leave the follower while its disk is gated.
		gates[follower].close()
		counters[follower].appResps.Store(0)

		applyDone := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req := buildPutObjectApplyRequest(t, testClassName, testShardName,
				makeTestObjectWithID(testUUID(1, 1)))
			_, err := stores[leader].Apply(ctx, req)
			applyDone <- err
		}()

		time.Sleep(600 * time.Millisecond)
		require.Zerof(t, counters[follower].appResps.Load(),
			"follower sent %d MsgAppResp while its fsync was gated — append acknowledged before durability",
			counters[follower].appResps.Load())
		select {
		case err := <-applyDone:
			t.Fatalf("Apply completed while the quorum follower could not fsync (err=%v)", err)
		default:
		}

		gates[follower].open()
		require.NoError(t, <-applyDone, "Apply failed after the follower's disk recovered")
		require.NotZero(t, counters[follower].appResps.Load(),
			"follower never acked the append after its fsync gate opened")
	})

	t.Run("vote grant waits for fsync", func(t *testing.T) {
		nodeIDs := []string{"node-a", "node-b"}
		gates := make([]*fsyncGate, len(nodeIDs))
		counters := make([]*electionTrafficCounter, len(nodeIDs))
		specs := make([]shard.TestStoreSpec, len(nodeIDs))
		for i, id := range nodeIDs {
			i := i
			gates[i] = &fsyncGate{}
			specs[i] = shard.TestStoreSpec{
				NodeID:      id,
				BeforeFlush: gates[i].hook,
				WrapTransport: func(inner shard.Transport) shard.Transport {
					counters[i] = &electionTrafficCounter{inner: inner}
					return counters[i]
				},
			}
		}
		// node-b's disk is gated from the start: it can exchange messages but
		// cannot persist a vote, so no leader may emerge (2-node quorum needs
		// node-b's granted — durable — vote) and node-b must send no
		// MsgVoteResp until its gate opens.
		gates[1].close()
		stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
			shard.TestClusterOptions{
				TickInterval:      20 * time.Millisecond,
				HeartbeatTimeout:  40 * time.Millisecond,
				ElectionTimeout:   200 * time.Millisecond,
				SnapshotThreshold: 4096,
			})
		// LIFO: opens gates before Store.Stop so a failed assertion cannot
		// deadlock cleanup on a parked flush.
		for _, g := range gates {
			t.Cleanup(g.open)
		}
		for _, s := range stores {
			require.NoError(t, s.Start(context.Background()))
		}

		time.Sleep(800 * time.Millisecond)
		require.Zerof(t, counters[1].vtResps.Load(),
			"gated node sent %d MsgVoteResp before its vote was durable", counters[1].vtResps.Load())
		for i, s := range stores {
			require.NotEqualf(t, shard.ShardStateLeader, s.State(),
				"node %d became leader although the quorum peer could not persist its vote", i)
		}

		gates[1].open()
		waitForClusterLeader(t, stores)
	})
}
