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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"go.etcd.io/raft/v3/raftpb"
)

// instrumentedTransport wraps a Transport to count outbound heartbeats and to
// inject per-Send latency, simulating a slow network write path. delay sleeps
// BEFORE delivery (messages arrive late); postDelay sleeps AFTER delivery
// (messages arrive on time but the sender's loop stalls — the fsync-convoy
// shape, where peers' responses queue up in the stalled sender's inbox).
type instrumentedTransport struct {
	inner      shard.Transport
	heartbeats atomic.Int64
	delay      atomic.Int64 // pre-delivery per-Send sleep, in nanoseconds
	postDelay  atomic.Int64 // post-delivery per-Send sleep, in nanoseconds
}

func (c *instrumentedTransport) Send(groupID uint64, msgs []raftpb.Message) {
	if d := time.Duration(c.delay.Load()); d > 0 {
		time.Sleep(d)
	}
	for i := range msgs {
		if msgs[i].Type == raftpb.MsgHeartbeat {
			c.heartbeats.Add(1)
		}
	}
	c.inner.Send(groupID, msgs)
	if d := time.Duration(c.postDelay.Load()); d > 0 {
		time.Sleep(d)
	}
}

func (c *instrumentedTransport) Close() error { return c.inner.Close() }

// makeTestObjectWithID mirrors makeTestObject with a caller-chosen UUID so
// concurrent writers produce distinguishable objects.
func makeTestObjectWithID(id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              testClassName,
			CreationTimeUnix:   1000000,
			LastUpdateTimeUnix: 1000000,
		},
		Vector:    []float32{0.1, 0.2, 0.3},
		VectorLen: 3,
	}
}

// testUUID derives a deterministic valid UUID from writer and sequence numbers.
func testUUID(writer, seq int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("%08x-%04x-4000-8000-000000000000", writer, seq%0x10000))
}

// waitForClusterLeader polls until exactly one store reports leadership and
// returns its index.
func waitForClusterLeader(t *testing.T, stores []*shard.Store) int {
	t.Helper()
	var leader int
	require.Eventually(t, func() bool {
		for i, s := range stores {
			if s.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "no leader elected")
	return leader
}

// TestStore_SlowApply_HeartbeatsKeepFlowing pins the bug-#2 production
// signature: when FSM apply is slow (LSM writes under import load), the Ready
// loop must keep generating heartbeats at the nominal rate. Before the async
// apply worker, Dispatch ran inline on the loop, collapsing tick consumption
// ~10x for busy groups: followers' election timers expired and the leader's
// CheckQuorum window dilated, causing mid-import step-downs.
//
// Numbers: heartbeat interval 40ms, 2 followers => nominal ~100 heartbeats
// sent by the leader in the 2s measurement window. With 8 writers and 40ms
// apply latency, the pre-fix loop stalls ~320ms per Ready and sends <=~20.
// The 50%-of-nominal threshold sits well clear of both.
func TestStore_SlowApply_HeartbeatsKeepFlowing(t *testing.T) {
	const (
		applyLatency = 40 * time.Millisecond
		writers      = 8
		measure      = 2 * time.Second
	)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	transports := make([]*instrumentedTransport, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		i := i
		m := mocks.NewMockshard(t)
		m.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
			func(context.Context, *storobj.Object) error {
				time.Sleep(applyLatency)
				return nil
			},
		)
		m.EXPECT().ReadOnlyErr().Return(nil).Maybe()
		m.EXPECT().ClassPresent().Return(true).Maybe()
		specs[i] = shard.TestStoreSpec{
			NodeID: id,
			Shard:  m,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				transports[i] = &instrumentedTransport{inner: inner}
				return transports[i]
			},
		}
	}

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   400 * time.Millisecond,
			SnapshotThreshold: 4096,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)

	// Continuous import load: each writer serially Applies objects until told
	// to stop. Apply errors are tolerated (leadership churn IS the failure
	// mode under test) — the assertion below is on heartbeat throughput.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var applyErrs atomic.Int64
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				req := buildPutObjectApplyRequest(t, testClassName, testShardName,
					makeTestObjectWithID(testUUID(w, seq)))
				applyCtx, applyCancel := context.WithTimeout(context.Background(), 5*time.Second)
				if _, err := stores[leader].Apply(applyCtx, req); err != nil {
					applyErrs.Add(1)
				}
				applyCancel()
			}
		}()
	}

	// Warm up, then measure heartbeats sent by the leader over the window.
	time.Sleep(500 * time.Millisecond)
	h0 := transports[leader].heartbeats.Load()
	time.Sleep(measure)
	h1 := transports[leader].heartbeats.Load()
	cancel()
	wg.Wait()

	sent := h1 - h0
	nominal := int64(measure/(40*time.Millisecond)) * int64(len(nodeIDs)-1)
	t.Logf("heartbeats sent by leader in %v: %d (nominal %d, apply errors %d)",
		measure, sent, nominal, applyErrs.Load())
	require.GreaterOrEqualf(t, sent, nominal/2,
		"leader heartbeat generation collapsed under slow FSM apply: sent %d of nominal %d in %v — Ready loop is starved by inline Dispatch",
		sent, nominal, measure)
}

// TestStore_TickStarvation_LeaderStepsDownOnTime pins FIX B (missed-tick
// replay): a leader whose Ready loop is stalled by slow outbound writes must
// still detect quorum loss via CheckQuorum after ~1 election timeout of WALL
// time. Before the fix the loop counted only ticks it consumed (capacity-1
// ticker), so a 300ms-per-Send stall dilated the 400ms CheckQuorum window to
// multiple seconds and the leader wedged in a stale leadership.
func TestStore_TickStarvation_LeaderStepsDownOnTime(t *testing.T) {
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	transports := make([]*instrumentedTransport, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		i := i
		specs[i] = shard.TestStoreSpec{
			NodeID: id,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				transports[i] = &instrumentedTransport{inner: inner}
				return transports[i]
			},
		}
	}

	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   400 * time.Millisecond,
			SnapshotThreshold: 4096,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leader := waitForClusterLeader(t, stores)

	// Let CheckQuorum observe a healthy quorum first.
	time.Sleep(300 * time.Millisecond)

	// Partition: stop both followers (they go silent — no responses, no
	// campaigns) and stall every leader Send by 300ms, so the leader's loop
	// consumes ~2 ticks per ~320ms instead of 10 per 200ms.
	for i, s := range stores {
		if i != leader {
			require.NoError(t, s.Stop())
		}
	}
	transports[leader].delay.Store(int64(300 * time.Millisecond))

	// CheckQuorum fires after electionTimeout (400ms) of quorum silence. With
	// wall-clock tick replay the leader steps down within ~1s; with consumed-
	// tick counting it takes >3s. 1.5s cleanly separates the two.
	require.Eventuallyf(t, func() bool {
		return stores[leader].State() != shard.ShardStateLeader
	}, 1500*time.Millisecond, 20*time.Millisecond,
		"leader did not step down within wall-clock CheckQuorum window: tick starvation dilated the election timer")
}

// buildStallWakeCluster wires the 3-node cluster shape shared by the
// stall-wake CheckQuorum tests: instrumented transports, 20ms ticks, 40ms
// heartbeats, no FSM load. Election timeouts are asymmetric — node 0 gets
// 400ms (a short CheckQuorum window and the first campaign, so it always
// wins the initial election), the others get 2.4s — so repeated ~540ms
// leader stalls can cross the leader's own CheckQuorum window every cycle
// while remaining far below the followers' campaign timers. Any leadership
// change in this cluster is therefore the LEADER's own doing, isolating the
// CheckQuorum mechanism from honest heartbeat-gap follower elections.
func buildStallWakeCluster(t *testing.T) ([]*shard.Store, []*instrumentedTransport) {
	t.Helper()
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	transports := make([]*instrumentedTransport, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		i := i
		specs[i] = shard.TestStoreSpec{
			NodeID: id,
			WrapTransport: func(inner shard.Transport) shard.Transport {
				transports[i] = &instrumentedTransport{inner: inner}
				return transports[i]
			},
			ElectionTimeout: 2400 * time.Millisecond,
		}
	}
	specs[0].ElectionTimeout = 400 * time.Millisecond
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   400 * time.Millisecond,
			SnapshotThreshold: 4096,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	return stores, transports
}

// TestStore_StallWake_NoSpuriousStepDownWithQueuedResponses pins the
// stall-wake CheckQuorum defect: a leader whose loop stalls AFTER its
// heartbeats were delivered (post-Send stall — the fsync-convoy shape) wakes
// with both the ticker and its followers' responses ready. If tick replay
// runs before those queued responses are stepped, the election-timeout burst
// evaluates CheckQuorum against pre-stall RecentActive flags — cleared by the
// previous burst's check — and the leader steps down against demonstrably
// healthy followers.
//
// Geometry: every ~540ms stall spans a full 400ms CheckQuorum window, so each
// wake whose select services the ticker first runs exactly one quorum check;
// with responses drained first the check always passes (followers respond
// within microseconds on the MemNetwork), so a leadership change here can
// only come from evaluating stale knowledge.
func TestStore_StallWake_NoSpuriousStepDownWithQueuedResponses(t *testing.T) {
	stores, transports := buildStallWakeCluster(t)
	leader := waitForClusterLeader(t, stores)
	require.Equal(t, 0, leader, "short-election-timeout node must win the initial election")

	// Let a healthy CheckQuorum round pass, then stall every leader Send
	// AFTER delivery for longer than the 400ms election timeout.
	time.Sleep(300 * time.Millisecond)
	transports[leader].postDelay.Store(int64(540 * time.Millisecond))

	// Followers stay healthy and responsive throughout; the leader must never
	// step down. Poll for 8s (~14 stall/wake cycles).
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		require.Equalf(t, shard.ShardStateLeader, stores[leader].State(),
			"leader stepped down despite queued fresh follower responses: CheckQuorum evaluated pre-stall knowledge on tick-replay wake")
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStore_StallWake_HonestStepDownStillFires is the distinction test for
// the drain-before-replay fix: the identical post-Send stall geometry, but
// with both followers stopped (genuinely silent). Draining the inbox finds
// nothing, so the wake burst must still cross the CheckQuorum window and step
// the leader down at the correct wall-clock time.
func TestStore_StallWake_HonestStepDownStillFires(t *testing.T) {
	stores, transports := buildStallWakeCluster(t)
	leader := waitForClusterLeader(t, stores)
	require.Equal(t, 0, leader, "short-election-timeout node must win the initial election")

	time.Sleep(300 * time.Millisecond)
	for i, s := range stores {
		if i != leader {
			require.NoError(t, s.Stop())
		}
	}
	transports[leader].postDelay.Store(int64(540 * time.Millisecond))

	require.Eventuallyf(t, func() bool {
		return stores[leader].State() != shard.ShardStateLeader
	}, 2*time.Second, 20*time.Millisecond,
		"leader with genuinely silent followers did not step down within the wall-clock CheckQuorum window")
}

// TestStore_CompressedCrossings_NoSpuriousStepDown pins the elapsed-carry
// CheckQuorum defect (live signature: step-downs with both followers' last
// responses stepped 150-306ms earlier, µs-identical ages, full match).
//
// Mechanism: raft clears the RecentActive evidence flags at every quorum
// evaluation and re-evaluates one election timeout of TICKS later. A replay
// burst that crosses the threshold mid-burst leaves an elapsed CARRY, so the
// next evaluation can be fed only (electionTicks − carry) ticks — a few
// hundred ms of wall time — after the previous one cleared the flags. Any
// wake in that compressed span whose drain happens to find no queued
// responses evaluates zero evidence and steps down, although the followers
// responded moments earlier and are merely slow this instant.
//
// Geometry: the leader's loop stalls 310ms after every Send (compressed
// crossings ~310ms apart, 2 of 3 wakes); followers normally respond
// instantly, so every wake's drain re-arms the flags and all evaluations
// pass. Each pulse delays the followers' outbound responses by 400ms for one
// batch, opening exactly one empty-drain wake; an ungated leader that lands a
// crossing there steps down spuriously. With crossings gated to a full
// election timeout of wall spacing, every evaluation is preceded by a drain
// that stepped at least one response batch since the previous clear, and the
// leader must survive all pulses.
func TestStore_CompressedCrossings_NoSpuriousStepDown(t *testing.T) {
	stores, transports := buildStallWakeCluster(t)
	leader := waitForClusterLeader(t, stores)
	require.Equal(t, 0, leader, "short-election-timeout node must win the initial election")

	// One healthy CheckQuorum round, then the compressed-crossing regime.
	time.Sleep(300 * time.Millisecond)
	transports[leader].postDelay.Store(int64(310 * time.Millisecond))

	for pulse := 0; pulse < 12; pulse++ {
		for i := range transports {
			if i != leader {
				transports[i].delay.Store(int64(400 * time.Millisecond))
			}
		}
		time.Sleep(450 * time.Millisecond)
		for i := range transports {
			if i != leader {
				transports[i].delay.Store(0)
			}
		}
		time.Sleep(800 * time.Millisecond)
		require.Equalf(t, shard.ShardStateLeader, stores[leader].State(),
			"leader stepped down during pulse %d although followers' responses were only ~310ms old: compressed CheckQuorum evaluations consumed freshly-cleared flags", pulse)
	}
}
