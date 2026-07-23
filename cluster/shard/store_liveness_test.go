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
// inject per-Send latency, simulating a slow network write path.
type instrumentedTransport struct {
	inner      shard.Transport
	heartbeats atomic.Int64
	delay      atomic.Int64 // per-Send sleep, in nanoseconds
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
			})
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
