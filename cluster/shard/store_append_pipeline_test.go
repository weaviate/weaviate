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
)

// TestStore_AppendPipeline_ConcurrentAppendsShareFsyncs pins the append lane's
// fsync amortization: N appends pending while one fsync is in flight must be
// covered by FEWER than N fsyncs. The append worker pipelines MsgStorageAppend
// batches into the sharedlog batcher without waiting for each one's covering
// flush, so every append that arrives during a flush rides the NEXT flush
// together — a depth-1 lane (append → fsync → respond → next) degenerates to
// one fsync per append and fails this test.
//
// The durability contract is asserted alongside: no Apply may complete while
// every fsync on the node is parked (single-node commit requires local
// durability), and every Apply must succeed once the disk recovers.
func TestStore_AppendPipeline_ConcurrentAppendsShareFsyncs(t *testing.T) {
	const pendingAppends = 8

	gate := &fsyncGate{}
	var flushes atomic.Int64
	specs := []shard.TestStoreSpec{{
		NodeID:      "node-a",
		Shard:       newRecordingShard(t, 0).mock,
		BeforeFlush: gate.hook,
		AfterFlush:  func() { flushes.Add(1) },
	}}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 4096,
		})
	// Registered after the store cleanup so it runs BEFORE it (LIFO): a failed
	// assertion must not leave a closed gate deadlocking Store.Stop.
	t.Cleanup(gate.open)
	require.NoError(t, stores[0].Start(context.Background()))
	waitForClusterLeader(t, stores)
	time.Sleep(200 * time.Millisecond) // drain bootstrap appends

	// Park every fsync, then issue the appends one at a time so each is staged
	// in its own Ready round — its own MsgStorageAppend — behind the parked
	// flush.
	gate.close()
	base := flushes.Load()

	var wg sync.WaitGroup
	var applyErrs atomic.Int64
	acked := make(chan struct{}, pendingAppends)
	for i := 0; i < pendingAppends; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			req := buildPutObjectApplyRequest(t, testClassName, testShardName,
				makeTestObjectWithID(testUUID(9, i)))
			if _, err := stores[0].Apply(ctx, req); err != nil {
				applyErrs.Add(1)
			}
			acked <- struct{}{}
		}()
		time.Sleep(30 * time.Millisecond)
	}
	require.Empty(t, acked, "no Apply may ack while every covering fsync is parked")

	gate.open()
	wg.Wait()
	require.Zero(t, applyErrs.Load(), "every gated append must succeed once the disk recovers")

	// Let trailing commit-advance appends (HardState-only) flush before
	// counting.
	time.Sleep(150 * time.Millisecond)
	used := flushes.Load() - base
	t.Logf("%d pending appends were covered by %d flushes", pendingAppends, used)
	require.Lessf(t, used, int64(pendingAppends),
		"%d appends pending across one parked flush consumed %d fsyncs — the append lane is not amortizing (depth-1 pipeline)",
		pendingAppends, used)
}
