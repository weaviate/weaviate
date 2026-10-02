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

package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A batch is one chunk file, so a queue whose records are slow to process
// must use a small chunk size to bound how long a single batch occupies a
// worker (as the geo and HFresh queues do): with the default 10MB chunks,
// one batch of tiny records can hold the only worker for minutes and
// starve every other queue (weaviate/0-weaviate-issues#670). This pins the
// scheduler behavior that makes the small-chunk remedy work: other queues
// get the worker between a queue's chunk-sized batches.
func TestSchedulerSmallChunksDoNotStarveOtherQueues(t *testing.T) {
	s := makeScheduler(t, 1)
	s.Start()
	defer s.Close(context.Background())

	started := make(chan struct{})
	var startOnce sync.Once
	var slowProcessed atomic.Int32
	slowDecoder := &mockTaskDecoder{
		execFn: func(ctx context.Context, task *mockTask) error {
			startOnce.Do(func() { close(started) })
			time.Sleep(50 * time.Millisecond)
			slowProcessed.Add(1)
			return nil
		},
	}

	// records are 13 bytes on disk (4-byte length prefix + 9-byte record),
	// so ~80 bytes per chunk ≈ 5 tasks ≈ 250ms of work per batch
	slowQ := makeQueueSize(t, s, slowDecoder, 80)
	t.Cleanup(func() { _ = slowQ.Close(context.Background()) })

	// 100 slow tasks: 5s of work in total for the only worker
	ids := make([]uint64, 100)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	pushMany(t, slowQ, 1, ids...)

	// wait until the slow queue owns the worker
	<-started

	fastCh, fastDecoder := streamExecutor()
	fastQ := makeQueue(t, s, fastDecoder)
	t.Cleanup(func() { _ = fastQ.Close(context.Background()) })
	pushMany(t, fastQ, 1, 201, 202, 203)

	// the fast queue must be served while the slow queue still has work
	timeout := time.After(3 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-fastCh:
		case <-timeout:
			t.Fatalf("fast queue starved: received %d of its 3 tasks while the slow queue processed %d/100",
				i, slowProcessed.Load())
		}
	}
	require.Less(t, slowProcessed.Load(), int32(100),
		"slow queue already drained, the test proved nothing about interleaving")
}
