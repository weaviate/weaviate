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

// A deployment that ships a smaller per-queue chunk size restarts with the
// old, larger chunks still on disk (sealed and partial). Those legacy
// chunks must drain correctly: a sealed one is dispatched as one (large)
// batch, the adopted partial is sealed and dispatched as-is, and only new
// writes land in chunks of the new size. Nothing may be lost or duplicated.
func TestChunkSizeChangeAcrossRestart(t *testing.T) {
	s := makeScheduler(t)
	dir := t.TempDir()

	// "old" queue: 13-byte header + 13 bytes per record on disk, so 1313
	// bytes seals a chunk at 100 records. 150 records leave one sealed
	// 100-record chunk plus a 50-record partial tail.
	oldQ := makeQueueWith(t, s, discardExecutor(), 1313, dir)
	pushed := make([]uint64, 0, 180)
	for i := uint64(1); i <= 150; i++ {
		require.NoError(t, oldQ.Push(makeRecord(1, i)))
		pushed = append(pushed, i)
	}
	require.NoError(t, oldQ.Flush())
	require.NoError(t, oldQ.Close(context.Background()))

	// restart with ~10-record chunks
	newQ := makeQueueWith(t, s, discardExecutor(), 143, dir)
	t.Cleanup(func() { _ = newQ.Close(context.Background()) })
	require.EqualValues(t, 150, newQ.Size(), "all legacy records must be recovered")

	for i := uint64(151); i <= 180; i++ {
		require.NoError(t, newQ.Push(makeRecord(1, i)))
		pushed = append(pushed, i)
	}
	require.NoError(t, newQ.Flush())
	// let the partial tail become stale so DequeueBatch picks it up
	// (makeQueueWith uses a 500ms stale timeout)
	time.Sleep(600 * time.Millisecond)

	seen := make(map[uint64]int)
	var legacyBatch bool
	for {
		b, err := newQ.DequeueBatch()
		require.NoError(t, err)
		if b == nil {
			break
		}
		if len(b.Tasks) == 100 {
			// the sealed legacy chunk comes through as one large batch
			legacyBatch = true
		}
		var hasPostRestartID bool
		for _, task := range b.Tasks {
			key := task.(*mockTask).key
			if key > 150 {
				hasPostRestartID = true
			}
			seen[key]++
		}
		// records pushed after the restart must land in chunks of the new
		// size: only legacy chunks may exceed it
		if hasPostRestartID {
			require.LessOrEqual(t, len(b.Tasks), 10,
				"post-restart records must be dispatched in new-size chunks")
		}
		b.Done()
	}

	require.True(t, legacyBatch, "the sealed legacy chunk should drain as one batch")
	require.Len(t, seen, 180)
	for _, id := range pushed {
		require.Equal(t, 1, seen[id], "record %d must drain exactly once", id)
	}
	require.EqualValues(t, 0, newQ.Size())
}
