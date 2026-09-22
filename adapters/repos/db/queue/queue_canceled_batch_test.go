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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RED TEST - pins a known defect, currently failing on purpose. Do not skip or
// weaken it.
//
// Only OnDone deletes a dequeued chunk, so a batch that ends in Cancel()
// deliberately leaves its chunk on disk, still counted, to be picked up again.
// It never is: chunkReader.ReadChunk drops the whole chunk list once its cursor
// passes the end, whether or not each chunk was removed, and nothing puts an
// un-removed chunk back. Size() then stays > 0 while DequeueBatch() returns
// nil, so those vectors are unindexed until a restart re-scans the directory.
// PauseQueue (a backup pauses every queue) and Close both cancel parked batches.
func TestDiskQueue_CanceledBatchStaysDequeuable(t *testing.T) {
	// not started on purpose: RegisterQueue is then a no-op, so nothing
	// dispatches in the background and the dequeue sequence is deterministic
	s := makeScheduler(t)
	defer func() { require.NoError(t, s.Close(t.Context())) }()

	q := makeStaleQueue(t, s)
	defer func() { _ = q.Close(t.Context()) }()

	pushMany(t, q, 1, 100, 200, 300)

	first := dequeueEventually(t, q, "the pushed records must be dequeued at least once")
	require.Len(t, first.Tasks, 3)
	require.EqualValues(t, 3, q.Size(),
		"records stay counted until the batch is marked done")

	// the worker gave the batch back instead of completing it: OnDone is never
	// called, so the chunk is deliberately left on disk
	first.Cancel()

	require.EqualValues(t, 3, q.Size(),
		"a cancelled batch must not drop its records from the queue's accounting")

	second := dequeueEventually(t, q,
		"a cancelled batch's chunk must stay dequeuable in-process: its records are "+
			"still counted by Size() and still on disk, so leaving them unreachable "+
			"until the next restart silently stalls indexing for those objects")
	require.Len(t, second.Tasks, 3,
		"the same tasks must come back, so the vectors are eventually indexed")
}

// makeStaleQueue builds a disk queue whose partial chunk is promoted almost
// immediately, so a test need not wait out the default stale timeout.
func makeStaleQueue(t *testing.T, s *Scheduler) *DiskQueue {
	t.Helper()

	q, err := NewDiskQueue(DiskQueueOptions{
		ID:           "canceled_batch_queue",
		Scheduler:    s,
		Logger:       newTestLogger(),
		Dir:          t.TempDir(),
		TaskDecoder:  discardExecutor(),
		StaleTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, q.Init())

	return q
}

// dequeueEventually polls DequeueBatch until it yields a batch, because a
// partial chunk is only promoted once it has been stale for StaleTimeout. It is
// not a tolerance for the behaviour under test.
func dequeueEventually(t *testing.T, q *DiskQueue, msg string) *Batch {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, err := q.DequeueBatch()
		require.NoError(t, err)
		if b != nil && len(b.Tasks) > 0 {
			return b
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("no batch was dequeued within the deadline: %s (queue reports %d pending records)", msg, q.Size())
	return nil
}
