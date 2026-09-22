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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// newManualScheduler is a started scheduler without workers or ticker; the test drains ch itself.
func newManualScheduler(t *testing.T, workers, chanCap int) (*Scheduler, chan *Batch) {
	t.Helper()

	s := makeScheduler(t, workers)
	s.ctx, s.cancelFn = context.WithCancel(context.Background())
	ch := make(chan *Batch, chanCap)
	s.chans = []chan *Batch{ch}

	return s, ch
}

// newSealedQueue creates a queue in dir holding one sealed chunk with the given record ids.
func newSealedQueue(t *testing.T, s *Scheduler, dir string, ids ...uint64) *DiskQueue {
	t.Helper()

	q, err := NewDiskQueue(DiskQueueOptions{
		ID:           "requeue_queue",
		Scheduler:    s,
		Logger:       newTestLogger(),
		Dir:          dir,
		TaskDecoder:  discardExecutor(),
		StaleTimeout: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, q.Init())
	s.RegisterQueue(q)

	if len(ids) > 0 {
		pushMany(t, q, 1, ids...)
		_, err = q.ForceSwitch(t.Context(), dir)
		require.NoError(t, err)
	}

	return q
}

func chunkFiles(t *testing.T, dir string) (chunks, quarantined []string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		switch {
		case chunkFilePattern.MatchString(e.Name()):
			chunks = append(chunks, e.Name())
		case filepath.Ext(e.Name()) == ".corrupt":
			quarantined = append(quarantined, e.Name())
		}
	}

	return chunks, quarantined
}

func ownedChunks(q *DiskQueue) int {
	q.r.m.Lock()
	defer q.r.m.Unlock()

	return len(q.r.chunkList)
}

func receiveBatches(t *testing.T, ch chan *Batch, n int) []*Batch {
	t.Helper()

	wbs := make([]*Batch, 0, n)
	for range n {
		select {
		case wb := <-ch:
			wbs = append(wbs, wb)
		case <-time.After(5 * time.Second):
			t.Fatalf("expected %d worker batches, got %d", n, len(wbs))
		}
	}

	return wbs
}

// fakeClock is a settable clock for the chunk reader.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(q *DiskQueue) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Now().UnixNano())
	q.r.m.Lock()
	q.r.now = c.Now
	q.r.m.Unlock()
	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// cancelUntilParked cancels the front chunk's batch until it is one past maxChunkRequeues.
func cancelUntilParked(t *testing.T, q *DiskQueue, wantTasks int) {
	t.Helper()

	for i := range maxChunkRequeues + 1 {
		b, err := q.DequeueBatch()
		require.NoError(t, err)
		require.NotNil(t, b, "cancellation %d: a re-armed chunk must be dequeuable again", i)
		require.Len(t, b.Tasks, wantTasks)
		b.Cancel()
	}
}

func taskKeys(b *Batch) []uint64 {
	keys := make([]uint64, 0, len(b.Tasks))
	for _, t := range b.Tasks {
		keys = append(keys, t.(*mockTask).key)
	}
	return keys
}

// A chunk canceled while its queue runs is parked after maxChunkRequeues, never dropped; pause and shutdown never count.
func TestDiskQueue_CanceledChunkRequeueIsBounded(t *testing.T) {
	tests := []struct {
		name string
		// setup puts the scheduler in the state under test
		setup    func(t *testing.T, s *Scheduler, q *DiskQueue)
		wantPark bool
	}{
		{
			name:     "running queue parks after the bound",
			setup:    func(*testing.T, *Scheduler, *DiskQueue) {},
			wantPark: true,
		},
		{
			name: "paused queue never parks",
			setup: func(_ *testing.T, s *Scheduler, q *DiskQueue) {
				s.PauseQueue(q.ID())
			},
		},
		{
			name: "closed scheduler never parks",
			setup: func(t *testing.T, s *Scheduler, _ *DiskQueue) {
				require.NoError(t, s.Close(t.Context()))
			},
		},
		{
			name: "unregistered queue never parks",
			setup: func(t *testing.T, s *Scheduler, q *DiskQueue) {
				s.UnregisterQueue(t.Context(), q.ID())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newManualScheduler(t, 1, 1)
			defer func() { _ = s.Close(t.Context()) }()

			dir := t.TempDir()
			q := newSealedQueue(t, s, dir, 100, 200, 300)
			defer func() { _ = q.Close(t.Context()) }()
			clock := newFakeClock(q)

			tt.setup(t, s, q)

			// the last cancellation is the first one past the bound
			cancelUntilParked(t, q, 3)

			chunks, quarantined := chunkFiles(t, dir)
			require.EqualValues(t, 3, q.Size(), "the chunk's records must stay counted")
			require.Len(t, chunks, 1)
			require.Empty(t, quarantined, "a canceled chunk must never be quarantined")
			require.Equal(t, 1, ownedChunks(q), "the reader must keep owning the chunk")

			if !tt.wantPark {
				b, err := q.DequeueBatch()
				require.NoError(t, err)
				require.NotNil(t, b, "a chunk canceled by pause or shutdown must stay dequeuable")
				require.Len(t, b.Tasks, 3)
				return
			}

			b, err := q.DequeueBatch()
			require.NoError(t, err)
			require.Nil(t, b, "a parked chunk must not be dequeued before its pause elapses")

			clock.Advance(chunkRequeuePause - time.Nanosecond)
			b, err = q.DequeueBatch()
			require.NoError(t, err)
			require.Nil(t, b, "a parked chunk must not be dequeued before its pause elapses")
			require.EqualValues(t, 3, q.Size())

			clock.Advance(time.Nanosecond)

			// a fresh requeue count: a full set of attempts before parking again
			cancelUntilParked(t, q, 3)
			b, err = q.DequeueBatch()
			require.NoError(t, err)
			require.Nil(t, b, "the chunk must be parked again after another full set of attempts")

			clock.Advance(chunkRequeuePause)
			b, err = q.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, b)
			require.Equal(t, []uint64{100, 200, 300}, taskKeys(b))
			b.Done()

			chunks, quarantined = chunkFiles(t, dir)
			require.EqualValues(t, 0, q.Size())
			require.Empty(t, chunks)
			require.Empty(t, quarantined)
			require.Zero(t, ownedChunks(q))
		})
	}
}

// A parked chunk blocks the whole queue: later chunks must not overtake it and reorder index operations.
func TestDiskQueue_ParkedChunkIsNotOvertaken(t *testing.T) {
	tests := []struct {
		name string
		// behind puts chunk B with record 300 behind the parked chunk A
		behind func(t *testing.T, q *DiskQueue, dir string)
	}{
		{
			name: "sealed chunk behind",
			behind: func(t *testing.T, q *DiskQueue, dir string) {
				pushMany(t, q, 1, 300)
				_, err := q.ForceSwitch(t.Context(), dir)
				require.NoError(t, err)
			},
		},
		{
			name: "stale partial chunk behind",
			behind: func(t *testing.T, q *DiskQueue, _ string) {
				pushMany(t, q, 1, 300)
				q.m.Lock()
				q.staleTimeout = time.Nanosecond
				q.m.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newManualScheduler(t, 1, 1)
			defer func() { _ = s.Close(t.Context()) }()

			dir := t.TempDir()
			q := newSealedQueue(t, s, dir, 100, 200)
			defer func() { _ = q.Close(t.Context()) }()
			clock := newFakeClock(q)

			cancelUntilParked(t, q, 2)
			tt.behind(t, q, dir)
			require.EqualValues(t, 3, q.Size())

			for range 3 {
				b, err := q.DequeueBatch()
				require.NoError(t, err)
				require.Nil(t, b, "nothing may be served while the front chunk is parked")
			}

			clock.Advance(chunkRequeuePause)

			b, err := q.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, b)
			require.Equal(t, []uint64{100, 200}, taskKeys(b), "the parked chunk must be served first")
			b.Done()

			b, err = q.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, b)
			require.Equal(t, []uint64{300}, taskKeys(b))
			b.Done()

			require.EqualValues(t, 0, q.Size())
		})
	}
}

// A parked chunk blocks neither pause, unregister nor close, and is served again after a restart.
func TestDiskQueue_ParkedChunkSurvivesRestart(t *testing.T) {
	s, _ := newManualScheduler(t, 1, 1)
	defer func() { _ = s.Close(t.Context()) }()

	dir := t.TempDir()
	q := newSealedQueue(t, s, dir, 100, 200)
	newFakeClock(q)

	cancelUntilParked(t, q, 2)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, q.Pause(ctx), "a parked chunk must not block pause")
	q.Resume()
	s.UnregisterQueue(ctx, q.ID())
	require.NoError(t, q.Close(ctx), "a parked chunk must not block close")

	chunks, quarantined := chunkFiles(t, dir)
	require.Len(t, chunks, 1)
	require.Empty(t, quarantined)

	s2 := makeScheduler(t)
	defer func() { _ = s2.Close(t.Context()) }()
	q2 := newSealedQueue(t, s2, dir)
	defer func() { _ = q2.Close(t.Context()) }()

	require.EqualValues(t, 2, q2.Size())
	b, err := q2.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, b, "the park is in memory only: a restart serves the chunk again")
	require.Equal(t, []uint64{100, 200}, taskKeys(b))
}

// A chunk canceled once and then processed is removed like any other.
func TestDiskQueue_RequeuedChunkIsRemovedOnDone(t *testing.T) {
	s, _ := newManualScheduler(t, 1, 1)
	defer func() { _ = s.Close(t.Context()) }()

	dir := t.TempDir()
	q := newSealedQueue(t, s, dir, 100, 200)
	defer func() { _ = q.Close(t.Context()) }()

	b, err := q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, b)
	b.Cancel()
	// a settled batch ignores a late Done: only the re-dequeued batch may delete the chunk
	b.Done()
	require.EqualValues(t, 2, q.Size())

	b, err = q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Len(t, b.Tasks, 2)
	b.Done()

	chunks, quarantined := chunkFiles(t, dir)
	require.EqualValues(t, 0, q.Size())
	require.Empty(t, chunks)
	require.Empty(t, quarantined)
	require.Zero(t, ownedChunks(q))
}

// A batch settles once every partition has: done if all were, canceled otherwise; parked ones stay open.
func TestScheduler_BatchSettlesWhenAllPartitionsSettle(t *testing.T) {
	type want int
	const (
		wantPending want = iota
		wantCanceled
		wantDone
	)

	// ids 100 and 101 land in different partitions with two workers
	done := func(i int) func(*testing.T, *Scheduler, []*Batch) {
		return func(_ *testing.T, _ *Scheduler, wbs []*Batch) { wbs[i].Done() }
	}
	cancel := func(i int) func(*testing.T, *Scheduler, []*Batch) {
		return func(_ *testing.T, _ *Scheduler, wbs []*Batch) { wbs[i].Cancel() }
	}
	park := func(i int) func(*testing.T, *Scheduler, []*Batch) {
		return func(_ *testing.T, _ *Scheduler, wbs []*Batch) { wbs[i].Requeue(time.Hour) }
	}

	tests := []struct {
		name  string
		steps []func(*testing.T, *Scheduler, []*Batch)
		want  want
	}{
		{name: "all done", steps: []func(*testing.T, *Scheduler, []*Batch){done(0), done(1)}, want: wantDone},
		{name: "first canceled", steps: []func(*testing.T, *Scheduler, []*Batch){cancel(0), done(1)}, want: wantCanceled},
		{name: "last canceled", steps: []func(*testing.T, *Scheduler, []*Batch){done(0), cancel(1)}, want: wantCanceled},
		{name: "all canceled", steps: []func(*testing.T, *Scheduler, []*Batch){cancel(0), cancel(1)}, want: wantCanceled},
		{name: "one done, one unsettled", steps: []func(*testing.T, *Scheduler, []*Batch){done(0)}, want: wantPending},
		{name: "one done, one parked", steps: []func(*testing.T, *Scheduler, []*Batch){done(0), park(1)}, want: wantPending},
		{
			name: "parked partition released by pause",
			steps: []func(*testing.T, *Scheduler, []*Batch){done(0), park(1), func(_ *testing.T, s *Scheduler, _ []*Batch) {
				s.PauseQueue("requeue_queue")
				s.ResumeQueue("requeue_queue")
			}},
			want: wantCanceled,
		},
		{
			name: "parked partition re-dispatched and done",
			steps: []func(*testing.T, *Scheduler, []*Batch){done(0), func(t *testing.T, s *Scheduler, wbs []*Batch) {
				wbs[1].Requeue(0)
				s.dispatchParked()
				redispatched := receiveBatches(t, s.chans[0], 1)
				require.Same(t, wbs[1], redispatched[0])
				redispatched[0].Done()
			}},
			want: wantDone,
		},
		{
			name:  "settling twice does not settle the batch twice",
			steps: []func(*testing.T, *Scheduler, []*Batch){done(0), done(0), cancel(0)},
			want:  wantPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ch := newManualScheduler(t, 2, 2)
			defer func() { _ = s.Close(t.Context()) }()

			dir := t.TempDir()
			q := newSealedQueue(t, s, dir, 100, 101)
			defer func() { _ = q.Close(t.Context()) }()

			qs := s.getQueue(q.ID())
			require.NotNil(t, qs)

			count, err := s.dispatchQueue(qs)
			require.NoError(t, err)
			require.EqualValues(t, 2, count)
			wbs := receiveBatches(t, ch, 2)

			for _, step := range tt.steps {
				step(t, s, wbs)
			}
			// release whatever the case left unsettled so Close does not wait on it
			defer func() {
				for _, wb := range wbs {
					wb.Cancel()
				}
			}()

			chunks, _ := chunkFiles(t, dir)
			switch tt.want {
			case wantDone:
				require.EqualValues(t, 0, q.Size())
				require.Empty(t, chunks, "a done batch must delete its chunk")
				b, err := q.DequeueBatch()
				require.NoError(t, err)
				require.Nil(t, b)
			case wantCanceled:
				require.EqualValues(t, 2, q.Size(), "a canceled batch must keep its records counted")
				require.Len(t, chunks, 1, "a canceled batch must keep its chunk on disk")
				b, err := q.DequeueBatch()
				require.NoError(t, err)
				require.NotNil(t, b, "a canceled batch's chunk must be dequeuable again")
				require.Len(t, b.Tasks, 2, "all tasks must come back, including the done partition's")
			case wantPending:
				require.EqualValues(t, 2, q.Size())
				require.Len(t, chunks, 1)
				b, err := q.DequeueBatch()
				require.NoError(t, err)
				require.Nil(t, b, "an unsettled batch's chunk must not be dispatched twice")
			}
		})
	}
}

// A parked partition released by shutdown leaves the chunk on disk for the next start.
func TestScheduler_ParkedBatchReleasedOnCloseKeepsChunk(t *testing.T) {
	tests := []struct {
		name  string
		close func(t *testing.T, s *Scheduler, q *DiskQueue)
	}{
		{
			name: "queue closed first",
			close: func(t *testing.T, s *Scheduler, q *DiskQueue) {
				require.NoError(t, q.Close(t.Context()))
				require.NoError(t, s.Close(t.Context()))
			},
		},
		{
			name: "scheduler closed first",
			close: func(t *testing.T, s *Scheduler, q *DiskQueue) {
				require.NoError(t, s.Close(t.Context()))
				require.NoError(t, q.Close(t.Context()))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ch := newManualScheduler(t, 2, 2)

			dir := t.TempDir()
			q := newSealedQueue(t, s, dir, 100, 101)

			qs := s.getQueue(q.ID())
			require.NotNil(t, qs)

			_, err := s.dispatchQueue(qs)
			require.NoError(t, err)
			wbs := receiveBatches(t, ch, 2)

			wbs[0].Done()
			wbs[1].Requeue(time.Hour)

			chunks, _ := chunkFiles(t, dir)
			require.Len(t, chunks, 1, "a parked partition must not let the chunk be deleted")

			tt.close(t, s, q)

			chunks, quarantined := chunkFiles(t, dir)
			require.Len(t, chunks, 1, "the chunk must survive shutdown")
			require.Empty(t, quarantined)

			// the next start picks the records up again
			s2 := makeScheduler(t)
			defer func() { _ = s2.Close(t.Context()) }()
			q2 := newSealedQueue(t, s2, dir)
			defer func() { _ = q2.Close(t.Context()) }()

			require.EqualValues(t, 2, q2.Size())
			b, err := q2.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, b)
			require.Len(t, b.Tasks, 2)
		})
	}
}

// A partition that cannot be handed to a worker cancels the batch instead of stranding it.
func TestScheduler_SendFailureCancelsBatch(t *testing.T) {
	// one slot: the second partition blocks until the queue is canceled
	s, ch := newManualScheduler(t, 2, 1)
	defer func() { _ = s.Close(t.Context()) }()

	dir := t.TempDir()
	q := newSealedQueue(t, s, dir, 100, 101)
	defer func() { _ = q.Close(t.Context()) }()

	qs := s.getQueue(q.ID())
	require.NotNil(t, qs)

	errCh := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := s.dispatchQueue(qs)
		errCh <- err
	}, newTestLogger())

	// wait for the first partition to occupy the slot, then fail the second send
	require.Eventually(t, func() bool { return len(ch) == 1 }, 5*time.Second, time.Millisecond)
	qs.cancelFn()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not give up on a canceled queue")
	}

	wbs := receiveBatches(t, ch, 1)
	wbs[0].Done()

	require.Zero(t, qs.activeTasks.Count(), "every partition must release its gauges")
	require.EqualValues(t, 2, q.Size())
	b, err := q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, b, "a batch whose partition was never sent must be re-armed, not stranded")
	require.Len(t, b.Tasks, 2)
}

// settleRecordingQueue serves one batch and records how it settles.
type settleRecordingQueue struct {
	batch    *Batch
	served   atomic.Bool
	done     atomic.Int32
	canceled atomic.Int32
}

func (f *settleRecordingQueue) ID() string        { return "settle_recording_queue" }
func (f *settleRecordingQueue) Metrics() *Metrics { return NewMetrics(newTestLogger(), nil, nil) }

func (f *settleRecordingQueue) Size() int64 {
	if f.served.Load() {
		return 0
	}
	return 1
}

func (f *settleRecordingQueue) DequeueBatch() (*Batch, error) {
	if f.served.Swap(true) {
		return nil, nil
	}
	f.batch.OnDone = func() { f.done.Add(1) }
	f.batch.OnCanceled = func() { f.canceled.Add(1) }
	return f.batch, nil
}

type panickingKeyTask struct{ mockWorkerTask }

func (panickingKeyTask) Key() uint64 { panic("simulated key panic") }

// A panic while partitioning a dequeued batch cancels it, so its chunk is re-armed.
func TestScheduler_DispatchPanicCancelsBatch(t *testing.T) {
	s, _ := newManualScheduler(t, 1, 1)
	defer func() { _ = s.Close(t.Context()) }()

	fq := &settleRecordingQueue{batch: &Batch{Tasks: []Task{&panickingKeyTask{}}}}
	s.RegisterQueue(fq)
	qs := s.getQueue(fq.ID())
	require.NotNil(t, qs)

	_, err := s.dispatchQueue(qs)
	require.Error(t, err)
	require.EqualValues(t, 1, fq.canceled.Load(), "the batch must be canceled")
	require.Zero(t, fq.done.Load(), "the batch must not be marked done")
}
