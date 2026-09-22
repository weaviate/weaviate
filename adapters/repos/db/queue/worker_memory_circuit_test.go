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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A batch that can only fail with memory sheds must be parked, not retried forever or dropped.
const (
	// safety net: the test fails if the loop never terminates
	memoryShedRetryBudget = 5 * time.Second

	memoryShedMaxAttempts = int32(maxMemoryPressureAttempts)

	// replaces the production pause so the requeue delay is assertable
	memoryShedTestPause = 42 * time.Second
)

// Parked means never marked done, so the disk queue keeps the chunk.
func TestWorker_ParksBatchAfterBoundedMemoryPressureRetries(t *testing.T) {
	tests := []struct {
		name string
		// consecutive failures before the task succeeds; -1 means never
		failures     int32
		err          error
		wantPark     bool
		wantAttempts int32
	}{
		{
			name:         "not enough memory never recovers",
			failures:     -1,
			err:          enterrors.NewNotEnoughMemory("add batch of 1000 vectors"),
			wantPark:     true,
			wantAttempts: memoryShedMaxAttempts,
		},
		{
			// same shape, different sentinel
			name:         "not enough memory mappings never recovers",
			failures:     -1,
			err:          enterrors.ErrNotEnoughMappings,
			wantPark:     true,
			wantAttempts: memoryShedMaxAttempts,
		},
		{
			// control: a bound must not disable legitimate retries
			name:         "single memory blip still recovers",
			failures:     1,
			err:          enterrors.NewNotEnoughMemory("transient blip"),
			wantAttempts: 2,
		},
		{
			// control: the bound must not cut short a batch that recovers on the last rung
			name:         "recovery on the last allowed attempt still succeeds",
			failures:     memoryShedMaxAttempts - 1,
			err:          enterrors.NewNotEnoughMemory("prolonged blip"),
			wantAttempts: memoryShedMaxAttempts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger, _ := test.NewNullLogger()
			w := &Worker{
				logger: logger,
				// exercise the policy without sleeping through the real ladder
				backoffFn:        func(int) time.Duration { return time.Millisecond },
				memPressurePause: memoryShedTestPause,
			}

			// the deferred cancel releases the worker if the loop never gives up
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var execCnt int32
			remaining := tt.failures
			task := &mockWorkerTask{
				executeFunc: func(context.Context) error {
					n := atomic.AddInt32(&execCnt, 1)
					if tt.failures < 0 || n <= remaining {
						return tt.err
					}
					return nil
				},
			}

			var (
				markedDone     atomic.Bool
				markedCanceled atomic.Bool
				requeued       atomic.Bool
				requeuedAfter  atomic.Int64
			)

			batch := &Batch{
				Ctx:        ctx,
				Tasks:      []Task{task},
				OnDone:     func() { markedDone.Store(true) },
				OnCanceled: func() { markedCanceled.Store(true) },
				OnRequeue: func(after time.Duration) {
					requeued.Store(true)
					requeuedAfter.Store(int64(after))
				},
			}

			done := make(chan error, 1)
			enterrors.GoWrapper(func() { done <- w.do(batch) }, logger)

			select {
			case err := <-done:
				if !tt.wantPark {
					require.NoError(t, err, "a recoverable batch must still complete")
					require.Equal(t, tt.wantAttempts, atomic.LoadInt32(&execCnt),
						"transient failures must still be retried")
					assert.True(t, markedDone.Load(), "a completed batch must be marked done")
					assert.False(t, requeued.Load(), "a completed batch must not be requeued")
					return
				}

				require.ErrorIs(t, err, errMemoryPressurePause,
					"the worker must report that it parked the batch, not that it failed it")
				require.Equal(t, tt.wantAttempts, atomic.LoadInt32(&execCnt),
					"a batch that can never succeed must not be re-executed more than %d times",
					memoryShedMaxAttempts)

				assert.False(t, markedDone.Load(),
					"a parked batch must never be marked done: that is what lets the disk "+
						"queue delete the chunk holding these vectors")
				assert.False(t, markedCanceled.Load(),
					"a parked batch must not be discarded: the objects exist, so dropping "+
						"their vectors is silent data loss")
				require.True(t, requeued.Load(),
					"the worker must hand the batch back so it is retried once memory frees")
				assert.Equal(t, int64(memoryShedTestPause), requeuedAfter.Load(),
					"the batch must be requeued with the configured long pause, not the "+
						"30s backoff ceiling")
				require.Len(t, batch.Tasks, 1,
					"the still-failing task must remain pending on the parked batch")
				require.Same(t, task, batch.Tasks[0])
			case <-time.After(memoryShedRetryBudget):
				t.Fatalf("worker is still retrying a batch that can never succeed: "+
					"%d attempts in %s and counting, with no bound on attempts or elapsed time",
					atomic.LoadInt32(&execCnt), memoryShedRetryBudget)
			}
		})
	}
}

// The scheduler must hand a parked batch back once its pause elapsed.
func TestScheduler_ParkedBatchIsRedispatched(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer func() { require.NoError(t, s.Close(t.Context())) }()

	q := makeQueue(t, s, discardExecutor())
	defer func() { _ = q.Close(t.Context()) }()

	qs := s.getQueue(q.ID())
	require.NotNil(t, qs)

	executed := make(chan struct{}, 1)
	done := make(chan struct{})

	batch := &Batch{
		Ctx: qs.ctx,
		Tasks: []Task{&mockWorkerTask{
			executeFunc: func(context.Context) error {
				executed <- struct{}{}
				return nil
			},
		}},
		OnDone: func() { close(done) },
	}

	s.parkBatch(qs, batch, 10*time.Millisecond)

	select {
	case <-executed:
	case <-time.After(5 * time.Second):
		t.Fatal("a parked batch was never handed back to a worker: its tasks would be lost")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the re-dispatched batch was never marked done")
	}
}

// Pausing a queue must not wait out a parked batch's pause.
func TestScheduler_PauseReleasesParkedBatch(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer func() { require.NoError(t, s.Close(t.Context())) }()

	q := makeQueue(t, s, discardExecutor())
	defer func() { _ = q.Close(t.Context()) }()

	qs := s.getQueue(q.ID())
	require.NotNil(t, qs)

	var markedDone atomic.Bool
	batch := &Batch{
		Ctx:    qs.ctx,
		Tasks:  []Task{&mockWorkerTask{}},
		OnDone: func() { markedDone.Store(true) },
		OnCanceled: func() {
			qs.activeTasks.Decr()
			s.activeTasks.Decr()
		},
	}

	// mirror what dispatchQueue does: the batch holds the gauges Pause waits on
	s.activeTasks.Incr()
	qs.activeTasks.Incr()
	s.parkBatch(qs, batch, time.Hour)

	start := time.Now()
	require.NoError(t, q.Pause(t.Context()))
	assert.Less(t, time.Since(start), 5*time.Second,
		"pausing must release parked batches instead of waiting out their pause")

	assert.False(t, markedDone.Load(),
		"a released batch must never be marked done: its tasks were not indexed")

	q.Resume()
}

// Shutdown must neither wait out the pause nor mark a parked batch done.
func TestScheduler_CloseReleasesParkedBatch(t *testing.T) {
	s := makeScheduler(t)
	s.Start()

	q := makeQueue(t, s, discardExecutor())

	qs := s.getQueue(q.ID())
	require.NotNil(t, qs)

	var (
		markedDone     atomic.Bool
		markedCanceled atomic.Bool
	)

	batch := &Batch{
		Ctx:        qs.ctx,
		Tasks:      []Task{&mockWorkerTask{}},
		OnDone:     func() { markedDone.Store(true) },
		OnCanceled: func() { markedCanceled.Store(true) },
	}

	// park it far beyond the test's patience
	s.parkBatch(qs, batch, time.Hour)

	start := time.Now()
	require.NoError(t, s.Close(t.Context()))
	assert.Less(t, time.Since(start), 5*time.Second,
		"Close must release parked batches instead of waiting out their pause")

	assert.True(t, markedCanceled.Load(), "a parked batch must be released on shutdown")
	assert.False(t, markedDone.Load(),
		"a released batch must never be marked done: its tasks were not indexed")
}
