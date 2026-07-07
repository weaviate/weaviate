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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestWorkerDo_RetryOnTransient(t *testing.T) {
	logger, hook := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Task1: fail transiently 5 times, succeed on 6th execution
	// Backoff: 1s + 2s + 4s + 8s + 16s = 31s total
	t1 := newFakeTask("t1", 5)

	// Task2: succeeds immediately
	t2 := newFakeTask("t2", 0)

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{t1, t2},
	}

	err := w.do(batch)
	require.NoError(t, err, "batch should complete successfully")

	// t1 should have 6 executes: 5 transient OOM + 1 success
	require.Equal(t, int32(6), atomic.LoadInt32(&t1.execCnt), "t1 execution count mismatch")
	// Logger printed 5 warnings (1 per transient OOM)
	assert.Len(t, hook.Entries, 5, "expected 5 log entries for t1 transient failures")
	// t2 should run exactly once (on the first pass)
	require.Equal(t, int32(1), atomic.LoadInt32(&t2.execCnt), "t2 should execute once")
}

// fakeTask fails with a transient error `failures` times, then succeeds.
type fakeTask struct {
	name     string
	failures int32 // remaining failures before success
	execCnt  int32
}

func newFakeTask(name string, transientFailures int) *fakeTask {
	return &fakeTask{
		name:     name,
		failures: int32(transientFailures),
	}
}

func (f *fakeTask) Key() uint64 {
	return 0
}

func (f *fakeTask) Op() uint8 {
	return 0
}

func (t *fakeTask) Execute(ctx context.Context) error {
	atomic.AddInt32(&t.execCnt, 1)

	// Simulate transient errors first, then success
	if atomic.LoadInt32(&t.failures) > 0 {
		atomic.AddInt32(&t.failures, -1)
		return enterrors.NewNotEnoughMemory("simulated transient OOM")
	}
	return nil
}

type mockWorkerTask struct {
	executeFunc func(context.Context) error
}

func (m *mockWorkerTask) Execute(ctx context.Context) error {
	if m.executeFunc != nil {
		return m.executeFunc(ctx)
	}
	return nil
}

func (m *mockWorkerTask) Key() uint64 {
	return 0
}

func (m *mockWorkerTask) Op() uint8 {
	return 0
}

func TestWorker_TransientErrorRetryIndefinitely(t *testing.T) {
	tests := []struct {
		name    string
		errFunc func() error
	}{
		{
			name:    "OOM",
			errFunc: func() error { return enterrors.NewNotEnoughMemory("simulated OOM") },
		},
		{
			name:    "ReadOnly",
			errFunc: func() error { return storagestate.ErrStatusReadOnly },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			w := &Worker{
				logger: logger,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Task that fails 3 times with transient error, then succeeds
			failCount := int32(3)
			task := &mockWorkerTask{
				executeFunc: func(ctx context.Context) error {
					if atomic.LoadInt32(&failCount) > 0 {
						atomic.AddInt32(&failCount, -1)
						return tt.errFunc()
					}
					return nil
				},
			}

			batch := &Batch{
				Ctx:   ctx,
				Tasks: []Task{task},
			}

			start := time.Now()
			err := w.do(batch)
			duration := time.Since(start)

			require.NoError(t, err, "batch should succeed after retries")
			assert.Equal(t, int32(0), atomic.LoadInt32(&failCount), "all failures should be retried")

			// With 3 failures: 1s + 2s + 4s = 7s total backoff
			expectedMin := 7 * time.Second
			assert.GreaterOrEqual(t, duration, expectedMin, "should have exponential backoff")
		})
	}
}

func TestWorker_PermanentErrorFailImmediately(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	ctx := context.Background()

	task := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			return common.ErrWrongDimensions // permanent error
		},
	}

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{task},
	}

	start := time.Now()
	err := w.do(batch)
	duration := time.Since(start)

	require.NoError(t, err, "should return nil (discarded)")
	assert.Less(t, duration, 50*time.Millisecond, "should fail immediately without retry")
}

func TestWorker_MixedErrorsDiscardBatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	ctx := context.Background()

	// Task 1: Transient error
	task1ExecCount := int32(0)
	task1 := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			atomic.AddInt32(&task1ExecCount, 1)
			return enterrors.NewNotEnoughMemory("OOM")
		},
	}

	// Task 2: Permanent error
	task2ExecCount := int32(0)
	task2 := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			atomic.AddInt32(&task2ExecCount, 1)
			return errors.New("some permanent error")
		},
	}

	// Task 3: Would succeed
	task3ExecCount := int32(0)
	task3 := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			atomic.AddInt32(&task3ExecCount, 1)
			return nil
		},
	}

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{task1, task2, task3},
	}

	start := time.Now()
	err := w.do(batch)
	duration := time.Since(start)

	require.NoError(t, err, "should return nil (batch discarded)")
	assert.Less(t, duration, 50*time.Millisecond, "should discard immediately when permanent error present")

	// All tasks should execute once (first pass)
	assert.Equal(t, int32(1), atomic.LoadInt32(&task1ExecCount), "task1 should execute once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&task2ExecCount), "task2 should execute once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&task3ExecCount), "task3 should execute once")
}

func TestWorker_ExponentialBackoff(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	// Test calculateBackoff values
	testCases := []struct {
		attempts int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},     // Capped at 30s
		{10, 30 * time.Second},    // Still capped
		{100, 30 * time.Second},   // Still capped
		{1000, 30 * time.Second},  // Still capped
		{10000, 30 * time.Second}, // Still capped
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("attempt_%d", tc.attempts), func(t *testing.T) {
			actual := w.calculateBackoff(tc.attempts)
			assert.Equal(t, tc.expected, actual, "backoff duration mismatch")
		})
	}
}

// context.DeadlineExceeded is deliberately excluded from both the permanent
// and the transient classification. It must still take the backoff path:
// falling through both used to re-execute the failed tasks immediately, in a
// tight loop with no sleep and no exit, pegging the worker at 100% CPU.
func TestWorker_DeadlineExceededBacksOff(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// task that always fails with a deadline error, e.g. from an internal
	// timeout
	execCount := int32(0)
	task := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			atomic.AddInt32(&execCount, 1)
			return fmt.Errorf("internal timeout: %w", context.DeadlineExceeded)
		},
	}

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{task},
	}

	// cancel the batch context after 500ms: the retry loop must exit
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- w.do(batch)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "should return context error")
	case <-time.After(5 * time.Second):
		t.Fatal("worker is stuck retrying deadline errors in a tight loop")
	}

	// with a backoff after the first attempt, the task must not have been
	// re-executed before the context was canceled
	require.LessOrEqual(t, atomic.LoadInt32(&execCount), int32(2), "deadline errors must be retried with backoff, not in a tight loop")
}

// A panicking task must not kill the worker goroutine: it is never restarted,
// and the batch would never be marked done or canceled, leaving the queue's
// active tasks gauge stuck so the queue is never scheduled again.
func TestWorker_RecoversFromPanickingTask(t *testing.T) {
	logger, _ := test.NewNullLogger()

	worker, ch := NewWorker(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	enterrors.GoWrapper(func() { worker.Run(ctx) }, logger)

	canceled := make(chan struct{})
	panicking := &Batch{
		Ctx: ctx,
		Tasks: []Task{&mockWorkerTask{
			executeFunc: func(ctx context.Context) error {
				panic("simulated task panic")
			},
		}},
		OnCanceled: func() { close(canceled) },
	}

	select {
	case ch <- panicking:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not accept the batch")
	}

	// the batch must be marked canceled so the scheduler's gauges are released
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking batch was not marked canceled")
	}

	// the worker must survive and process subsequent batches
	done := make(chan struct{})
	healthy := &Batch{
		Ctx:    ctx,
		Tasks:  []Task{&mockWorkerTask{}},
		OnDone: func() { close(done) },
	}

	select {
	case ch <- healthy:
	case <-time.After(2 * time.Second):
		t.Fatal("worker died after a panicking task")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not process the batch after a panic")
	}
}

func TestWorker_ContextCancellationDuringRetry(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{
		logger: logger,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Task that always fails with transient error
	task := &mockWorkerTask{
		executeFunc: func(ctx context.Context) error {
			return enterrors.NewNotEnoughMemory("OOM")
		},
	}

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{task},
	}

	// Cancel context after 500ms
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := w.do(batch)
	duration := time.Since(start)

	require.Error(t, err, "should return context error")
	assert.True(t, errors.Is(err, context.Canceled), "should be context.Canceled")
	assert.Less(t, duration, 1*time.Second, "should exit quickly after cancel")
	assert.GreaterOrEqual(t, duration, 500*time.Millisecond, "should have run for at least 500ms")
}
