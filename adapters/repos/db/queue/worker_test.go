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
	"syscall"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

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

// A recoverable error cancels the batch right away, without running the
// remaining tasks: the scheduler replays the whole batch on the queue's next
// turn, so the worker is not held while the error lasts.
func TestWorker_RecoverableErrorCancelsBatch(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "not enough memory", err: enterrors.NewNotEnoughMemory("simulated")},
		{name: "read only", err: storagestate.ErrStatusReadOnly},
		{name: "disk full", err: fmt.Errorf("write: %w", syscall.ENOSPC)},
		{
			name: "no usable entrypoint",
			err: errors.Wrap(fmt.Errorf("%w: local fallback exhausted", enterrors.ErrNoUsableEntrypoint),
				"find and connect neighbors"),
		},
		{name: "deadline exceeded", err: fmt.Errorf("internal timeout: %w", context.DeadlineExceeded)},
		{name: "internal cancellation", err: fmt.Errorf("aborted: %w", context.Canceled)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			w := &Worker{logger: logger}

			var executed []int
			task := func(i int, err error) Task {
				return &mockWorkerTask{executeFunc: func(context.Context) error {
					executed = append(executed, i)
					return err
				}}
			}

			var done, canceled int
			batch := &Batch{
				Ctx:        context.Background(),
				Tasks:      []Task{task(1, nil), task(2, tt.err), task(3, nil)},
				OnDone:     func() { done++ },
				OnCanceled: func() { canceled++ },
			}

			err := w.do(batch)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, []int{1, 2}, executed)
			require.Zero(t, done)
			require.Equal(t, 1, canceled)
		})
	}
}

// A batch with both permanent and recoverable errors is replayed: the tasks
// that hit a recoverable error must not be discarded with the others.
func TestWorker_MixedErrorsCancelBatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	w := &Worker{logger: logger}

	var done, canceled int
	batch := &Batch{
		Ctx: context.Background(),
		Tasks: []Task{
			&mockWorkerTask{executeFunc: func(context.Context) error { return errors.New("some permanent error") }},
			&mockWorkerTask{executeFunc: func(context.Context) error { return enterrors.NewNotEnoughMemory("OOM") }},
		},
		OnDone:     func() { done++ },
		OnCanceled: func() { canceled++ },
	}

	require.Error(t, w.do(batch))
	require.Zero(t, done)
	require.Equal(t, 1, canceled)
}

func TestWorker_CanceledBatchContext(t *testing.T) {
	logger, hook := test.NewNullLogger()
	w := &Worker{logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var canceled int
	batch := &Batch{
		Ctx: ctx,
		Tasks: []Task{&mockWorkerTask{executeFunc: func(ctx context.Context) error {
			return ctx.Err()
		}}},
		OnCanceled: func() { canceled++ },
	}

	require.ErrorIs(t, w.do(batch), context.Canceled)
	require.Equal(t, 1, canceled)
	require.Empty(t, hook.Entries, "closing a queue is not worth a warning")
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

	require.NoError(t, w.do(batch), "should return nil (discarded)")
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
