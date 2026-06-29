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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrepareForBackup_DrainTimeout verifies that when a task blocks and the
// drain times out, PrepareForBackup returns an error containing "drain" and
// the queue ID, completes within a bounded time, leaves the queue NOT paused
// (Resume ran), and does NOT enable maintenance mode (snapshot must not proceed
// with in-flight tasks).
func TestPrepareForBackup_DrainTimeout(t *testing.T) {
	s := makeScheduler(t, 1)
	s.Start()
	defer s.Close(t.Context())

	// Create a blocking task that never completes
	blockCh := make(chan struct{})
	started := make(chan struct{})
	var taskStarted atomic.Bool

	decoder := &mockTaskDecoder{
		execFn: func(ctx context.Context, task *mockTask) error {
			if !taskStarted.Swap(true) {
				close(started)
			}
			// Block until test cleanup or context cancelled
			select {
			case <-blockCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	q := makeQueueSize(t, s, decoder, 50)
	defer func() {
		close(blockCh) // unblock task for cleanup
		q.Close(t.Context())
	}()

	// Push a task that will block
	pushMany(t, q, 1, 100)

	// Trigger scheduling and wait for the task to start executing
	s.Schedule(t.Context())
	select {
	case <-started:
		// Task is now blocking
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for blocking task to start")
	}

	// Call PrepareForBackup with a short timeout
	drainTimeout := 100 * time.Millisecond
	startTime := time.Now()

	err := q.PrepareForBackup(t.Context(), drainTimeout)
	elapsed := time.Since(startTime)

	// Assert: completes within bounded time (well under blocking task duration)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"PrepareForBackup should complete within bounded time, not wait for blocking task")

	// Assert: returns an error containing relevant information
	// NOTE: Current code has a bug where error is discarded. This assertion
	// catches the bug: it should fail until the fix is applied.
	if assert.Error(t, err, "PrepareForBackup should return error when drain times out") {
		assert.True(t, strings.Contains(err.Error(), "drain") ||
			strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "deadline"),
			"error should mention drain/timeout/deadline, got: %v", err)
	}

	// Assert: queue is NOT left paused (Resume ran via defer)
	assert.False(t, s.IsQueuePaused(q.id),
		"queue should not be left paused after PrepareForBackup error")

	// Assert: maintenance mode was NOT enabled
	// This is the key assertion that catches the discarded-error bug:
	// current code enables maintenance mode despite the timeout error.
	assert.False(t, q.maintenanceMode.Load(),
		"maintenance mode should NOT be enabled when drain fails - would create inconsistent snapshot")
}

// TestPrepareForBackup_NormalDrain verifies that with fast-completing tasks,
// PrepareForBackup succeeds, enables maintenance mode, and resumes the queue.
func TestPrepareForBackup_NormalDrain(t *testing.T) {
	s := makeScheduler(t, 1)
	s.Start()
	defer s.Close(t.Context())

	// Create fast-completing tasks
	decoder := &mockTaskDecoder{
		execFn: func(ctx context.Context, task *mockTask) error {
			return nil // complete immediately
		},
	}

	q := makeQueueSize(t, s, decoder, 50)
	defer q.Close(t.Context())

	// Push some tasks
	pushMany(t, q, 1, 100, 200, 300)

	// Trigger scheduling
	s.Schedule(t.Context())

	// Wait for tasks to complete
	err := q.Wait(t.Context())
	require.NoError(t, err)

	// Call PrepareForBackup with generous timeout
	err = q.PrepareForBackup(t.Context(), 5*time.Second)

	// Assert: succeeds
	require.NoError(t, err, "PrepareForBackup should succeed with fast-completing tasks")

	// Assert: maintenance mode is enabled
	assert.True(t, q.maintenanceMode.Load(),
		"maintenance mode should be enabled after successful PrepareForBackup")

	// Assert: queue is NOT left paused (Resume ran via defer)
	assert.False(t, s.IsQueuePaused(q.id),
		"queue should not be left paused after PrepareForBackup")
}

// TestPrepareForBackup_ZeroTimeoutMeansUnbounded verifies that with
// drainTimeout == 0 (no timeout), PrepareForBackup does NOT abort instantly.
// A quickly-completing drain should still succeed.
// drainTimeout == 0 means unbounded wait (honors only parent ctx deadline).
func TestPrepareForBackup_ZeroTimeoutMeansUnbounded(t *testing.T) {
	s := makeScheduler(t, 1)
	s.Start()
	defer s.Close(t.Context())

	// Create fast-completing tasks
	decoder := &mockTaskDecoder{
		execFn: func(ctx context.Context, task *mockTask) error {
			return nil // complete immediately
		},
	}

	q := makeQueueSize(t, s, decoder, 50)
	defer q.Close(t.Context())

	// Push some tasks
	pushMany(t, q, 1, 100, 200, 300)

	// Trigger scheduling and wait for completion
	s.Schedule(t.Context())
	err := q.Wait(t.Context())
	require.NoError(t, err)

	// Call PrepareForBackup with drainTimeout == 0 (unbounded wait).
	// The key assertion is that this does NOT abort instantly (as would happen
	// if we incorrectly wrapped with context.WithTimeout(ctx, 0)).
	err = q.PrepareForBackup(t.Context(), 0)

	// Assert: succeeds (does NOT abort instantly)
	require.NoError(t, err,
		"PrepareForBackup with zero timeout should NOT abort instantly; quick drain should succeed")

	// Assert: maintenance mode is enabled
	assert.True(t, q.maintenanceMode.Load(),
		"maintenance mode should be enabled after successful PrepareForBackup")
}
