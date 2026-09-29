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

package distributedtask

// TestNoopProviderFrameworkGaps drives the framework gap fixes through the
// full scheduler loop using stub providers. Per-mechanism unit tests live
// in scheduler_framework_gaps_test.go and manager_force_terminate_test.go;
// these subtests add the end-to-end scheduler-harness path.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestNoopProviderFrameworkGaps(t *testing.T) {
	t.Run("node death mid-task exits via stale timeout", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			ForceTerminator:    ft,
			Providers:          map[string]Provider{"ns": &stubProvider{startTask: stubStartOK}},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       1 * time.Second,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-stale",
			UnitIds:               []string{"u1", "u2"},
			SubmittedAtUnixMillis: clock.Now().UnixMilli(),
			StaleTimeoutMs:        5000,
		}), 1)
		require.NoError(t, err)

		// Claim u1; leave u2 unclaimed to simulate a dead node.
		err = manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: "ns", Id: "task-stale", Version: 1,
			NodeId: "node-1", UnitId: "u1",
			Progress: 0.0, UpdatedAtUnixMillis: clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(100 * time.Millisecond)

		// First tick establishes watermarks.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 0, ft.callCount(), "no force-terminate yet")

		// Advance u1; u2 stays stale.
		err = manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: "ns", Id: "task-stale", Version: 1,
			NodeId: "node-1", UnitId: "u1",
			Progress: 0.5, UpdatedAtUnixMillis: clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// Advance past the 5s stale timeout.
		clock.Advance(6 * time.Second)
		time.Sleep(100 * time.Millisecond)

		assert.GreaterOrEqual(t, ft.callCount(), 1,
			"stale detector should have proposed force-terminate for u2")
	})

	t.Run("terminal cleanup retries across restart until it succeeds", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var callCount atomic.Int32
		provider := &terminalCleanupStubProvider{
			unitAwareStubProvider: unitAwareStubProvider{
				stubProvider: stubProvider{startTask: stubStartOK},
				onTaskCompleted: func(task *Task) error {
					n := callCount.Add(1)
					if n <= 2 {
						return fmt.Errorf("backend unreachable")
					}
					return nil
				},
			},
			cleanupDone: false,
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		// Create a FAILED task before scheduler starts (simulates restart).
		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-cleanup",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: clock.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = manager.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: "ns", Id: "task-cleanup", Version: 1,
			Reason:                  "test",
			RequestedTerminalStatus: string(TaskStatusFailed),
			TerminatedAtUnixMillis:  clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// New scheduler instance (bootstrap).
		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       1 * time.Second,
		})

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(100 * time.Millisecond)

		// Bootstrap skips the pre-mark because TerminalCleanupDone returns false.
		clock.Advance(1 * time.Second)
		time.Sleep(200 * time.Millisecond)
		assert.GreaterOrEqual(t, int(callCount.Load()), 1,
			"OnTaskCompleted should fire after bootstrap (not pre-marked)")

		// Re-fires because first attempt failed.
		clock.Advance(1 * time.Second)
		time.Sleep(200 * time.Millisecond)
		assert.GreaterOrEqual(t, int(callCount.Load()), 2)

		// Succeeds on the third attempt.
		clock.Advance(1 * time.Second)
		time.Sleep(200 * time.Millisecond)
		assert.GreaterOrEqual(t, int(callCount.Load()), 3)

		// No further re-fires after success.
		clock.Advance(1 * time.Second)
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, int32(3), callCount.Load(),
			"should stop re-firing after success")
	})
}
