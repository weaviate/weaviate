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

package cluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// rConfig is a keyed composite literal, so omitting one of the four cleanup
// knobs still compiles. The omitted field is a nil *DynamicValue whose Get()
// returns the zero value, which disables the sweep with every other test green.
func TestReplicaMovementCleanupGetters(t *testing.T) {
	t.Run("an unwired bool knob falls back loudly", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		get := boolGetter(logger, nil, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)

		require.True(t, get(), "a nil knob must not silently read false")
		requireLoggedError(t, hook, "REPLICA_MOVEMENT_CLEANUP_ENABLED")
	})

	t.Run("an unwired duration knob falls back loudly", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		get := durationGetter(logger, nil, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)

		require.Equal(t, defaultReplicaMovementCleanupMaxAge, get(),
			"a nil knob must not silently read 0, which disables the sweep")
		requireLoggedError(t, hook, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE")
	})

	// Only the fallback path logs, so it is the only one a nil logger can trip.
	// New never reaches here with one, but a direct caller like this test can.
	t.Run("a nil logger falls back without panicking", func(t *testing.T) {
		require.True(t, boolGetter(nil, nil, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)())
		require.Equal(t, defaultReplicaMovementCleanupMaxAge,
			durationGetter(nil, nil, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)())
	})

	t.Run("a wired knob is read live, not snapshotted", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()

		enabled := runtime.NewDynamicValue(false)
		maxAge := runtime.NewDynamicValue(time.Hour)

		getEnabled := boolGetter(logger, enabled, "REPLICA_MOVEMENT_CLEANUP_ENABLED", true)
		getMaxAge := durationGetter(logger, maxAge, "REPLICA_MOVEMENT_CLEANUP_MAX_AGE", defaultReplicaMovementCleanupMaxAge)

		require.False(t, getEnabled())
		require.Equal(t, time.Hour, getMaxAge())

		// The reload loop's SetValue must be observed by the sweeper's next tick.
		enabled.SetValue(true)
		maxAge.SetValue(30 * time.Minute)

		require.True(t, getEnabled())
		require.Equal(t, 30*time.Minute, getMaxAge())
		require.Empty(t, hook.AllEntries(), "a wired knob must not log the not-wired error")
	})
}

type countingNotifier struct{ wakes atomic.Int32 }

func (n *countingNotifier) Wake() { n.wakes.Add(1) }

func TestService_SetDistributedTaskSchedulerNotifier(t *testing.T) {
	// A Store holding only a distributed task Manager is enough: AddTask wakes
	// the installed notifier without a Raft node.
	newService := func(planner *replication.RebalancePlanner) *Service {
		manager := distributedtask.NewManager(distributedtask.ManagerParameters{})
		return &Service{
			Raft:             &Raft{store: &Store{distributedTasksManager: manager}},
			rebalancePlanner: planner,
		}
	}

	logger, _ := logrustest.NewNullLogger()
	newPlanner := func(t *testing.T, ready func() bool) *replication.RebalancePlanner {
		t.Helper()
		planner, err := replication.NewRebalancePlanner(replication.RebalancePlannerParams{
			Logger:      logger,
			FSM:         replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry()),
			Proposer:    &Raft{},
			Drafting:    &Raft{},
			Clock:       clockwork.NewFakeClock(),
			Registerer:  prometheus.NewPedanticRegistry(),
			Schema:      schema.SchemaReader{},
			CurrentTerm: func() uint64 { return 0 },
			ReadyToPlan: ready,
			Enabled:     func() bool { return false },
		})
		require.NoError(t, err)
		return planner
	}

	t.Run("one wake reaches the scheduler and the planner", func(t *testing.T) {
		// Run calls RunOnce only after a wake, and RunOnce's first gate is
		// ReadyToPlan, so a send on gate proves the planner took a wake. The
		// send never blocks, so Run always returns once ctx is cancelled. If
		// Run panics, the deferred close still runs before GoWrapper recovers
		// the panic.
		gate := make(chan struct{}, 1)
		planner := newPlanner(t, func() bool {
			select {
			case gate <- struct{}{}:
			default:
			}
			return false
		})
		svc := newService(planner)
		scheduler := &countingNotifier{}
		svc.SetDistributedTaskSchedulerNotifier(scheduler)

		require.NoError(t, svc.store.distributedTasksManager.AddTask(exclusionTaskCommand(t, "ns"), 1))

		require.Equal(t, int32(1), scheduler.wakes.Load())
		ctx, cancel := context.WithCancel(context.Background())
		var runErr error
		runDone := make(chan struct{})
		enterrors.GoWrapper(func() {
			defer close(runDone)
			runErr = planner.Run(ctx)
		}, logger)
		defer func() {
			cancel()
			<-runDone
			require.ErrorIs(t, runErr, context.Canceled)
		}()
		select {
		case <-gate:
		case <-runDone:
			t.Fatal("Run exited before the planner took the wake")
		case <-time.After(10 * time.Second):
			t.Fatal("the planner did not take the wake")
		}
	})

	t.Run("a nil notifier with a planner disables wakes", func(t *testing.T) {
		// synctest.Wait returns only once Run is durably blocked. A wake from
		// AddTask would make Run call ReadyToPlan before it blocks again, so a zero
		// count proves no wake arrived. Run counts as durably blocked only if the
		// planner's wake channel is created inside the bubble.
		synctest.Test(t, func(t *testing.T) {
			var runs atomic.Int32
			planner := newPlanner(t, func() bool {
				runs.Add(1)
				return false
			})
			svc := newService(planner)
			svc.SetDistributedTaskSchedulerNotifier(nil)

			ctx, cancel := context.WithCancel(context.Background())
			runDone := make(chan error, 1)
			enterrors.GoWrapper(func() { runDone <- planner.Run(ctx) }, logger)
			defer func() {
				cancel()
				require.ErrorIs(t, <-runDone, context.Canceled)
			}()

			require.NotPanics(t, func() {
				require.NoError(t, svc.store.distributedTasksManager.AddTask(exclusionTaskCommand(t, "ns"), 1))
			})
			synctest.Wait()

			require.Zero(t, runs.Load(), "the planner must not be woken")
		})
	})

	t.Run("without a planner the scheduler is still woken", func(t *testing.T) {
		svc := newService(nil)
		scheduler := &countingNotifier{}
		svc.SetDistributedTaskSchedulerNotifier(scheduler)

		require.NoError(t, svc.store.distributedTasksManager.AddTask(exclusionTaskCommand(t, "ns"), 1))

		require.Equal(t, int32(1), scheduler.wakes.Load())
	})
}

func requireLoggedError(t *testing.T, hook *logrustest.Hook, knob string) {
	t.Helper()
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, knob) {
			return
		}
	}
	require.Failf(t, "missing log line", "an unwired %s must be reported at Error level, not silently defaulted", knob)
}
