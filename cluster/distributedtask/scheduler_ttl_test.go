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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// A cancel stamped by a node whose clock ran far ahead must still age out.
func TestSchedulerTTLCountsFromFirstSeenFinished(t *testing.T) {
	cases := []struct {
		name             string
		restartAfterSeen time.Duration
	}{
		{name: "far-future finish time ages out a TTL after first seen"},
		{name: "restart resets first seen, delaying cleanup", restartAfterSeen: 12 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			const taskID = "1234"
			var version uint64 = 10
			require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             h.tasksNamespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			}), version))
			require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace:             h.tasksNamespace,
				Id:                    taskID,
				Version:               version,
				CancelledAtUnixMillis: h.clock.Now().AddDate(100, 0, 0).UnixMilli(),
			})))

			h.scheduler.tick()
			if tc.restartAfterSeen > 0 {
				h.clock.Advance(tc.restartAfterSeen)
				h.scheduler.tick()
				h.scheduler = NewScheduler(SchedulerParams{
					TaskLister:        h.manager,
					TaskCleaner:       h.cleaner,
					Providers:         h.registeredProviders,
					Clock:             h.clock,
					Logger:            h.logger,
					MetricsRegisterer: monitoring.NoopRegisterer,
					LocalNode:         h.localNodeID,
					CompletedTaskTTL:  h.completedTaskTTL,
					TickInterval:      h.schedulerTickInterval,
				})
				h.scheduler.tick()
			}
			h.clock.Advance(h.completedTaskTTL - time.Minute)
			h.scheduler.tick()
			require.Len(t, h.listManagerTasks(t)[h.tasksNamespace], 1)

			h.expectCleanUpTask(t, h.tasksNamespace, taskID, version)
			h.clock.Advance(time.Minute)
			h.scheduler.tick()
			require.Empty(t, h.listManagerTasks(t)[h.tasksNamespace])
		})
	}
}
