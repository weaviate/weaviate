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

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// unknownFutureStatus stands in for a status a newer release introduced
// and this build has never heard of — what a mixed-version cluster sees
// during a rolling upgrade.
const unknownFutureStatus TaskStatus = "VALIDATING"

// TestCleanUpTask_RefusesUnknownStatus pins the FSM-side eviction guard.
// An unrecognized status has a zero FinishedAt, so the TTL comparison is
// trivially satisfied and only the liveness check stands between a live
// task and deletion from RAFT state.
func TestCleanUpTask_RefusesUnknownStatus(t *testing.T) {
	const (
		namespace = "test"
		taskID    = "1"
		version   = uint64(10)
	)

	for _, status := range []TaskStatus{unknownFutureStatus, TaskStatusPreparing, TaskStatusSwapping} {
		t.Run(string(status), func(t *testing.T) {
			h := newTestHarness(t).init(t)

			require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			}), version))
			h.manager.tasks[namespace][taskID].Status = status

			err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: namespace,
				Id:        taskID,
				Version:   version,
			}))
			require.ErrorContains(t, err, "still running")
		})
	}
}

// TestSchedulerTTLSweep_SkipsUnknownStatus pins that the sweep never
// issues a cleanup for a task in an unrecognized status. h.cleaner has no
// expectation set, so any CleanUpDistributedTask call fails the test.
func TestSchedulerTTLSweep_SkipsUnknownStatus(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))
	h.manager.tasks[h.tasksNamespace]["1"].Status = unknownFutureStatus

	h.startScheduler(t)
	defer h.Close()

	h.advanceClock(h.completedTaskTTL)
	h.advanceClock(h.schedulerTickInterval)

	require.Len(t, h.listManagerTasks(t)[h.tasksNamespace], 1,
		"a task in an unrecognized status must survive the TTL sweep")
}
