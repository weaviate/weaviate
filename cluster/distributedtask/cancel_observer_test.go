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

	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// TestManagerCancelObserver pins the apply-path hook a namespace needs to make
// "this node has seen the cancel" observable to its peers. Without it the
// earliest signal is the next scheduler tick, which is a minute away by default
// and far outside what a caller waiting on a cancel can budget for.
func TestManagerCancelObserver(t *testing.T) {
	const (
		namespace = "test"
		taskID    = "1"
		version   = uint64(10)
	)

	addCmd := func(t *testing.T, h *testHarness) *cmd.ApplyRequest {
		return toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             namespace,
			Id:                    taskID,
			Payload:               []byte(`{"collection":"Movies"}`),
			SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})
	}
	cancelCmd := func(t *testing.T, h *testHarness) *cmd.ApplyRequest {
		return toCmd(t, &cmd.CancelDistributedTaskRequest{
			Namespace:             namespace,
			Id:                    taskID,
			Version:               version,
			CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
		})
	}

	t.Run("a registered observer sees the cancelled task", func(t *testing.T) {
		h := newTestHarness(t).init(t)

		var observed []*Task
		h.manager.RegisterCancelObserver(namespace, func(task *Task) {
			observed = append(observed, task)
		})

		require.NoError(t, h.manager.AddTask(addCmd(t, h), version))
		require.Empty(t, observed, "adding a task must not fire the cancel observer")

		require.NoError(t, h.manager.CancelTask(cancelCmd(t, h)))

		require.Len(t, observed, 1, "the observer must fire as the cancel applies")
		require.Equal(t, taskID, observed[0].ID)
		require.Equal(t, namespace, observed[0].Namespace)
		require.Equal(t, version, observed[0].Version)
		require.Equal(t, TaskStatusCancelled, observed[0].Status,
			"the observer must see the task already in its cancelled state")
		require.JSONEq(t, `{"collection":"Movies"}`, string(observed[0].Payload),
			"the payload is what identifies the shards to gate")
	})

	t.Run("a namespace without an observer applies normally", func(t *testing.T) {
		h := newTestHarness(t).init(t)

		h.manager.RegisterCancelObserver("some-other-namespace", func(*Task) {
			require.Fail(t, "another namespace's observer must not fire")
		})

		require.NoError(t, h.manager.AddTask(addCmd(t, h), version))
		require.NoError(t, h.manager.CancelTask(cancelCmd(t, h)))
	})

	t.Run("nil and empty registrations are dropped", func(t *testing.T) {
		h := newTestHarness(t).init(t)

		h.manager.RegisterCancelObserver(namespace, nil)
		h.manager.RegisterCancelObserver("", func(*Task) {
			require.Fail(t, "an observer registered under an empty namespace must never fire")
		})

		require.NoError(t, h.manager.AddTask(addCmd(t, h), version))
		require.NoError(t, h.manager.CancelTask(cancelCmd(t, h)))
	})
}
