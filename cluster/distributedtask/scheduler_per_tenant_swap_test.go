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
	"context"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestPerTenantSwap(t *testing.T) {
	addTwoTenantTask := func(t *testing.T, h *barrierHarness, taskID string) {
		t.Helper()
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             h.namespace,
			Id:                    taskID,
			SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			UnitSpecs: []*cmd.UnitSpec{
				{Id: "a-1", GroupId: "tenant-a"},
				{Id: "a-2", GroupId: "tenant-a"},
				{Id: "b-1", GroupId: "tenant-b"},
				{Id: "b-2", GroupId: "tenant-b"},
			},
			NeedsPreparationBarrier: true,
		}), 1))
		for unit, node := range map[string]string{"a-1": "node-1", "a-2": "node-2", "b-1": "node-1", "b-2": "node-2"} {
			require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
				context.Background(), h.namespace, taskID, 1, node, unit, 0.1))
		}
		h.tickAll()
		for unit, node := range map[string]string{"a-1": "node-1", "a-2": "node-2"} {
			require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
				context.Background(), h.namespace, taskID, 1, node, unit))
		}
		h.tickAll()
		h.tickAll()
	}

	t.Run("a finished tenant swaps on every replica while another runs", func(t *testing.T) {
		defer leaktest.Check(t)()
		h := newBarrierHarness(t, []string{"node-1", "node-2"})
		defer h.close()

		addTwoTenantTask(t, h, "T1")

		task := h.getTask(t, "T1")
		require.Equal(t, TaskStatusStarted, task.Status, "tenant-b still runs")
		for _, n := range h.nodes {
			require.Equal(t, 1, n.provider.groupCount("T1"), "%s: tenant-a must have prepared", n.id)
			require.Equal(t, []string{"tenant-a"}, n.provider.swapGroups, "%s: only tenant-a may swap", n.id)
		}
	})

	t.Run("a tenant decided before another fails still swaps; the failed one never does", func(t *testing.T) {
		defer leaktest.Check(t)()
		h := newBarrierHarness(t, []string{"node-1", "node-2"})
		defer h.close()

		addTwoTenantTask(t, h, "T2")
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitFailure(
			context.Background(), h.namespace, "T2", 1, "node-1", "b-1", "boom"))
		h.tickAll()
		h.tickAll()

		require.Equal(t, TaskStatusFailed, h.getTask(t, "T2").Status)
		for _, n := range h.nodes {
			require.Equal(t, 1, n.provider.groupCount("T2"), "%s: tenant-a must have prepared before tenant-b failed", n.id)
			require.Equal(t, []string{"tenant-a"}, n.provider.swapGroups, "%s: tenant-a swaps, tenant-b never does", n.id)
		}
	})
}

// Pins that a swap decision survives a RAFT snapshot/restore, and that a
// snapshot predating decisions restores with none.
func TestPerTenantSwap_DecisionSurvivesSnapshot(t *testing.T) {
	h := newBarrierHarness(t, []string{"node-1", "node-2"})
	defer h.close()
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:               h.namespace,
		Id:                      "T",
		SubmittedAtUnixMillis:   h.clock.Now().UnixMilli(),
		UnitSpecs:               []*cmd.UnitSpec{{Id: "a-1", GroupId: "tenant-a"}, {Id: "b-1", GroupId: "tenant-b"}},
		NeedsPreparationBarrier: true,
	}), 1))
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, "T", 1, "node-1", "b-1", 0.1))
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, "T", 1, "node-1", "a-1"))
	h.tickAll()
	require.True(t, h.getTask(t, "T").GroupSwapDecided("tenant-a"))

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)
	restored := NewManager(ManagerParameters{Clock: h.clock, Logger: h.logger})
	require.NoError(t, restored.Restore(snap))
	tasks, err := restored.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, h.getTask(t, "T").SwapDecidedGroups, tasks[h.namespace][0].SwapDecidedGroups)

	legacy := []byte(`{"tasks":{"ns":[{"namespace":"ns","id":"old","version":1,"status":"FAILED",` +
		`"units":{"u":{"id":"u","groupId":"g","nodeId":"n","status":"COMPLETED"}}}]}}`)
	require.NoError(t, restored.Restore(legacy))
	tasks, err = restored.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.False(t, tasks["ns"][0].GroupSwapDecided("g"))
	require.Empty(t, tasks["ns"][0].StatusEntry().DecidedGroups)
}
