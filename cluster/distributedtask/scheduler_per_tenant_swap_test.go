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
	addTask := func(t *testing.T, h *barrierHarness, taskID string) {
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: h.namespace, Id: taskID, SubmittedAtUnixMillis: h.clock.Now().UnixMilli(), NeedsPreparationBarrier: true,
			UnitSpecs: []*cmd.UnitSpec{
				{Id: "a-1", GroupId: "tenant-a"}, {Id: "a-2", GroupId: "tenant-a"},
				{Id: "b-1", GroupId: "tenant-b"}, {Id: "b-2", GroupId: "tenant-b"},
			},
		}), 1))
	}
	complete := func(t *testing.T, h *barrierHarness, taskID string, units map[string]string) {
		for unit, node := range units {
			require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
				context.Background(), h.namespace, taskID, 1, node, unit))
		}
	}
	addTwoTenantTask := func(t *testing.T, h *barrierHarness, taskID string) {
		addTask(t, h, taskID)
		for unit, node := range map[string]string{"a-1": "node-1", "a-2": "node-2", "b-1": "node-1", "b-2": "node-2"} {
			require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
				context.Background(), h.namespace, taskID, 1, node, unit, 0.1))
		}
		h.tickAll()
		complete(t, h, taskID, map[string]string{"a-1": "node-1", "a-2": "node-2"})
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

	t.Run("a terminal task drops its undecided tenants after one tick", func(t *testing.T) {
		defer leaktest.Check(t)()
		h := newBarrierHarness(t, []string{"node-1", "node-2"})
		defer h.close()
		addTwoTenantTask(t, h, "T2")
		require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
			Namespace: h.namespace, Id: "T2", Version: 1, CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
		h.tickAll()
		for _, n := range h.nodes {
			n.scheduler.mu.Lock()
			pending := n.scheduler.perTaskState[h.getTask(t, "T2").TaskDescriptor].swapPending
			n.scheduler.mu.Unlock()
			require.Empty(t, pending, "%s: tenant-b can never be decided, so nothing is left to visit", n.id)
		}
	})

	// A decided swap must not wait behind another group's PREP batch in the same tick.
	t.Run("a decided swap runs before other groups' PREP", func(t *testing.T) {
		defer leaktest.Check(t)()
		h := newBarrierHarness(t, []string{"node-1", "node-2"})
		defer h.close()
		addTask(t, h, "T3")
		complete(t, h, "T3", map[string]string{"a-1": "node-1", "a-2": "node-2"})
		h.tick(0) // node-1 prepares tenant-a
		h.tick(1) // node-2 prepares tenant-a, which decides it
		require.True(t, h.getTask(t, "T3").GroupSwapDecided("tenant-a"))

		complete(t, h, "T3", map[string]string{"b-1": "node-1", "b-2": "node-2"})
		h.tick(0)
		require.Equal(t, []string{"prep:tenant-a", "swap:tenant-a", "prep:tenant-b"}, h.nodes[0].provider.calls,
			"node-1 swaps the decided group before preparing the next one")
	})
}
