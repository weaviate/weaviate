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
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/utils"
)

func TestRaftRebalanceDrafting(t *testing.T) {
	const ns = replicationTypes.RebalanceDraftNamespace
	srv, ctx, cleanup := startSingleNodeRaft(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()))
	defer cleanup()

	fsmTask := func(t *testing.T, planID string) *distributedtask.Task {
		t.Helper()
		for _, task := range srv.LocalDistributedTasks()[ns] {
			if task.ID == planID {
				return task
			}
		}
		require.FailNow(t, "no draft task", "plan %s", planID)
		return nil
	}
	draftTask := func(t *testing.T, planID string) replication.DraftTask {
		t.Helper()
		for _, task := range srv.RebalanceDraftTasks() {
			if task.PlanID == planID {
				return task
			}
		}
		require.FailNow(t, "no draft task", "plan %s", planID)
		return replication.DraftTask{}
	}

	t.Run("add carries the plan, one unit per sorted node, the stale timeout and the retry budget", func(t *testing.T) {
		require.NoError(t, srv.AddRebalanceDraftTask(ctx, "plan-1", []string{"node-3", "node-1", "node-2"}))

		// A task's version is the index of the log entry that added it.
		task := fsmTask(t, "plan-1")
		var entry raft.Log
		require.NoError(t, srv.store.logCache.GetLog(task.Version, &entry))
		var apply cmd.ApplyRequest
		require.NoError(t, proto.Unmarshal(entry.Data, &apply))
		require.Equal(t, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, apply.Type)
		var add cmd.AddDistributedTaskRequest
		require.NoError(t, json.Unmarshal(apply.SubCommand, &add))

		assert.Equal(t, ns, add.Namespace)
		assert.Equal(t, "plan-1", add.Id)
		assert.JSONEq(t, `{"planId":"plan-1"}`, string(add.Payload))
		assert.Equal(t, []*cmd.UnitSpec{{Id: "node-1"}, {Id: "node-2"}, {Id: "node-3"}}, add.UnitSpecs)
		assert.False(t, add.NeedsPreparationBarrier)
		assert.Equal(t, int64(10*60*1000), add.StaleTimeoutMs)
		assert.Equal(t, int32(draftMaxUnitRetries), add.MaxUnitRetries)

		assert.Equal(t, distributedtask.TaskStatusStarted, task.Status)
		assert.Equal(t, int64(10*60*1000), task.StaleTimeoutMs)
		assert.Equal(t, int32(3), task.MaxUnitRetries)
	})

	t.Run("terminate cancels the task, and is a no-op once it ended", func(t *testing.T) {
		require.NoError(t, srv.AddRebalanceDraftTask(ctx, "plan-2", []string{"node-1"}))
		version := fsmTask(t, "plan-2").Version

		require.NoError(t, srv.TerminateRebalanceDraftTask(ctx, "plan-2", version))
		require.NoError(t, srv.TerminateRebalanceDraftTask(ctx, "plan-2", version))

		task := fsmTask(t, "plan-2")
		assert.Equal(t, distributedtask.TaskStatusCancelled, task.Status)
		assert.Equal(t, "rebalance plan is no longer planning", task.Error)
	})

	t.Run("draft tasks map each unit status and the terminal flag", func(t *testing.T) {
		require.NoError(t, srv.AddRebalanceDraftTask(ctx, "plan-3", []string{"node-1", "node-2", "node-3", "node-4"}))
		version := fsmTask(t, "plan-3").Version
		require.NoError(t, srv.RecordDistributedTaskUnitCompletion(ctx, ns, "plan-3", version, "node-1", "node-1"))
		require.NoError(t, srv.UpdateDistributedTaskUnitProgress(ctx, ns, "plan-3", version, "node-3", "node-3", 0))

		assert.Equal(t, replication.DraftTask{
			PlanID:  "plan-3",
			Version: version,
			Units: map[string]replication.DraftUnitStatus{
				"node-1": replication.DraftUnitCompleted,
				"node-2": replication.DraftUnitPending,
				"node-3": replication.DraftUnitInProgress,
				"node-4": replication.DraftUnitPending,
			},
		}, draftTask(t, "plan-3"))

		require.NoError(t, srv.RecordDistributedTaskUnitFailure(ctx, ns, "plan-3", version, "node-2", "node-2", "disk read failed"))

		assert.Equal(t, replication.DraftTask{
			PlanID:   "plan-3",
			Version:  version,
			Terminal: true,
			Error:    "unit node-2 failed: disk read failed",
			Units: map[string]replication.DraftUnitStatus{
				"node-1": replication.DraftUnitCompleted,
				"node-2": replication.DraftUnitFailed,
				"node-3": replication.DraftUnitInProgress,
				"node-4": replication.DraftUnitPending,
			},
		}, draftTask(t, "plan-3"))
	})
}
