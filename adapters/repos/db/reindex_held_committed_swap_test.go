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

package db

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// A committed swap whose shard is held on its own migration fails the task
// instead of keeping it SWAPPING, so the collection can be deleted; the swap
// stays owed.
func TestAHeldCommittedSwapFailsTheTaskAndKeepsItsDebt(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-held-commit")
	node, unitID := fx.idx.getSchema.NodeName(), fx.subject.Key.UnitID
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  fx.logger,
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
	})
	payload := fx.subject.reindexPayload()
	payload.UnitToShard, payload.UnitToNode = map[string]string{unitID: fx.name}, map[string]string{unitID: node}
	restoreSwappingTask(t, mgr, fx.desc, payload, unitID, fx.name, node)
	// The node's applied task list decides the swap, so the unarmable record cannot be stamped.
	fx.idx.db.SetMigrationTaskSources(fx.ctx,
		func(shard string) ([]distributedtask.TaskStatusEntry, bool) {
			return mgr.LocalTaskStatuses(ReindexNamespace, shard), true
		},
		func(context.Context) ([]*distributedtask.Task, error) { return nil, errors.New("leader unreachable") })

	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.Len(t, shard.migrationMirrors.heldUnarmed(), 1)
	fx.idx.db.indexLock.Lock()
	fx.idx.db.indices[indexID(schema.ClassName(fx.className))] = fx.idx
	fx.idx.db.indexLock.Unlock()

	sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
		CompletionRecorder: &heldStepOutcome{done: make(chan string, 16)},
		AckRecorder:        managerAckRecorder{mgr},
		TaskLister:         mgr,
		Providers:          map[string]distributedtask.Provider{ReindexNamespace: fx.provider},
		Logger:             fx.logger, LocalNode: node, TickInterval: 50 * time.Millisecond,
	})
	require.NoError(t, sched.Start(fx.ctx))
	t.Cleanup(sched.Close)

	task := func() *distributedtask.Task {
		tasks, _ := mgr.ListDistributedTasks(fx.ctx)
		return tasks[ReindexNamespace][0]
	}
	require.Eventually(t, func() bool { sched.Wake(); return task().Status == distributedtask.TaskStatusFailed },
		10*time.Second, 20*time.Millisecond, "the held swap's failure ack fails the task")
	require.Contains(t, task().Error, "held READONLY")
	require.Equal(t, []string{node}, task().SwapsOwed[fx.name], "the swap stays owed")
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.NoError(t, mgr.CheckClassMutation(fx.className))
	require.NoError(t, mgr.CheckTenantMutation(fx.className, []string{fx.name}))
}

func restoreSwappingTask(t *testing.T, mgr *distributedtask.Manager, desc distributedtask.TaskDescriptor,
	payload ReindexTaskPayload, unitID, group, node string,
) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	type taskWithUnits struct {
		*distributedtask.Task
		Units map[string]*distributedtask.Unit `json:"units"`
	}
	snapshot, err := json.Marshal(map[string]map[string][]taskWithUnits{"tasks": {ReindexNamespace: {{
		Task: &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: desc, Payload: raw,
			Status: distributedtask.TaskStatusSwapping, NeedsPreparationBarrier: true,
			SwapDecidedGroups: map[string]time.Time{group: time.Now()},
			SwapsOwed:         map[string][]string{group: {node}},
		},
		Units: map[string]*distributedtask.Unit{unitID: {
			ID: unitID, GroupID: group, NodeID: node, Status: distributedtask.UnitStatusCompleted,
		}},
	}}}})
	require.NoError(t, err)
	require.NoError(t, mgr.Restore(snapshot))
}

// managerAckRecorder applies the scheduler's acks to the FSM, as a RAFT apply would.
type managerAckRecorder struct{ mgr *distributedtask.Manager }

func (r managerAckRecorder) RecordDistributedTaskPostCompletionAck(_ context.Context, namespace, taskID string,
	version uint64, groupID, nodeID string, success bool, errMsg string,
) error {
	sub, err := json.Marshal(&api.RecordDistributedTaskPostCompletionAckRequest{
		Namespace: namespace, Id: taskID, Version: version, GroupId: groupID, NodeId: nodeID,
		Success: success, Error: errMsg, AckedAtUnixMillis: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return r.mgr.RecordPostCompletionAck(&api.ApplyRequest{SubCommand: sub})
}

func (r managerAckRecorder) RecordDistributedTaskPreparationCompleteAck(context.Context, string, string, uint64, string, string, bool, string) error {
	return errors.New("no PREP ack expected")
}
