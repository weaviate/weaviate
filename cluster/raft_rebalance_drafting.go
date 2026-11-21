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
	"slices"
	"time"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
)

const (
	// draftStaleTimeout is how long a PENDING or IN_PROGRESS draft unit may
	// stay unchanged before the scheduler's stale detection fails the task.
	draftStaleTimeout = 10 * time.Minute
	// draftMaxUnitRetries is how many times a node's failed draft is retried
	// before the task fails.
	draftMaxUnitRetries = 3
)

type rebalanceDraftPayload struct {
	PlanID string `json:"planId"`
}

// AddRebalanceDraftTask starts the draft task of plan planID, with one unit
// per node. The task ID is the plan ID.
func (s *Raft) AddRebalanceDraftTask(ctx context.Context, planID string, nodes []string) error {
	sorted := slices.Sorted(slices.Values(nodes))
	units := make([]distributedtask.UnitSpec, len(sorted))
	for i, node := range sorted {
		units[i] = distributedtask.UnitSpec{ID: node}
	}
	return s.AddDistributedTaskWithGroupsOptions(ctx, types.RebalanceDraftNamespace, planID,
		rebalanceDraftPayload{PlanID: planID}, units, false,
		AddDistributedTaskOptions{
			StaleTimeoutMs: draftStaleTimeout.Milliseconds(),
			MaxUnitRetries: draftMaxUnitRetries,
		})
}

// TerminateRebalanceDraftTask cancels the draft task of plan planID at version.
// It is a no-op on a task that already ended.
func (s *Raft) TerminateRebalanceDraftTask(ctx context.Context, planID string, version uint64) error {
	return s.ForceTerminateDistributedTask(ctx, types.RebalanceDraftNamespace, planID, version,
		"rebalance plan is no longer planning", distributedtask.TaskStatusCancelled.String())
}

// RebalanceDraftTasks reads the draft tasks from this node's FSM. That copy can
// lag behind the leader's.
func (s *Raft) RebalanceDraftTasks() []replication.DraftTask {
	tasks := s.LocalDistributedTasks()[types.RebalanceDraftNamespace]
	out := make([]replication.DraftTask, 0, len(tasks))
	for _, task := range tasks {
		units := make(map[string]replication.DraftUnitStatus, len(task.Units))
		for id, unit := range task.Units {
			units[id] = replication.DraftUnitStatus(unit.Status)
		}
		out = append(out, replication.DraftTask{
			PlanID:   task.ID,
			Version:  task.Version,
			Terminal: task.Status.IsTerminal(),
			Error:    task.Error,
			Units:    units,
		})
	}
	return out
}
