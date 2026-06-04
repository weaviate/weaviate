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
	"encoding/json"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (s *Raft) applyDistributedTaskCommand(ctx context.Context, cmdType cmd.ApplyRequest_Type, req any) error {
	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	if _, err = s.Execute(ctx, &cmd.ApplyRequest{Type: cmdType, SubCommand: subCommand}); err != nil {
		// If the leader returned a permanent FSM rejection, the gRPC
		// transport stripped the Go error type. Re-hydrate the sentinel
		// here so callers (e.g. ReindexProvider.failUnit) can rely on
		// errors.Is for classification rather than substring matching
		// against an upstream-defined message.
		err = distributedtask.RehydratePermanentRejection(err)
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

// AddDistributedTask is the no-barrier variant — task uses pre-barrier
// semantics (STARTED → SWAPPING directly on AllUnitsTerminal). Use
// AddDistributedTaskWithBarrier when the task needs cluster-wide swap
// coordination (semantic migrations in the reindex provider; future
// providers needing the same property).
func (s *Raft) AddDistributedTask(ctx context.Context, namespace, taskID string, taskPayload any, unitIDs []string) error {
	return s.AddDistributedTaskWithBarrier(ctx, namespace, taskID, taskPayload, unitIDs, false)
}

// AddDistributedTaskWithGroups creates a task with units that have explicit group assignments.
// UnitSpecs take precedence over UnitIds when both are present.
func (s *Raft) AddDistributedTaskWithGroups(
	ctx context.Context, namespace, taskID string,
	taskPayload any, unitSpecs []distributedtask.UnitSpec,
) error {
	return s.AddDistributedTaskWithGroupsBarrier(ctx, namespace, taskID, taskPayload, unitSpecs, false)
}

// AddDistributedTaskWithBarrier is the PREP-barrier-aware counterpart
// to AddDistributedTask. When needsPreparationBarrier=true the task uses the
// two-phase RAFT-coordinated swap: AllUnitsTerminal routes to
// PREPARING (not SWAPPING directly), and each node's PREP completion
// must ack before any node fires its atomic swap.
//
// AddDistributedTask is equivalent to AddDistributedTaskWithBarrier(...,
// false) for callers that don't need cluster-wide swap coordination
// (format-only migrations, debug-originated tasks).
func (s *Raft) AddDistributedTaskWithBarrier(
	ctx context.Context, namespace, taskID string,
	taskPayload any, unitIDs []string, needsPreparationBarrier bool,
) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:               namespace,
		Id:                      taskID,
		Payload:                 payloadBytes,
		SubmittedAtUnixMillis:   time.Now().UnixMilli(),
		UnitIds:                 unitIDs,
		NeedsPreparationBarrier: needsPreparationBarrier,
	})
}

// AddDistributedTaskWithGroupsBarrier is the grouped, PREP-barrier-aware
// counterpart to AddDistributedTaskWithGroups. See
// AddDistributedTaskWithBarrier for the barrier semantics; this variant
// takes UnitSpecs (with optional GroupID per unit) instead of bare
// unit IDs.
func (s *Raft) AddDistributedTaskWithGroupsBarrier(
	ctx context.Context, namespace, taskID string,
	taskPayload any, unitSpecs []distributedtask.UnitSpec, needsPreparationBarrier bool,
) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	protoSpecs := make([]*cmd.UnitSpec, len(unitSpecs))
	for i, spec := range unitSpecs {
		protoSpecs[i] = &cmd.UnitSpec{Id: spec.ID, GroupId: spec.GroupID}
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:               namespace,
		Id:                      taskID,
		Payload:                 payloadBytes,
		SubmittedAtUnixMillis:   time.Now().UnixMilli(),
		UnitSpecs:               protoSpecs,
		NeedsPreparationBarrier: needsPreparationBarrier,
	})
}

func (s *Raft) CancelDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCEL, &cmd.CancelDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Version:               taskVersion,
		CancelledAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (s *Raft) RecordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error {
	return s.recordDistributedTaskUnitCompletion(ctx, namespace, taskID, version, nodeID, unitID, "")
}

func (s *Raft) RecordDistributedTaskUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	return s.recordDistributedTaskUnitCompletion(ctx, namespace, taskID, version, nodeID, unitID, errMsg)
}

func (s *Raft) recordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		NodeId:               nodeID,
		UnitId:               unitID,
		Error:                errMsg,
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (s *Raft) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_UNIT_PROGRESS, &cmd.UpdateDistributedTaskUnitProgressRequest{
		Namespace:           namespace,
		Id:                  taskID,
		Version:             version,
		NodeId:              nodeID,
		UnitId:              unitID,
		Progress:            progress,
		UpdatedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (s *Raft) CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP, &cmd.CleanUpDistributedTaskRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   taskVersion,
	})
}

func (s *Raft) MarkDistributedTaskFinalized(ctx context.Context, namespace, taskID string, taskVersion uint64) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FINALIZED, &cmd.MarkTaskFinalizedRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Version:               taskVersion,
		FinalizedAtUnixMillis: time.Now().UnixMilli(),
	})
}

// RecordDistributedTaskPostCompletionAck commits one node's SWAP-phase
// callback result to the FSM (OnGroupCompleted for non-barrier tasks,
// OnSwapRequested for barrier tasks). The scheduler tick on each node
// fires this after its local SWAP body has returned so the cluster has
// durable evidence of which nodes' post-completion work succeeded
// before MarkDistributedTaskFinalized is allowed to land.
//
// See [distributedtask.PostCompletionAckRecorder].
func (s *Raft) RecordDistributedTaskPostCompletionAck(
	ctx context.Context,
	namespace, taskID string,
	taskVersion uint64,
	nodeID string,
	success bool,
	errMsg string,
) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_POST_COMPLETION_ACK,
		&cmd.RecordDistributedTaskPostCompletionAckRequest{
			Namespace:         namespace,
			Id:                taskID,
			Version:           taskVersion,
			NodeId:            nodeID,
			Success:           success,
			Error:             errMsg,
			AckedAtUnixMillis: time.Now().UnixMilli(),
		})
}

// RecordDistributedTaskPreparationCompleteAck commits one node's
// OnGroupCompleted PREP-phase result to the FSM. The scheduler tick
// on each node fires this after its local PREP body has returned, so
// the cluster has durable evidence of which nodes' prep work
// succeeded before the PREPARING → SWAPPING transition is committed.
// This is the load-bearing barrier.
//
// See [distributedtask.PostCompletionAckRecorder].
func (s *Raft) RecordDistributedTaskPreparationCompleteAck(
	ctx context.Context,
	namespace, taskID string,
	taskVersion uint64,
	nodeID string,
	success bool,
	errMsg string,
) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK,
		&cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace:         namespace,
			Id:                taskID,
			Version:           taskVersion,
			NodeId:            nodeID,
			Success:           success,
			Error:             errMsg,
			AckedAtUnixMillis: time.Now().UnixMilli(),
		})
}
