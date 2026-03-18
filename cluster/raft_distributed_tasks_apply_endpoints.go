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
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

func (s *Raft) AddDistributedTask(ctx context.Context, namespace, taskID string, taskPayload any, unitIDs []string) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Payload:               payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
		UnitIds:               unitIDs,
	})
}

// AddDistributedTaskWithGroups creates a task with units that have explicit group assignments.
// UnitSpecs take precedence over UnitIds when both are present.
func (s *Raft) AddDistributedTaskWithGroups(
	ctx context.Context, namespace, taskID string,
	taskPayload any, subUnitSpecs []distributedtask.UnitSpec,
) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	protoSpecs := make([]*cmd.UnitSpec, len(subUnitSpecs))
	for i, spec := range subUnitSpecs {
		protoSpecs[i] = &cmd.UnitSpec{Id: spec.ID, GroupId: spec.GroupID}
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Payload:               payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
		UnitSpecs:             protoSpecs,
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
