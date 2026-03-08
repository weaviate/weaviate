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

func (s *Raft) AddDistributedTask(ctx context.Context, namespace, taskID string, taskPayload any, subUnitIDs []string) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Payload:               payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
		SubUnitIds:            subUnitIDs,
	})
}

// AddDistributedTaskWithGroups creates a task with sub-units that have explicit group assignments.
// SubUnitSpecs take precedence over SubUnitIds when both are present.
func (s *Raft) AddDistributedTaskWithGroups(
	ctx context.Context, namespace, taskID string,
	taskPayload any, subUnitSpecs []distributedtask.SubUnitSpec,
) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	protoSpecs := make([]*cmd.SubUnitSpec, len(subUnitSpecs))
	for i, spec := range subUnitSpecs {
		protoSpecs[i] = &cmd.SubUnitSpec{Id: spec.ID, GroupId: spec.GroupID}
	}
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, &cmd.AddDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Payload:               payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
		SubUnitSpecs:          protoSpecs,
	})
}

func (s *Raft) RecordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64) error {
	return s.recordDistributedTaskNodeCompletion(ctx, namespace, taskID, version, nil)
}

func (s *Raft) RecordDistributedTaskNodeFailure(ctx context.Context, namespace, taskID string, version uint64, failureReason string) error {
	return s.recordDistributedTaskNodeCompletion(ctx, namespace, taskID, version, &failureReason)
}

func (s *Raft) recordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64, failureReason *string) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_NODE_COMPLETED, &cmd.RecordDistributedTaskNodeCompletionRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		NodeId:               s.nodeSelector.LocalName(),
		Error:                failureReason,
		FinishedAtUnixMillis: time.Now().UnixMilli(),
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

func (s *Raft) RecordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string) error {
	return s.recordDistributedTaskSubUnitCompletion(ctx, namespace, taskID, version, nodeID, subUnitID, "")
}

func (s *Raft) RecordDistributedTaskSubUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID, errMsg string) error {
	return s.recordDistributedTaskSubUnitCompletion(ctx, namespace, taskID, version, nodeID, subUnitID, errMsg)
}

func (s *Raft) recordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID, errMsg string) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_COMPLETED, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		NodeId:               nodeID,
		SubUnitId:            subUnitID,
		Error:                errMsg,
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (s *Raft) UpdateDistributedTaskSubUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string, progress float32) error {
	return s.applyDistributedTaskCommand(ctx, cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_SUB_UNIT_PROGRESS, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
		Namespace:           namespace,
		Id:                  taskID,
		Version:             version,
		NodeId:              nodeID,
		SubUnitId:           subUnitID,
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
