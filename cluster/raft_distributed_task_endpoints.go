//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (s *Raft) AddDistributedTask(ctx context.Context, taskType, taskID string, taskPayload any) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}

	req := cmd.AddDistributedTaskRequest{
		TaskType:              taskType,
		TaskId:                taskID,
		TaskPayload:           payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("add distributed task: %w", err)
	}
	return nil
}

func (s *Raft) RecordLocalNodeDistributedTaskCompletion(ctx context.Context, taskType, taskID string) error {
	s.nodeSelector.LocalName()

	req := cmd.DistributedTaskFinishedByTheNodeRequest{
		TaskId:               taskID,
		TaskType:             taskType,
		NodeId:               s.nodeSelector.LocalName(),
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_NODE_FINISHED,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("add distributed task: %w", err)
	}
	return nil
}

func (s *Raft) CancelDistributedTask(ctx context.Context, taskType, taskID string) error {
	s.nodeSelector.LocalName()

	req := cmd.CancelDistributedTaskRequest{
		TaskId:                taskID,
		TaskType:              taskType,
		CancelledAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCELLED,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("add distributed task: %w", err)
	}
	return nil
}
