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
		Type:                  taskType,
		Id:                    taskID,
		Payload:               payloadBytes,
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
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

func (s *Raft) RecordDistributedTaskNodeCompletion(ctx context.Context, taskType, taskID string, version uint64) error {
	req := cmd.RecordDistributedTaskNodeCompletionRequest{
		Type:                 taskType,
		Id:                   taskID,
		Version:              version,
		NodeId:               s.nodeSelector.LocalName(),
		Error:                nil,
		FinishedAtUnixMillis: 0,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_NODE_COMPLETED,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

func (s *Raft) RecordNodeDistributedTaskCompletion(ctx context.Context, taskType, taskID string, version uint64) error {
	return s.recordNodeDistributedTaskCompletion(ctx, taskType, taskID, version, nil)
}

func (s *Raft) RecordNodeDistributedTaskFailure(ctx context.Context, taskType, taskID string, version uint64, failureReason string) error {
	return s.recordNodeDistributedTaskCompletion(ctx, taskType, taskID, version, &failureReason)
}

func (s *Raft) recordNodeDistributedTaskCompletion(ctx context.Context, taskType, taskID string, version uint64, failureReason *string) error {
	req := cmd.RecordDistributedTaskNodeCompletionRequest{
		Type:                 taskType,
		Id:                   taskID,
		Version:              version,
		NodeId:               s.nodeSelector.LocalName(),
		Error:                failureReason,
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_NODE_COMPLETED,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

func (s *Raft) CancelDistributedTask(ctx context.Context, taskType, taskID string, taskVersion uint64) error {
	req := cmd.CancelDistributedTaskRequest{
		Type:                  taskType,
		Id:                    taskID,
		Version:               taskVersion,
		CancelledAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCEL,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

func (s *Raft) CleanUpDistributedTask(ctx context.Context, taskType, taskID string, taskVersion uint64) error {
	req := cmd.CleanUpDistributedTaskRequest{
		Type:    taskType,
		Id:      taskID,
		Version: taskVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}
