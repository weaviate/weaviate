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

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (s *Raft) AddDistributedTask(ctx context.Context, namespace, taskID string, taskPayload any, subUnitIDs ...string) error {
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}

	req := cmd.AddDistributedTaskRequestWithSubUnits{
		Namespace:             namespace,
		Id:                    taskID,
		Payload:               payloadBytes,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
		SubUnitIds:            subUnitIDs,
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

func (s *Raft) RecordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64) error {
	return s.recordDistributedTaskNodeCompletion(ctx, namespace, taskID, version, nil)
}

func (s *Raft) RecordDistributedTaskNodeFailure(ctx context.Context, namespace, taskID string, version uint64, failureReason string) error {
	return s.recordDistributedTaskNodeCompletion(ctx, namespace, taskID, version, &failureReason)
}

func (s *Raft) recordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64, failureReason *string) error {
	req := cmd.RecordDistributedTaskNodeCompletionRequest{
		Namespace:            namespace,
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

func (s *Raft) CancelDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error {
	req := cmd.CancelDistributedTaskRequest{
		Namespace:             namespace,
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

func (s *Raft) CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error {
	req := cmd.CleanUpDistributedTaskRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   taskVersion,
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

// RecordDistributedTaskSubUnitCompletion records a successful sub-unit completion via Raft.
func (s *Raft) RecordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, subUnitID string) error {
	return s.recordDistributedTaskSubUnitCompletion(ctx, namespace, taskID, version, subUnitID, nil)
}

// RecordDistributedTaskSubUnitFailure records a sub-unit failure via Raft.
func (s *Raft) RecordDistributedTaskSubUnitFailure(ctx context.Context, namespace, taskID string, version uint64, subUnitID string, errMsg string) error {
	return s.recordDistributedTaskSubUnitCompletion(ctx, namespace, taskID, version, subUnitID, &errMsg)
}

func (s *Raft) recordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, subUnitID string, errMsg *string) error {
	req := cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            subUnitID,
		NodeId:               s.nodeSelector.LocalName(),
		Error:                errMsg,
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_COMPLETED,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}

// RecordDistributedTaskSubUnitProgress records fractional progress for a sub-unit via Raft.
// The Manager may silently drop updates that arrive more frequently than SubUnitProgressMinInterval.
func (s *Raft) RecordDistributedTaskSubUnitProgress(ctx context.Context, namespace, taskID string, version uint64, subUnitID string, progress float64) error {
	req := cmd.RecordDistributedTaskSubUnitProgressRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   version,
		SubUnitId: subUnitID,
		NodeId:    s.nodeSelector.LocalName(),
		Progress:  progress,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_PROGRESS,
		SubCommand: subCommand,
	}
	if _, err = s.Execute(ctx, command); err != nil {
		return fmt.Errorf("executing command: %w", err)
	}
	return nil
}
