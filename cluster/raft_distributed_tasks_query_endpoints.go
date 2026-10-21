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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (s *Raft) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	command := &cmd.QueryRequest{
		Type: cmd.QueryRequest_TYPE_DISTRIBUTED_TASK_LIST,
	}
	queryResp, err := s.Query(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := distributedtask.ListDistributedTasksResponse{}
	if err = json.Unmarshal(queryResp.Payload, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Tasks, nil
}

// GetDistributedTask is leader-routed. Returns (nil, nil) when the task
// does not exist.
func (s *Raft) GetDistributedTask(ctx context.Context, namespace, taskID string) (*distributedtask.Task, error) {
	subCommand, err := json.Marshal(&cmd.GetDistributedTaskRequest{
		Namespace: namespace,
		Id:        taskID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get task request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_DISTRIBUTED_TASK_GET,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(ctx, command)
	if err != nil {
		// A missing task comes back as ErrTaskDoesNotExist. On the
		// leader the error chain preserves the sentinel. On a follower
		// the gRPC round-trip flattens it to a string.
		// RehydratePermanentRejection restores the sentinel from the
		// [dtm-perm/...] text marker.
		err = distributedtask.RehydratePermanentRejection(err)
		if errors.Is(err, distributedtask.ErrTaskDoesNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := distributedtask.GetDistributedTaskResponse{}
	if err = json.Unmarshal(queryResp.Payload, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Task, nil
}
