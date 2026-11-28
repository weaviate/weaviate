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
	"fmt"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
)

func (s *Raft) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return s.queryDistributedTasks(ctx, cmd.QueryRequest_TYPE_DISTRIBUTED_TASK_LIST, s.store.distributedTasksManager.DecodeTaskList)
}

func (s *Raft) ListDistributedTaskHeaders(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return s.queryDistributedTasks(ctx, cmd.QueryRequest_TYPE_DISTRIBUTED_TASK_HEADERS, s.store.distributedTasksManager.DecodeTaskHeaders)
}

func (s *Raft) queryDistributedTasks(ctx context.Context, queryType cmd.QueryRequest_Type,
	decode func([]byte) (map[string][]*distributedtask.Task, error),
) (map[string][]*distributedtask.Task, error) {
	// The leader RPC waits for a connection that may never come up. Most callers pass no deadline.
	ctx, cancel := utils.BoundIfNoDeadline(ctx, s.store.cfg.ConsistencyWaitTimeout)
	defer cancel()
	queryResp, err := s.Query(ctx, &cmd.QueryRequest{Type: queryType})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	tasks, err := decode(queryResp.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return tasks, nil
}
