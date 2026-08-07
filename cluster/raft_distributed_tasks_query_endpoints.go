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

// ListDistributedTasksLocal answers from this node's own FSM instead of the
// leader.
//
// Pick this one when the answer is rendered against other state read from this
// same node, so the two are apply-ordered with respect to each other. Read from
// the leader instead and a follower can report a task FINISHED before it has
// applied the schema change that same task committed to the log first.
//
// Pick [Raft.ListDistributedTasks] for anything that decides whether an
// operation may proceed: a lagging follower's view admits work the leader
// already knows conflicts.
//
// Neither is a claim about what a FINISHED task implies about the schema. A
// task can finish without flipping anything — change-algorithm defers its
// class-level flip until every searchable property has migrated. The ordering
// here only says this node cannot see the task before the flip it did make.
func (s *Raft) ListDistributedTasksLocal(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return s.store.distributedTasksManager.ListDistributedTasks(ctx)
}
