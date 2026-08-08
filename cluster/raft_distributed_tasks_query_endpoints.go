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

// ListDistributedTasksAtLocalConsistency answers from this node's own FSM instead
// of the leader. Use it when rendering against other state also read from this
// node: reading from the leader instead, a follower could report a task FINISHED
// before applying the schema change that task committed first. Two local reads
// are still two reads, not a shared snapshot, but this shrinks the window from
// the leader's apply lag to the gap between the calls.
//
// Use [Raft.ListDistributedTasks] instead for anything gating whether an
// operation may proceed — a lagging follower's view can admit work the leader
// already knows conflicts.
//
// A FINISHED task does not imply the schema flipped: change-algorithm defers
// its class-level flip until every searchable property has migrated. The
// ordering guarantee here only covers a flip this node already made.
func (s *Raft) ListDistributedTasksAtLocalConsistency(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return s.store.distributedTasksManager.ListDistributedTasks(ctx)
}
