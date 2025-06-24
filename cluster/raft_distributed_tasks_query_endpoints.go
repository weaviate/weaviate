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
