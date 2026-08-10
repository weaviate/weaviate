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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// Every handler-level test fakes this service, so this is the only place the
// real method's read is observable. The node here has never joined a cluster
// (FSM state but no raft), so a local read still answers while a
// leader-routed one cannot — the asymmetry that makes the two reads
// distinguishable without a second node.
func TestRaft_ListDistributedTasksAtLocalConsistency_AnswersFromThisNodesFSM(t *testing.T) {
	ctx := context.Background()

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	snap, err := json.Marshal(map[string]any{
		"tasks": map[string][]*distributedtask.Task{
			"reindex": {{
				Namespace:      "reindex",
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "local-only", Version: 1},
				Status:         distributedtask.TaskStatusSwapping,
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, m.store.distributedTasksManager.Restore(snap))

	got, err := srv.ListDistributedTasksAtLocalConsistency(ctx)
	require.NoError(t, err)
	require.Len(t, got["reindex"], 1)
	require.Equal(t, "local-only", got["reindex"][0].ID)

	// The control that gives the assertion above its meaning: the leader read
	// has no answer on this node, so a local read that delegated to it would
	// have returned an error instead of the task.
	leaderCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, leaderErr := srv.ListDistributedTasks(leaderCtx)
	require.Error(t, leaderErr, "sanity: the leader read must not be able to answer here")
}
