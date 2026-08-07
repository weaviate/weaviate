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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// The TTL decision belongs to the sweep that proposes the cleanup, not to each
// node that applies it — an apply that reads the local wall clock forks the
// FSM. CleanUpDistributedTask therefore has to put the decision on the request.
// The task seeded here finished a moment ago and the node's TTL is an hour, so
// a request that carries no decision is refused; the cleanup only lands
// because the proposer's verdict travels with it.
func TestRaft_CleanUpDistributedTask_CarriesTheProposersTTLVerdict(t *testing.T) {
	ctx := context.Background()

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	m.cfg.DistributedTasks.CompletedTaskTTL = time.Hour
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(schema.NewMockreplicationFSM(t))
	m.store = &s

	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("Close", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	t.Cleanup(func() { _ = srv.Close(ctx) })
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, 200*time.Millisecond, srv.Ready))

	finishedAt := time.Now()
	snap, err := json.Marshal(map[string]any{
		"tasks": map[string][]*distributedtask.Task{
			"reindex": {{
				Namespace:      "reindex",
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "just-finished", Version: 1},
				Status:         distributedtask.TaskStatusFinished,
				StartedAt:      finishedAt.Add(-time.Minute),
				FinishedAt:     finishedAt,
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, srv.store.distributedTasksManager.Restore(snap))

	require.NoError(t, srv.CleanUpDistributedTask(ctx, "reindex", "just-finished", 1),
		"the apply must obey the proposer's TTL verdict; without it on the request the "+
			"node re-measures the age itself and refuses this task as too fresh")

	got, err := srv.ListDistributedTasksLocal(ctx)
	require.NoError(t, err)
	require.Empty(t, got["reindex"], "the cleaned-up task must be gone")
}
