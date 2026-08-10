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
// FSM. CleanUpDistributedTask therefore has to put the measurements the sweep
// made on the request: the moment it looked, the TTL it looked against, and the
// finish time it read. The two arms below share one task age and differ only in
// the TTL on the request, so the TTL is what decides.
func TestRaft_CleanUpDistributedTask_CarriesTheProposersMeasurements(t *testing.T) {
	ctx := context.Background()

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
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

	// Both tasks finished a minute before the sweep looked.
	const age = time.Minute
	finishedAt := time.Now()
	proposedAt := finishedAt.Add(age)

	for _, tc := range []struct {
		name     string
		taskID   string
		ttl      time.Duration
		wantKept bool
	}{
		{name: "a TTL the age has run past deletes", taskID: "expired", ttl: time.Second},
		{name: "a TTL the age has not reached keeps", taskID: "still-retained", ttl: time.Hour, wantKept: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := json.Marshal(map[string]any{
				"tasks": map[string][]*distributedtask.Task{
					"reindex": {{
						Namespace:      "reindex",
						TaskDescriptor: distributedtask.TaskDescriptor{ID: tc.taskID, Version: 1},
						Status:         distributedtask.TaskStatusFinished,
						StartedAt:      finishedAt.Add(-time.Minute),
						FinishedAt:     finishedAt,
					}},
				},
			})
			require.NoError(t, err)
			require.NoError(t, srv.store.distributedTasksManager.Restore(snap))

			err = srv.CleanUpDistributedTask(ctx, "reindex", tc.taskID, 1, finishedAt, proposedAt, tc.ttl)
			got, listErr := srv.ListDistributedTasksAtLocalConsistency(ctx)
			require.NoError(t, listErr)

			if tc.wantKept {
				require.ErrorContains(t, err, "too fresh to clean up")
				require.Len(t, got["reindex"], 1, "the TTL on the request must be what decides")
				return
			}
			require.NoError(t, err)
			require.Empty(t, got["reindex"], "the cleaned-up task must be gone")
		})
	}
}
