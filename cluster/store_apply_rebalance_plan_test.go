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
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// The replication package's unit tests call CompletePlanning with an explicit
// term, so only a test through Store.Apply catches a dispatch that passes the
// entry's index instead of its term.
func TestStore_Apply_RebalanceCompletePlanningUsesLogTerm(t *testing.T) {
	const (
		planID    = "plan-1"
		entryTerm = 7
	)

	apply := func(t *testing.T, ms *MockStore, index uint64, cmdType api.ApplyRequest_Type, sub any) error {
		t.Helper()
		resp, ok := ms.store.Apply(&raft.Log{
			Index: index,
			Term:  entryTerm,
			Type:  raft.LogCommand,
			Data:  cmdAsBytes("", cmdType, sub, nil),
		}).(Response)
		require.True(t, ok)
		return resp.Error
	}
	getPlan := func(t *testing.T, ms *MockStore) replication.Plan {
		t.Helper()
		p, ok := ms.store.replicationManager.GetReplicationFSM().GetPlan()
		require.True(t, ok)
		return p
	}
	// storeWithPlan returns a store holding a PLANNING plan created at index 1.
	storeWithPlan := func(t *testing.T) *MockStore {
		t.Helper()
		ms := NewMockStore(t, "Node-1", 0)
		require.NoError(t, apply(t, &ms, 1, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN,
			replication.CreatePlanRequest{
				PlanID:          planID,
				SourceNodes:     []string{"Node-1"},
				TargetNodes:     []string{"Node-2"},
				CreatedAtUnixMs: 1000,
			}))
		return &ms
	}
	complete := func(runTerm uint64) replication.CompletePlanningRequest {
		return replication.CompletePlanningRequest{
			PlanID:              planID,
			RunID:               "run-a",
			RunTerm:             runTerm,
			Outcome:             replication.PlanStateProposed,
			MoveCount:           3,
			PlanHash:            "hash-1",
			ApproveBeforeUnixMs: 5000,
		}
	}

	t.Run("create lands a PLANNING plan", func(t *testing.T) {
		ms := storeWithPlan(t)

		got := getPlan(t, ms)
		require.Equal(t, planID, got.ID)
		require.Equal(t, replication.PlanStatePlanning, got.State)
		require.Equal(t, []string{"Node-1"}, got.SourceNodes)
		require.Equal(t, []string{"Node-2"}, got.TargetNodes)
	})

	t.Run("run term equal to the entry's term lands", func(t *testing.T) {
		ms := storeWithPlan(t)

		require.NoError(t, apply(t, ms, 2, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_COMPLETE_PLANNING, complete(entryTerm)))

		got := getPlan(t, ms)
		require.Equal(t, planID, got.ID)
		require.Equal(t, replication.PlanStateProposed, got.State)
		require.Equal(t, "run-a", got.RunID)
		require.Equal(t, uint64(entryTerm), got.RunTerm)
		require.Equal(t, "hash-1", got.PlanHash)
	})

	for _, tc := range []struct {
		name    string
		runTerm uint64
	}{
		{name: "run term equal to the entry's index is refused", runTerm: 2},
		{name: "run term above the entry's term is refused", runTerm: entryTerm + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := storeWithPlan(t)
			before := getPlan(t, ms)

			err := apply(t, ms, 2, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_COMPLETE_PLANNING, complete(tc.runTerm))

			require.ErrorIs(t, err, replication.ErrPlanStaleRun)
			require.Equal(t, before, getPlan(t, ms))
		})
	}

	t.Run("stop moves a PLANNING plan to STOPPED", func(t *testing.T) {
		ms := storeWithPlan(t)

		require.NoError(t, apply(t, ms, 2, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_STOP,
			replication.StopPlanRequest{PlanID: planID}))

		require.Equal(t, replication.PlanStateStopped, getPlan(t, ms).State)
	})

	t.Run("delete removes a PROPOSED plan", func(t *testing.T) {
		ms := storeWithPlan(t)
		require.NoError(t, apply(t, ms, 2, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_COMPLETE_PLANNING, complete(entryTerm)))

		require.NoError(t, apply(t, ms, 3, api.ApplyRequest_TYPE_REPLICATION_REBALANCE_DELETE,
			replication.DeletePlanRequest{PlanID: planID}))

		_, ok := ms.store.replicationManager.GetReplicationFSM().GetPlan()
		require.False(t, ok)
	})

	t.Run("a payload that does not decode is a bad request", func(t *testing.T) {
		for _, cmdType := range []api.ApplyRequest_Type{
			api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN,
			api.ApplyRequest_TYPE_REPLICATION_REBALANCE_COMPLETE_PLANNING,
			api.ApplyRequest_TYPE_REPLICATION_REBALANCE_STOP,
			api.ApplyRequest_TYPE_REPLICATION_REBALANCE_DELETE,
		} {
			t.Run(cmdType.String(), func(t *testing.T) {
				ms := storeWithPlan(t)

				err := apply(t, ms, 2, cmdType, "not an object")

				require.ErrorIs(t, err, replication.ErrBadRequest)
			})
		}
	})
}
