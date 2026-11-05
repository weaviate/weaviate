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

package db

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// Pins that two nodes applying the same log reach the same task list, and so
// the same index-DELETE verdict, whatever their clocks read: the cleanup apply
// reads none.
func TestCleanupAppliesAlikeOnNodesWithSkewedClocks(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenizationFilterable, Collection: "C", Properties: []string{"title"},
		TargetTokenization: models.PropertyTokenizationField,
	})
	require.NoError(t, err)
	task := func(id string, version uint64, status distributedtask.TaskStatus, finished time.Time, decided ...string) any {
		groups := map[string]time.Time{}
		for _, g := range decided {
			groups[g] = finished
		}
		return struct {
			*distributedtask.Task
			Units map[string]*distributedtask.Unit `json:"units"`
		}{&distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
			Payload: payload, Status: status, FinishedAt: finished, SwapDecidedGroups: groups,
		}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}}}
	}
	// A partial run P, and the finished resubmit F that covers it.
	snapshot, err := json.Marshal(map[string]any{"tasks": map[string][]any{ReindexNamespace: {
		task("P", 10, distributedtask.TaskStatusCancelled, t0, "a"),
		task("F", 20, distributedtask.TaskStatusFinished, t0.Add(10*time.Minute), "a", "b"),
	}}})
	require.NoError(t, err)

	node := func() *distributedtask.Manager {
		m := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New()})
		require.NoError(t, m.Restore(snapshot))
		m.SetSchemaMutationDetectors(map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: &ReindexProvider{}})
		return m
	}
	// The leader's sweep proposes each cleanup once its TTL passed by the leader's
	// clock; a follower whose clock runs behind applies the same entries.
	leader, follower := node(), node()
	for _, id := range []string{"P", "F"} {
		version := map[string]uint64{"P": 10, "F": 20}[id]
		sub, err := json.Marshal(api.CleanUpDistributedTaskRequest{Namespace: ReindexNamespace, Id: id, Version: version})
		require.NoError(t, err)
		req := &api.ApplyRequest{Type: api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP, SubCommand: sub}
		require.NoError(t, leader.CleanUpTask(req))
		require.NoError(t, follower.CleanUpTask(req), "cleanup of %s", id)
	}

	ls, err := leader.Snapshot()
	require.NoError(t, err)
	fs, err := follower.Snapshot()
	require.NoError(t, err)
	require.JSONEq(t, string(ls), string(fs), "both nodes hold the same task list")
	require.Equal(t, leader.CheckPropertyUpdate("C", "title") == nil, follower.CheckPropertyUpdate("C", "title") == nil,
		"and so answer an index DELETE alike")
}
