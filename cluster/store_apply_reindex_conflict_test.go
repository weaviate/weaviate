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
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TestStoreApply_RefusesAReindexConflictForEveryMigrationType drives the
// conflict check at the tier that decides whether a task exists: the RAFT
// AddTask apply. The tier matters because Store.Apply runs the command body
// through enterrors.GoWrapper, so anything the check panics on is recovered and
// the response carries no error — the submit reports success and the task is
// absent from this node's FSM.
//
// Every declared migration type is exercised as the in-flight one, so a type
// missing from either bucket-touch switch shows up here as a refusal that never
// arrives, rather than as a lost entry in production.
func TestStoreApply_RefusesAReindexConflictForEveryMigrationType(t *testing.T) {
	addTask := func(t *testing.T, ms MockStore, index uint64, id string,
		mt db.ReindexMigrationType,
	) Response {
		t.Helper()
		payload, err := json.Marshal(db.ReindexTaskPayload{
			Collection:    "C",
			MigrationType: mt,
			Properties:    []string{"prop"},
		})
		require.NoError(t, err)

		resp, ok := ms.store.Apply(&raft.Log{
			Index: index,
			Type:  raft.LogCommand,
			Data: cmdAsBytes("C", api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD,
				api.AddDistributedTaskRequest{
					Namespace: db.ReindexNamespace,
					Id:        id,
					Payload:   payload,
					UnitIds:   []string{"u1"},
				}, nil),
		}).(Response)
		require.True(t, ok, "Apply must answer with a Response")
		return resp
	}

	for _, mt := range db.AllReindexMigrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			ms, _ := setupApplyTest(t)
			ms.store.distributedTasksManager.SetConflictDetectors(
				map[string]distributedtask.ConflictDetector{
					db.ReindexNamespace: &db.ReindexProvider{},
				})

			require.NoError(t, addTask(t, ms, 1, "first", mt).Error,
				"the first task on a free property must apply")

			second := addTask(t, ms, 2, "second", db.ReindexTypeChangeAlgorithm)
			require.Error(t, second.Error,
				"a live %s on the same property must make the apply refuse; an apply that "+
					"answers without an error is a submit the client sees succeed with no task", mt)
			require.Contains(t, second.Error.Error(), "conflicts")

			tasks, err := ms.store.distributedTasksManager.ListDistributedTasks(t.Context())
			require.NoError(t, err)
			require.Len(t, tasks[db.ReindexNamespace], 1,
				"the refused task must not be in the FSM")
			require.Equal(t, "first", tasks[db.ReindexNamespace][0].ID)
		})
	}
}
