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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

func TestAdmitPropose_SuspendRefusedWhileReindexWrites(t *testing.T) {
	task := func(namespace, collection string, status distributedtask.TaskStatus, swapsOwed map[string][]string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace:      namespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "task1", Version: 1},
			Payload:        []byte(collection),
			Status:         status,
			SwapsOwed:      swapsOwed,
		}
	}
	owed := map[string][]string{"shard1": {"node2"}}

	for _, tc := range []struct {
		name    string
		task    *distributedtask.Task
		target  api.NamespaceState
		refused bool
	}{
		{name: "no task", target: api.NamespaceStateSuspended},
		{name: "started", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateSuspended, refused: true},
		{name: "preparing", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusPreparing, nil), target: api.NamespaceStateSuspended, refused: true},
		{name: "swapping", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusSwapping, nil), target: api.NamespaceStateSuspended, refused: true},
		{name: "cancelled with a swap owed", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusCancelled, owed), target: api.NamespaceStateSuspended, refused: true},
		{name: "failed with a swap owed", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusFailed, owed), target: api.NamespaceStateSuspended, refused: true},
		{name: "cancelled with no swap owed", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusCancelled, nil), target: api.NamespaceStateSuspended},
		{name: "finished", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusFinished, nil), target: api.NamespaceStateSuspended},
		{name: "started in another namespace", task: task("reindex", "beta:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateSuspended},
		{name: "started in a namespace with a common prefix", task: task("reindex", "alphabet:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateSuspended},
		{name: "started task type with no collection", task: task("other", "alpha:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateSuspended},
		{name: "resume", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateActive},
		{name: "delete", task: task("reindex", "alpha:Movies", distributedtask.TaskStatusStarted, nil), target: api.NamespaceStateDeleting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewMockStore(t, "node1", 0, func(c *Config) {
				c.DistributedTaskCollectionExtractors = map[string]distributedtask.CollectionExtractor{
					"reindex": func(p []byte) (string, bool) { return string(p), len(p) > 0 },
				}
			}).store
			if tc.task != nil {
				snapshot, err := json.Marshal(map[string]map[string][]*distributedtask.Task{
					"tasks": {tc.task.Namespace: {tc.task}},
				})
				require.NoError(t, err)
				require.NoError(t, s.distributedTasksManager.Restore(snapshot))
			}
			sub, err := json.Marshal(api.ChangeNamespaceStateRequest{Name: "alpha", TargetState: tc.target})
			require.NoError(t, err)

			err = s.admitPropose(&api.ApplyRequest{Type: api.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE, SubCommand: sub}, "", nil)
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, namespaces.ErrSuspendBlockedByReindex)
			require.Contains(t, err.Error(), `reindex task "task1" on collection "alpha:Movies"`)
			require.Contains(t, err.Error(), "POST /v1/schema/alpha:Movies/properties/{property}/index/{index}/cancel")
		})
	}
}
