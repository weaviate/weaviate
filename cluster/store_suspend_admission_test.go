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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

func TestExecute_ConcurrentTaskAddAndSuspendAdmitOnlyOne(t *testing.T) {
	srv, ms := newBarrierTestStore(t, func(c *Config) {
		c.DistributedTaskCollectionExtractors = map[string]distributedtask.CollectionExtractor{
			"reindex": func(p []byte) (string, bool) { return string(p), len(p) > 0 },
		}
	})
	s := srv.store

	for i := range 20 {
		ns := fmt.Sprintf("ns%d", i)
		seedNamespaceInState(t, ms.cfg.NamespacesController, ns, api.NamespaceStateActive)
		addSub, err := json.Marshal(&api.AddDistributedTaskRequest{
			Namespace: "reindex", Id: ns, UnitIds: []string{"unit1"},
			Payload: []byte(ns + ":Movies"), SubmittedAtUnixMillis: time.Now().UnixMilli(),
		})
		require.NoError(t, err)
		suspendSub, err := json.Marshal(api.ChangeNamespaceStateRequest{
			Name: ns, TargetState: api.NamespaceStateSuspended, Version: api.NamespaceLatestCommandPolicyVersion,
		})
		require.NoError(t, err)
		commands := []*api.ApplyRequest{
			{Type: api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, SubCommand: addSub},
			{Type: api.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE, SubCommand: suspendSub},
		}

		errs := make([]error, len(commands))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j, command := range commands {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				<-start
				_, errs[j] = s.Execute(command)
			}, s.log)
		}
		close(start)
		wg.Wait()

		admitted := 0
		for _, err := range errs {
			if err == nil {
				admitted++
				continue
			}
			require.True(t, errors.Is(err, namespaces.ErrSuspendBlockedByReindex) ||
				errors.Is(err, namespaces.ErrNamespaceSuspended), "%s: unexpected refusal: %v", ns, err)
		}
		require.Equal(t, 1, admitted, "%s: the task add and the suspend were both admitted", ns)
	}
}
