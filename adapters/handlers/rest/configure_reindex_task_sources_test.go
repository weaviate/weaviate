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

package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/distributed_tasks"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// A quiet new leader refuses task lists until a query starts its catch-up, so each caller retries.
func TestTaskListCallersRetryANewLeadersRefusal(t *testing.T) {
	notCaughtUp := fmt.Errorf("%w: catching up", types.ErrFSMNotCaughtUp)
	tasks := map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: {{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "d", Version: 1},
		Payload:        mustDropPayload(t, "C", "v1"),
		Status:         distributedtask.TaskStatusStarted,
	}}}
	for _, tc := range []struct {
		name string
		call func(t *testing.T, refusals *atomic.Int32)
	}{
		{name: "GET /v1/tasks", call: func(t *testing.T, refusals *atomic.Int32) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).Return(nil)
			api := &operations.WeaviateAPI{}
			setupDistributedTasksHandlers(api, authorizer, fakeTaskLister{tasks: tasks, err: notCaughtUp, refusals: refusals})
			resp := api.DistributedTasksDistributedTasksGetHandler.Handle(
				distributed_tasks.DistributedTasksGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/tasks", nil)},
				&models.Principal{Username: "cluster-reader"})
			ok, isOK := resp.(*distributed_tasks.DistributedTasksGetOK)
			require.True(t, isOK, "got %T", resp)
			require.Len(t, ok.Payload[db.DropVectorIndexNamespace], 1)
		}},
		{name: "re-issued vector index DELETE", call: func(t *testing.T, refusals *atomic.Int32) {
			enq := &dropVectorIndexEnqueuer{clusterService: &fakeClusterDropClient{
				tasks: tasks, listErr: notCaughtUp, listRefusals: refusals,
			}}
			active, err := enq.HasActiveDrop(context.Background(), "C", "v1")
			require.NoError(t, err)
			require.True(t, active)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusals := &atomic.Int32{}
			refusals.Store(1)
			tc.call(t, refusals)
			require.Equal(t, int32(-1), refusals.Load(), "refused once, then answered")
		})
	}
}

// stubMigrationTaskRaft records whether the task list was reached at all.
type stubMigrationTaskRaft struct {
	caughtUp   bool
	local      map[string][]distributedtask.TaskStatusEntry
	cluster    map[string][]*distributedtask.Task
	clusterErr error

	localReads   int
	clusterReads int
}

func (s *stubMigrationTaskRaft) FSMHasCaughtUp() bool { return s.caughtUp }

func (s *stubMigrationTaskRaft) LocalTaskStatuses(namespace, _ string) []distributedtask.TaskStatusEntry {
	s.localReads++
	return s.local[namespace]
}

func (s *stubMigrationTaskRaft) ListDistributedTaskHeaders(context.Context) (map[string][]*distributedtask.Task, error) {
	s.clusterReads++
	if s.clusterErr != nil {
		return nil, s.clusterErr
	}
	return s.cluster, nil
}

func reindexTasks(ids ...string) []*distributedtask.Task {
	tasks := make([]*distributedtask.Task, 0, len(ids))
	for _, id := range ids {
		tasks = append(tasks, &distributedtask.Task{
			Namespace:      db.ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		})
	}
	return tasks
}

func reindexTaskStatuses(ids ...string) []distributedtask.TaskStatusEntry {
	var out []distributedtask.TaskStatusEntry
	for _, task := range reindexTasks(ids...) {
		out = append(out, task.StatusEntryFor(""))
	}
	return out
}

// A node mid-catch-up must report "I cannot tell", not an empty task list the
// reconciler would read as authoritative and act on destructively.
func TestMigrationLocalTaskSourceWithholdsUntilTheFSMHasCaughtUp(t *testing.T) {
	tests := []struct {
		name      string
		caughtUp  bool
		local     map[string][]distributedtask.TaskStatusEntry
		wantTasks []distributedtask.TaskStatusEntry
		wantKnown bool
		wantReads int
	}{
		{
			name:      "still applying its log: withholds, and does not read the task list",
			caughtUp:  false,
			local:     map[string][]distributedtask.TaskStatusEntry{db.ReindexNamespace: reindexTaskStatuses("a")},
			wantTasks: nil,
			wantKnown: false,
			wantReads: 0,
		},
		{
			name:      "caught up: reports the reindex namespace",
			caughtUp:  true,
			local:     map[string][]distributedtask.TaskStatusEntry{db.ReindexNamespace: reindexTaskStatuses("a", "b")},
			wantTasks: reindexTaskStatuses("a", "b"),
			wantKnown: true,
			wantReads: 1,
		},
		{
			name:      "caught up with no reindex tasks: an empty list is authoritative",
			caughtUp:  true,
			local:     map[string][]distributedtask.TaskStatusEntry{"other": reindexTaskStatuses("a")},
			wantTasks: nil,
			wantKnown: true,
			wantReads: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raft := &stubMigrationTaskRaft{caughtUp: test.caughtUp, local: test.local}

			tasks, known := newMigrationLocalTaskSource(raft)("")

			assert.Equal(t, test.wantKnown, known)
			assert.Equal(t, test.wantTasks, tasks)
			assert.Equal(t, test.wantReads, raft.localReads,
				"a node that cannot answer must not read a task list it would answer with")
		})
	}
}

// Cluster half of the same gate: fail loudly rather than hand back a list this
// node cannot vouch for.
func TestMigrationClusterTaskSourceRefusesUntilTheFSMHasCaughtUp(t *testing.T) {
	listErr := errors.New("leader unreachable")

	tests := []struct {
		name      string
		caughtUp  bool
		cluster   map[string][]*distributedtask.Task
		listErr   error
		wantTasks []*distributedtask.Task
		wantErr   string
		wantReads int
	}{
		{
			name:      "still applying its log: refuses, and does not query the leader",
			caughtUp:  false,
			cluster:   map[string][]*distributedtask.Task{db.ReindexNamespace: reindexTasks("a")},
			wantErr:   "still applying its RAFT log",
			wantReads: 0,
		},
		{
			name:      "caught up: reports the reindex namespace",
			caughtUp:  true,
			cluster:   map[string][]*distributedtask.Task{db.ReindexNamespace: reindexTasks("a", "b")},
			wantTasks: reindexTasks("a", "b"),
			wantReads: 1,
		},
		{
			name:      "caught up and the leader query fails: the error is passed through",
			caughtUp:  true,
			listErr:   listErr,
			wantErr:   listErr.Error(),
			wantReads: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raft := &stubMigrationTaskRaft{
				caughtUp: test.caughtUp, cluster: test.cluster, clusterErr: test.listErr,
			}

			tasks, err := newMigrationClusterTaskSource(raft)(context.Background())

			if test.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantErr)
				assert.Nil(t, tasks)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.wantTasks, tasks)
			}
			assert.Equal(t, test.wantReads, raft.clusterReads,
				"a node that cannot answer must not query a list it would answer with")
		})
	}
}
