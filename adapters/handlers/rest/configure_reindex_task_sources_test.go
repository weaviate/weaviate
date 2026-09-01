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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// stubMigrationTaskRaft answers the three questions the task sources ask, and
// records whether the task list was reached at all.
type stubMigrationTaskRaft struct {
	caughtUp   bool
	local      map[string][]*distributedtask.Task
	cluster    map[string][]*distributedtask.Task
	clusterErr error

	localReads   int
	clusterReads int
}

func (s *stubMigrationTaskRaft) FSMHasCaughtUp() bool { return s.caughtUp }

func (s *stubMigrationTaskRaft) LocalDistributedTasks() map[string][]*distributedtask.Task {
	s.localReads++
	return s.local
}

func (s *stubMigrationTaskRaft) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
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

// TestMigrationLocalTaskSourceWithholdsUntilTheFSMHasCaughtUp pins the gate a
// node mid-catch-up needs: its own applied view is incomplete, so it must
// report "I cannot tell", not an empty task list the reconciler would read as
// authoritative and act on destructively.
func TestMigrationLocalTaskSourceWithholdsUntilTheFSMHasCaughtUp(t *testing.T) {
	tests := []struct {
		name      string
		caughtUp  bool
		local     map[string][]*distributedtask.Task
		wantTasks []*distributedtask.Task
		wantKnown bool
		wantReads int
	}{
		{
			name:      "still applying its log: withholds, and does not read the task list",
			caughtUp:  false,
			local:     map[string][]*distributedtask.Task{db.ReindexNamespace: reindexTasks("a")},
			wantTasks: nil,
			wantKnown: false,
			wantReads: 0,
		},
		{
			name:      "caught up: reports the reindex namespace",
			caughtUp:  true,
			local:     map[string][]*distributedtask.Task{db.ReindexNamespace: reindexTasks("a", "b")},
			wantTasks: reindexTasks("a", "b"),
			wantKnown: true,
			wantReads: 1,
		},
		{
			name:      "caught up with no reindex tasks: an empty list is authoritative",
			caughtUp:  true,
			local:     map[string][]*distributedtask.Task{"other": reindexTasks("a")},
			wantTasks: nil,
			wantKnown: true,
			wantReads: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raft := &stubMigrationTaskRaft{caughtUp: test.caughtUp, local: test.local}

			tasks, known := newMigrationLocalTaskSource(raft)()

			assert.Equal(t, test.wantKnown, known)
			assert.Equal(t, test.wantTasks, tasks)
			assert.Equal(t, test.wantReads, raft.localReads,
				"a node that cannot answer must not read a task list it would answer with")
		})
	}
}

// TestMigrationClusterTaskSourceRefusesUntilTheFSMHasCaughtUp is the cluster
// half of the same gate: the leader query must fail loudly rather than hand
// back a list this node cannot vouch for.
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
