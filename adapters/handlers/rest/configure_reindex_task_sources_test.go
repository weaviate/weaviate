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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

type fakeMigrationTaskRaft struct {
	caughtUp bool
	local    map[string][]*distributedtask.Task
	list     map[string][]*distributedtask.Task
	listErr  error
}

func (f *fakeMigrationTaskRaft) LocalDistributedTasks() map[string][]*distributedtask.Task {
	return f.local
}
func (f *fakeMigrationTaskRaft) FSMHasCaughtUp() bool { return f.caughtUp }
func (f *fakeMigrationTaskRaft) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return f.list, f.listErr
}

// A task absent from the map is read as terminal and licenses a discard, so a
// node still applying its RAFT tail must report its map as unusable rather
// than as empty.
func TestMigrationLocalTaskSourceMeasuresCatchUp(t *testing.T) {
	tasks := []*distributedtask.Task{{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t", Version: 7},
	}}

	tests := []struct {
		name         string
		caughtUp     bool
		local        map[string][]*distributedtask.Task
		wantReadable bool
		wantTasks    []*distributedtask.Task
	}{
		{
			name:         "still applying its tail: the map is not an answer",
			local:        map[string][]*distributedtask.Task{db.ReindexNamespace: tasks},
			wantReadable: false,
		},
		{
			name:         "caught up with a task in the namespace",
			caughtUp:     true,
			local:        map[string][]*distributedtask.Task{db.ReindexNamespace: tasks},
			wantReadable: true,
			wantTasks:    tasks,
		},
		{
			name:         "caught up with no reindex namespace at all",
			caughtUp:     true,
			local:        map[string][]*distributedtask.Task{},
			wantReadable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, readable := newMigrationLocalTaskSource(
				&fakeMigrationTaskRaft{caughtUp: tt.caughtUp, local: tt.local})()
			require.Equal(t, tt.wantReadable, readable)
			require.Equal(t, tt.wantTasks, got)
		})
	}
}

func TestMigrationClusterTaskSourceReadsTheLeader(t *testing.T) {
	tasks := []*distributedtask.Task{{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t", Version: 7},
	}}

	tests := []struct {
		name      string
		raft      *fakeMigrationTaskRaft
		wantErr   bool
		wantTasks []*distributedtask.Task
		because   string
	}{
		{
			name: "caught up, leader answers",
			raft: &fakeMigrationTaskRaft{
				caughtUp: true,
				list:     map[string][]*distributedtask.Task{db.ReindexNamespace: tasks},
			},
			wantTasks: tasks,
		},
		{
			name:    "the leader is unreachable",
			raft:    &fakeMigrationTaskRaft{caughtUp: true, listErr: errors.New("leader unreachable")},
			wantErr: true,
			because: "an unreachable leader must not read as an empty task map",
		},
		{
			// The window the gate does cover: the startup replay, where a task
			// the cluster committed is genuinely not in this node's map yet
			// and reads exactly like one the cluster removed.
			name: "still applying its tail: this node must not answer for the cluster",
			raft: &fakeMigrationTaskRaft{
				list: map[string][]*distributedtask.Task{db.ReindexNamespace: tasks},
			},
			wantErr: true,
			because: "a partial map must not be served as the cluster's list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newMigrationClusterTaskSource(tt.raft)(context.Background())
			if tt.wantErr {
				require.Error(t, err, tt.because)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantTasks, got)
		})
	}
}
