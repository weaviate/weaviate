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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// fsmStep is one applied-index position: the task list and the class as this
// node answers them at that point.
type fsmStep struct {
	tasks map[string][]*distributedtask.Task
	class *models.Class
}

// fsmSnapshot models one node's FSM: an applied index that can advance between
// two independent local reads, plus the leader's view of the same tasks.
//
//	step 0 — task STARTED,  searchable flag off
//	step 1 — task FINISHED, searchable flag on   (OnTaskCompleted committed the
//	                                              flip, then finalize)
type fsmSnapshot struct {
	step                int
	advanceBetweenReads bool
	leaderTasks         map[string][]*distributedtask.Task
	steps               []fsmStep
}

func (f *fsmSnapshot) LocalDistributedTasks() map[string][]*distributedtask.Task {
	out := f.steps[f.step].tasks
	f.tick()
	return out
}

// ListDistributedTasks is the leader's view. The status read must not use it:
// the leader can report a task committed at an index this node's schema has
// not applied.
func (f *fsmSnapshot) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return f.leaderTasks, nil
}

func (f *fsmSnapshot) ReadOnlyClass(string) *models.Class {
	out := f.steps[f.step].class
	f.tick()
	return out
}

func (f *fsmSnapshot) tick() {
	if f.advanceBetweenReads && f.step == 0 {
		f.step = 1
	}
}

func newFSMSnapshot(t *testing.T) *fsmSnapshot {
	t.Helper()

	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableSearchable,
		Collection:    "C",
		Properties:    []string{"p"},
	}
	tasks := func(status distributedtask.TaskStatus) map[string][]*distributedtask.Task {
		return map[string][]*distributedtask.Task{
			db.ReindexNamespace: {buildTask(t, "C:enable-searchable:p:0001", status, payload, nil)},
		}
	}
	class := func(flagOn bool) *models.Class {
		return &models.Class{
			Class:      "C",
			Properties: []*models.Property{{Name: "p", IndexSearchable: &flagOn}},
		}
	}

	return &fsmSnapshot{steps: []fsmStep{
		{tasks: tasks(distributedtask.TaskStatusStarted), class: class(false)},
		{tasks: tasks(distributedtask.TaskStatusFinished), class: class(true)},
	}}
}

// The index-status response compares a task's status against a schema flag.
// Both operands must come from this node and the task list must be read first.
// Otherwise the schema can be the older of the two: the flag reads off while
// the task already reads FINISHED, no synthetic entry is produced, the entry is
// dropped at the emit gate, and the response is `"indexes": []` — which the UI
// renders as "None".
//
// Each row would go green again if the production read order or the production
// read source were reverted; see the mutation receipts in the PR description.
func TestIndexStatusOperands_ComeFromOneNodeInOneOrder(t *testing.T) {
	tests := []struct {
		name        string
		advance     bool
		leaderAhead bool
		noLister    bool
		wantNoTasks bool
		wantStatus  distributedtask.TaskStatus
		wantFlagOn  bool
	}{
		{
			name:       "schema is never the older operand",
			advance:    true,
			wantStatus: distributedtask.TaskStatusStarted,
			wantFlagOn: true,
		},
		{
			name:        "task list comes from this node not the leader",
			leaderAhead: true,
			wantStatus:  distributedtask.TaskStatusStarted,
			wantFlagOn:  false,
		},
		{
			name:        "no cluster service",
			noLister:    true,
			wantNoTasks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := newFSMSnapshot(t)
			snapshot.advanceBetweenReads = tt.advance
			if tt.leaderAhead {
				snapshot.leaderTasks = snapshot.steps[1].tasks
			}

			var lister localTaskLister
			if !tt.noLister {
				lister = snapshot
			}

			class, parsed := indexStatusOperands("C", lister, snapshot)
			require.NotNil(t, class)
			if tt.wantNoTasks {
				require.Empty(t, parsed)
				return
			}
			require.Len(t, parsed, 1)
			require.Equal(t, tt.wantStatus, parsed[0].task.Status)
			require.Equal(t, tt.wantFlagOn, *class.Properties[0].IndexSearchable)
		})
	}
}
