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
// two independent local reads.
//
//	step 0 — task STARTED,  searchable flag off
//	step 1 — task FINISHED, searchable flag on
type fsmSnapshot struct {
	step                int
	advanceBetweenReads bool
	steps               []fsmStep
}

func (f *fsmSnapshot) LocalDistributedTasks() map[string][]*distributedtask.Task {
	out := f.steps[f.step].tasks
	f.tick()
	return out
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

// Tasks must be read before the schema, or a stale flag-off can pair with an
// already-read FINISHED task and drop the entry.
func TestIndexStatusOperands_ComeFromOneNodeInOneOrder(t *testing.T) {
	tests := []struct {
		name        string
		advance     bool
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
			name:        "no cluster service",
			noLister:    true,
			wantNoTasks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := newFSMSnapshot(t)
			snapshot.advanceBetweenReads = tt.advance

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
