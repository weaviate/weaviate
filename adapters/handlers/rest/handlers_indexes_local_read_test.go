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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	restschema "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// splitViewTaskService answers the leader query and the local FSM read
// differently, which is the whole point of the distinction.
type splitViewTaskService struct {
	reindexTaskService
	leader []*distributedtask.Task
	local  []*distributedtask.Task
}

func (s *splitViewTaskService) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{db.ReindexNamespace: s.leader}, nil
}

func (s *splitViewTaskService) ListDistributedTasksLocal(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{db.ReindexNamespace: s.local}, nil
}

// The index status renders the task list against the schema flags it is
// ordered with, so both have to come from this node. A list read at the leader
// can carry a FINISHED task whose schema flip this node has not applied yet,
// which renders as "finished, but the index is still off".
func TestGetIndexes_ReadsTasksFromTheLocalFSM(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}
	units := map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted, Progress: 1.0},
	}

	svc := &splitViewTaskService{
		// The leader is ahead: it has already applied the task's ending.
		leader: []*distributedtask.Task{
			buildTask(t, "Movies:enable-filterable:title", distributedtask.TaskStatusFinished, payload, units),
		},
		// This node has not, so its schema flags still belong to a swap in flight.
		local: []*distributedtask.Task{
			buildTask(t, "Movies:enable-filterable:title", distributedtask.TaskStatusSwapping, payload, units),
		},
	}

	filterable := getFilterableEntry(t, submissionHandlers(t, svc, nil))
	require.NotNil(t, filterable, "the filterable entry must be present")
	require.Equal(t, "indexing", filterable.Status,
		"the endpoint must render the local FSM's SWAPPING, not the leader's FINISHED")
	require.InDelta(t, 1.0, filterable.Progress, 0.0001)
}

// Whether an entry is emitted at all is the handler's call, not
// mergeReindexStatus's: flag on → always emit, flag off → emit only when a
// task carries actionable signal. Deleting the FINISHED override moved this
// decision here for good, so this is where it is pinned.
func TestGetIndexes_FlagOffEmitsOnlyOnActionableSignal(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}
	units := map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted, Progress: 1.0},
	}

	tests := []struct {
		name       string
		flagOn     bool
		taskStatus distributedtask.TaskStatus
		wantEntry  bool
		wantStatus string
	}{
		{
			name:      "flag off, no task",
			wantEntry: false,
		},
		{
			name:       "flag off, task FINISHED",
			taskStatus: distributedtask.TaskStatusFinished,
			wantEntry:  false,
		},
		{
			name:       "flag off, swap in flight",
			taskStatus: distributedtask.TaskStatusSwapping,
			wantEntry:  true,
			wantStatus: "indexing",
		},
		{
			name:       "flag off, task FAILED",
			taskStatus: distributedtask.TaskStatusFailed,
			wantEntry:  true,
			wantStatus: "failed",
		},
		{
			name:       "flag on, no task",
			flagOn:     true,
			wantEntry:  true,
			wantStatus: "ready",
		},
		{
			name:       "flag on, task FINISHED",
			flagOn:     true,
			taskStatus: distributedtask.TaskStatusFinished,
			wantEntry:  true,
			wantStatus: "ready",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flagOn := tc.flagOn
			class := &models.Class{
				Class: "Movies",
				Properties: []*models.Property{{
					Name: "title", DataType: []string{"text"}, IndexFilterable: &flagOn,
				}},
			}

			var local []*distributedtask.Task
			if tc.taskStatus != "" {
				local = []*distributedtask.Task{
					buildTask(t, "Movies:enable-filterable:title", tc.taskStatus, payload, units),
				}
			}

			filterable := getFilterableEntry(t,
				submissionHandlersForClass(t, &splitViewTaskService{local: local}, nil, class))

			if !tc.wantEntry {
				require.Nil(t, filterable, "a flag-off index with nothing to report must not be listed")
				return
			}
			require.NotNil(t, filterable)
			require.Equal(t, tc.wantStatus, filterable.Status)
		})
	}
}

// The local read is for rendering, not for deciding. Admission has to answer
// at the leader: a follower that has not yet applied the leader's task would
// admit a second reindex on a bucket that is already migrating.
func TestUpdateIndex_ChecksConflictsAgainstTheLeader(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRebuildSearchable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}
	units := map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.5},
	}

	svc := &splitViewTaskService{
		// The leader knows a migration is running on this property.
		leader: []*distributedtask.Task{
			buildTask(t, "Movies:rebuild-searchable:title", distributedtask.TaskStatusStarted, payload, units),
		},
		// This node has not applied it yet.
		local: nil,
	}

	responder := submitReindex(submissionHandlers(t, svc, nil))
	_, isConflict := responder.(*restschema.SchemaObjectsIndexesUpdateConflict)
	require.True(t, isConflict,
		"admission must refuse against the leader's task list, got %T", responder)
}

// erroringLocalReadService fails only the local read, which is the one failure
// mode the status endpoint has left.
type erroringLocalReadService struct {
	reindexTaskService
}

func (erroringLocalReadService) ListDistributedTasksLocal(context.Context) (map[string][]*distributedtask.Task, error) {
	return nil, errors.New("local FSM read failed")
}

// A task list this node cannot read must not be answered as "no tasks":
// every index would render `ready` and the operator would go on to a write
// the submit gate then refuses.
func TestGetIndexes_UnreadableLocalTaskListFailsClosed(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/v1/schema/Movies/indexes", nil)
	require.NoError(t, err)

	h := submissionHandlers(t, erroringLocalReadService{}, nil)
	responder := h.getIndexes(restschema.SchemaObjectsIndexesGetParams{
		HTTPRequest: req,
		ClassName:   "Movies",
	}, nil)

	require.IsType(t, &restschema.SchemaObjectsIndexesGetInternalServerError{}, responder)
}

// getFilterableEntry serves GET /indexes for the fixture collection and
// returns the "title" property's filterable entry, or nil when the response
// does not carry one.
func getFilterableEntry(t *testing.T, h *indexesHandlers) *models.IndexStatus {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "/v1/schema/Movies/indexes", nil)
	require.NoError(t, err)

	responder := h.getIndexes(restschema.SchemaObjectsIndexesGetParams{
		HTTPRequest: req,
		ClassName:   "Movies",
	}, nil)
	ok, isOK := responder.(*restschema.SchemaObjectsIndexesGetOK)
	require.True(t, isOK, "expected 200, got %T", responder)

	for _, prop := range ok.Payload.Properties {
		if prop.Name != "title" {
			continue
		}
		for _, idx := range prop.Indexes {
			if idx.Type == "filterable" {
				return idx
			}
		}
	}
	return nil
}
