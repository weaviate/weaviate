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

// The index status has to render the task list against the schema flags it was
// ordered with. Reading the list from the leader and the flags from local
// state was what made "FINISHED but the flag is still off" observable at all,
// and it is why the endpoint used to need a timed finalize window.
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

	h := submissionHandlers(t, svc, nil)

	req, err := http.NewRequest(http.MethodGet, "/v1/schema/Movies/indexes", nil)
	require.NoError(t, err)
	responder := h.getIndexes(restschema.SchemaObjectsIndexesGetParams{
		HTTPRequest: req,
		ClassName:   "Movies",
	}, nil)

	ok, isOK := responder.(*restschema.SchemaObjectsIndexesGetOK)
	require.True(t, isOK, "expected 200, got %T", responder)

	var filterable *models.IndexStatus
	for _, prop := range ok.Payload.Properties {
		if prop.Name != "title" {
			continue
		}
		for _, idx := range prop.Indexes {
			if idx.Type == "filterable" {
				filterable = idx
			}
		}
	}
	require.NotNil(t, filterable, "the filterable entry must be present")
	require.Equal(t, "indexing", filterable.Status,
		"the endpoint must render the local FSM's SWAPPING, not the leader's FINISHED")
	require.InDelta(t, 1.0, filterable.Progress, 0.0001)
}
