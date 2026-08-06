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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// unreadableTaskPayload defeats the full ReindexTaskPayload decoder while
// leaving the collection readable: unitToShard is a map today, and a newer
// node shipping it as a string is what a rolling upgrade produces.
func unreadableTaskPayload(collection string) []byte {
	return []byte(`{"collection":"` + collection + `","unitToShard":"a-newer-node-changed-this-shape"}`)
}

func unreadableTask(id, collection string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 3},
		Status:         status,
		Payload:        unreadableTaskPayload(collection),
	}
}

// The backup gate refuses a whole collection when a live reindex task's
// payload will not decode, and the refusal tells the operator to poll this
// endpoint until every index reads "ready". Reporting "ready" for that
// collection points them back at a backup that keeps being refused.
func TestIndexStatusSurfacesATaskWhosePayloadWillNotDecode(t *testing.T) {
	tests := []struct {
		name       string
		task       *distributedtask.Task
		collection string
		wantStatus string
	}{
		{
			name:       "live task on this collection",
			task:       unreadableTask("t1", "Movies", distributedtask.TaskStatusStarted),
			collection: "Movies",
			wantStatus: models.IndexStatusStatusPending,
		},
		{
			name:       "live task, collection cased differently",
			task:       unreadableTask("t1", "movies", distributedtask.TaskStatusStarted),
			collection: "Movies",
			wantStatus: models.IndexStatusStatusPending,
		},
		{
			name:       "live task in a swapping status",
			task:       unreadableTask("t1", "Movies", distributedtask.TaskStatusSwapping),
			collection: "Movies",
			wantStatus: models.IndexStatusStatusPending,
		},
		{
			name:       "live task on another collection",
			task:       unreadableTask("t1", "Reviews", distributedtask.TaskStatusStarted),
			collection: "Movies",
			wantStatus: "ready",
		},
		{
			name:       "terminal task blocks nothing",
			task:       unreadableTask("t1", "Movies", distributedtask.TaskStatusFinished),
			collection: "Movies",
			wantStatus: "ready",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, tc.collection, "title", "filterable", true,
				tasksMap(tc.task), time.Hour, nil)
			require.Equal(t, tc.wantStatus, idx.Status,
				"the status endpoint is the remedy the backup refusal names")
			require.Zero(t, idx.Progress, "no progress was readable")
		})
	}
}

// A decodable task on the same collection still wins: it can be matched to
// the exact property and index type, so its real progress is the better
// answer than the collection-wide fallback.
func TestIndexStatusPrefersADecodableTaskOverTheFallback(t *testing.T) {
	live := buildTask(t, "t-live", distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}, map[string]*distributedtask.Unit{
		"u1": {Status: distributedtask.UnitStatusInProgress},
	})

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "Movies", "title", "filterable", true,
		tasksMap(live, unreadableTask("t-poison", "Movies", distributedtask.TaskStatusStarted)),
		time.Hour, nil)

	require.Equal(t, models.IndexStatusStatusIndexing, idx.Status)
}

// A decodable task that leaves the entry at "ready" must not swallow the
// fallback. FINISHED tasks live for DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS
// (5 days by default), so any collection reindexed in the last week has one
// sitting next to the live task the backup gate is refusing on.
func TestIndexStatusFallsBackWhenTheMatchedTaskStillReadsReady(t *testing.T) {
	finished := func(finishedAt time.Time) *distributedtask.Task {
		task := buildTask(t, "t-finished", distributedtask.TaskStatusFinished, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeRepairFilterable,
			Collection:    "Movies",
			Properties:    []string{"title"},
		}, map[string]*distributedtask.Unit{
			"u1": {Status: distributedtask.UnitStatusCompleted},
		})
		task.FinishedAt = finishedAt
		return task
	}

	unknownStatus := buildTask(t, "t-unknown", distributedtask.TaskStatus("REBALANCING"), db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}, nil)

	tests := []struct {
		name    string
		matched *distributedtask.Task
		flagOn  bool
	}{
		{
			name:    "finished, schema flag already caught up",
			matched: finished(time.Now()),
			flagOn:  true,
		},
		{
			name:    "finished a day ago, outside the finalize window",
			matched: finished(time.Now().Add(-24 * time.Hour)),
			flagOn:  false,
		},
		{
			name:    "a status this build does not know",
			matched: unknownStatus,
			flagOn:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "Movies", "title", "filterable", tc.flagOn,
				tasksMap(tc.matched, unreadableTask("t-poison", "Movies", distributedtask.TaskStatusStarted)),
				time.Hour, nil)

			require.Equal(t, models.IndexStatusStatusPending, idx.Status,
				"the live unreadable task still holds the collection at the backup gate, and the "+
					"refusal sends the operator here to poll until every index reads ready")
			require.Zero(t, idx.Progress, "no progress was readable")
		})
	}
}

// The same refusal tells the operator to cancel the task. A cancel that
// answers NO_OP leaves them with no remedy at all: the payload is unreadable
// on every node, so a restart does not help either.
func TestCancelClearsATaskWhosePayloadWillNotDecode(t *testing.T) {
	const collection = "Movies"

	svc := &raceTaskService{tasks: []*distributedtask.Task{
		unreadableTask("Movies:unknown:ab3f", collection, distributedtask.TaskStatusStarted),
	}}

	var busy atomic.Bool
	h := submissionHandlers(t, svc, togglingProber{busy: &busy})
	h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, fixtureNode,
		func() int { return 1 }, context.Background()))

	responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "cancel must be accepted, got %T", responder)
	require.Equal(t, "CANCELLED", accepted.Payload.Status,
		"the task the backup refusal named must be cancellable")
	require.Len(t, svc.cancelled, 1, "the task must have been cancelled in DTM")
	require.Equal(t, "Movies:unknown:ab3f", svc.cancelled[0].ID)
}

// The fallback matches on the collection alone, because that is all that
// decoded. It must not reach a task naming a different collection.
func TestCancelLeavesAnUnreadableTaskOfAnotherCollectionAlone(t *testing.T) {
	svc := &raceTaskService{tasks: []*distributedtask.Task{
		unreadableTask("Reviews:unknown:ab3f", "Reviews", distributedtask.TaskStatusStarted),
	}}

	var busy atomic.Bool
	h := submissionHandlers(t, svc, togglingProber{busy: &busy})

	responder := h.cancelReindexTask(context.Background(), "Movies", "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "cancel must be accepted, got %T", responder)
	require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
	require.Empty(t, svc.cancelled)
}
