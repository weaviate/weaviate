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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

func unreadableTask(id, collection string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 3},
		Status:         status,
		Payload:        retypedFieldPayload(collection),
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
// fallback: FINISHED tasks live for days, so one often sits next to the live
// task the backup gate is refusing on. The finalize window is separately
// pinned by TestMergeReindexStatus_FinishedBeforeSchemaFlip.
func TestIndexStatusFallsBackWhenTheMatchedTaskStillReadsReady(t *testing.T) {
	matched := buildTask(t, "t-finished", distributedtask.TaskStatusFinished, db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}, map[string]*distributedtask.Unit{
		"u1": {Status: distributedtask.UnitStatusCompleted},
	})
	matched.FinishedAt = time.Now()

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "Movies", "title", "filterable", true,
		tasksMap(matched, unreadableTask("t-poison", "Movies", distributedtask.TaskStatusStarted)),
		time.Hour, nil)

	require.Equal(t, models.IndexStatusStatusPending, idx.Status,
		"the live unreadable task still holds the collection at the backup gate, and the "+
			"refusal sends the operator here to poll until every index reads ready")
	require.Zero(t, idx.Progress, "no progress was readable")
}

// unattributableTaskPayload defeats the full decoder AND the lenient
// collection reader. Nothing in it says which shards the task holds, so the
// backup gate refuses every collection in the cluster rather than one.
func unattributableTaskPayload() []byte {
	return []byte(`{"unitToShard":"a-newer-node-changed-this-shape"}`)
}

func unattributableTask(id string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 3},
		Status:         status,
		Payload:        unattributableTaskPayload(),
	}
}

// renamedFieldTask is the other way a payload can name no collection, and the
// one a decoder cannot see. It holds the same cluster-wide gate as a payload
// that will not decode at all, so every pass that handles one has to handle
// this too.
func renamedFieldTask(id string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 3},
		Status:         status,
		Payload:        renamedFieldPayload("Movies"),
	}
}

// A live task that names no collection refuses every backup and every restore
// in the cluster, and no collection's status endpoint can report it. If cancel
// cannot reach it either, the only exit left is RUNTIME_REINDEX_ENABLED=false,
// and a restart does not help because the task lives in RAFT.
func TestCancelClearsATaskThatNamesNoCollection(t *testing.T) {
	const collection = "Movies"

	// The table is built with the outer t, so the rows are plain slices.
	onMovies := func(id string) *distributedtask.Task {
		return buildTask(t, id, distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeRepairFilterable,
			Collection:    collection,
			Properties:    []string{"title"},
		}, nil)
	}
	orphan := unattributableTask("orphan", distributedtask.TaskStatusStarted)

	tests := []struct {
		name  string
		tasks []*distributedtask.Task
		// principal is nil for a caller confined to no namespace.
		principal *models.Principal
		// wantCancelledID is empty when nothing may be cancelled in DTM.
		wantCancelledID string
		// wantTaskID is the id the response hands back — the operator's only
		// handle on a task they now have to watch finish.
		wantTaskID string
		wantStatus string
		// wantRefusedID is the task a 409 must name when DTM will not accept
		// the cancel. Set instead of wantStatus.
		wantRefusedID string
	}{
		{
			name:            "the only live task names no collection",
			tasks:           []*distributedtask.Task{orphan},
			wantCancelledID: "orphan",
			wantTaskID:      "orphan",
			wantStatus:      "CANCELLED",
		},
		{
			name:            "a decodable task on this collection wins",
			tasks:           []*distributedtask.Task{orphan, onMovies("t-decodable")},
			wantCancelledID: "t-decodable",
			wantTaskID:      "t-decodable",
			wantStatus:      "CANCELLED",
		},
		{
			name: "an unreadable task naming this collection wins",
			tasks: []*distributedtask.Task{
				orphan, unreadableTask("t-named", collection, distributedtask.TaskStatusStarted),
			},
			wantCancelledID: "t-named",
			wantTaskID:      "t-named",
			wantStatus:      "CANCELLED",
		},
		{
			name:            "the only live task has a renamed collection field",
			tasks:           []*distributedtask.Task{renamedFieldTask("renamed", distributedtask.TaskStatusStarted)},
			wantCancelledID: "renamed",
			wantTaskID:      "renamed",
			wantStatus:      "CANCELLED",
		},
		{
			name:       "a terminal task holds no gate and is left alone",
			tasks:      []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusFinished)},
			wantStatus: reindexCancelStatusNoOp,
		},
		{
			// DTM refuses a cancel once the task is past its units, so this
			// one cannot be cleared — but it is still holding the gates, so
			// the answer names it rather than reporting nothing to cancel.
			name:          "past its units is refused, not reported as nothing to cancel",
			tasks:         []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusSwapping)},
			wantRefusedID: "orphan",
		},
		{
			// The id is the caller's handle on the cancel, and a namespaced
			// caller has never seen their own prefix on anything.
			name:            "a namespaced caller gets their own prefix stripped off the id",
			tasks:           []*distributedtask.Task{onMovies("acme:t-namespaced")},
			principal:       &models.Principal{Username: "u1", Namespace: "acme"},
			wantCancelledID: "acme:t-namespaced",
			wantTaskID:      "t-namespaced",
			wantStatus:      "CANCELLED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: tc.tasks}
			h := cancelHandlers(t, svc)

			principal := tc.principal
			if principal == nil {
				principal = &models.Principal{Username: "u1"}
			}
			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable", principal)

			if tc.wantRefusedID != "" {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "a task past its units must be refused, got %T", responder)
				require.Contains(t, conflict.Payload.Error[0].Message, tc.wantRefusedID,
					"the refusal must name the task still holding every backup in the cluster")
				require.Empty(t, svc.cancelled, "a refused cancel must not reach DTM")
				return
			}

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "cancel must be accepted, got %T", responder)
			require.Equal(t, tc.wantStatus, accepted.Payload.Status)

			if tc.wantCancelledID == "" {
				require.Empty(t, svc.cancelled, "nothing was holding the gate")
				require.Empty(t, accepted.Payload.TaskID,
					"a NO_OP must name no task, or it discloses one the caller may not reach")
				return
			}
			require.Len(t, svc.cancelled, 1)
			require.Equal(t, tc.wantCancelledID, svc.cancelled[0].ID,
				"the task holding the gate is the one that must be cancelled")
			require.Equal(t, tc.wantTaskID, accepted.Payload.TaskID,
				"the caller has to get the id back, stripped of their own namespace, "+
					"or they cannot poll the cancel they just asked for")
		})
	}
}
