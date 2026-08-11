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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
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

// unattributableTaskPayload defeats the full ReindexTaskPayload decoder AND
// the lenient collection reader. Nothing in it says which shards the task
// holds, so the backup gate refuses every collection in the cluster rather
// than one.
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
// one a decoder cannot see: a newer node renames the collection field, Go
// ignores the unknown key, and this decodes without error into an empty
// payload. It holds the same cluster-wide gate as a payload that will not
// decode at all, so every pass that handles one has to handle this too.
func renamedFieldTask(t *testing.T, id string, status distributedtask.TaskStatus) *distributedtask.Task {
	t.Helper()
	raw := []byte(`{"collektion":"Movies","unitToShard":{"u1":"shard1"}}`)

	var probe db.ReindexTaskPayload
	require.NoError(t, json.Unmarshal(raw, &probe),
		"this fixture is only meaningful while the payload decodes without error")
	require.Empty(t, probe.Collection,
		"this fixture is only meaningful while the decoded collection is empty")

	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 3},
		Status:         status,
		Payload:        raw,
	}
}

// A live task that names no collection refuses every backup and every restore
// in the cluster, and no collection's status endpoint can report it. If cancel
// cannot reach it either, the only exit left is RUNTIME_REINDEX_ENABLED=false,
// and a restart does not help because the task lives in RAFT.
func TestCancelClearsATaskThatNamesNoCollection(t *testing.T) {
	const collection = "Movies"

	decodableOnMovies := func(t *testing.T) *distributedtask.Task {
		return buildTask(t, "t-decodable", distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeRepairFilterable,
			Collection:    collection,
			Properties:    []string{"title"},
		}, nil)
	}

	tests := []struct {
		name  string
		tasks func(t *testing.T) []*distributedtask.Task
		// authorizer is nil for a caller with every grant.
		authorizer authorization.Authorizer
		// principal is nil for a caller confined to no namespace.
		principal *models.Principal
		// wantCancelledID is empty when nothing may be cancelled in DTM.
		wantCancelledID string
		// wantTaskID is the id the response hands back — the operator's only
		// handle on a task they now have to watch finish.
		wantTaskID string
		wantStatus string
	}{
		{
			name: "the only live task names no collection",
			tasks: func(*testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusStarted)}
			},
			wantCancelledID: "orphan",
			wantTaskID:      "orphan",
			wantStatus:      "CANCELLED",
		},
		{
			name: "a decodable task on this collection wins",
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					unattributableTask("orphan", distributedtask.TaskStatusStarted),
					decodableOnMovies(t),
				}
			},
			wantCancelledID: "t-decodable",
			wantTaskID:      "t-decodable",
			wantStatus:      "CANCELLED",
		},
		{
			name: "an unreadable task naming this collection wins",
			tasks: func(*testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					unattributableTask("orphan", distributedtask.TaskStatusStarted),
					unreadableTask("t-named", collection, distributedtask.TaskStatusStarted),
				}
			},
			wantCancelledID: "t-named",
			wantTaskID:      "t-named",
			wantStatus:      "CANCELLED",
		},
		{
			name: "the only live task has a renamed collection field",
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{renamedFieldTask(t, "renamed", distributedtask.TaskStatusStarted)}
			},
			wantCancelledID: "renamed",
			wantTaskID:      "renamed",
			wantStatus:      "CANCELLED",
		},
		{
			name: "a terminal task holds no gate and is left alone",
			tasks: func(*testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusFinished)}
			},
			wantStatus: reindexCancelStatusNoOp,
		},
		{
			// DTM cancels every non-terminal status, so a task that left
			// STARTED is still cancellable — the gates it holds must clear.
			name: "past STARTED is still cancellable",
			tasks: func(*testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusSwapping)}
			},
			wantCancelledID: "orphan",
			wantTaskID:      "orphan",
			wantStatus:      "CANCELLED",
		},
		{
			// Cancelling it stops a migration on some other collection and
			// answers with a task id naming that collection, so the URL's own
			// grant is not enough. The answer is the one a caller would get if
			// this pass did not exist, so a denial discloses nothing.
			name: "UPDATE on the URL's collection alone cannot reach a task that names none",
			tasks: func(*testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{unattributableTask("orphan", distributedtask.TaskStatusStarted)}
			},
			authorizer: grantUpdateOn(collection),
			wantStatus: reindexCancelStatusNoOp,
		},
		{
			// The id is the caller's handle on the cancel, and a namespaced
			// caller has never seen their own prefix on anything.
			name: "a namespaced caller gets their own prefix stripped off the id",
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					buildTask(t, "acme:t-namespaced", distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
						MigrationType: db.ReindexTypeRepairFilterable,
						Collection:    collection,
						Properties:    []string{"title"},
					}, nil),
				}
			},
			principal:       &models.Principal{Username: "u1", Namespace: "acme"},
			wantCancelledID: "acme:t-namespaced",
			wantTaskID:      "t-namespaced",
			wantStatus:      "CANCELLED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: tc.tasks(t)}
			h := cancelHandlers(t, svc)
			if tc.authorizer != nil {
				h.appState.Authorizer = tc.authorizer
			}

			principal := tc.principal
			if principal == nil {
				principal = &models.Principal{Username: "u1"}
			}
			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable", principal)

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

// A task in a status this build does not know is live to the backup gate
// ([db.IsLiveReindexTaskStatus]), and the refusal sends the operator to this
// endpoint to poll until every index reads "ready". Reporting "ready" for it —
// or preferring an older terminal attempt over it — makes that a loop.
func TestIndexStatusSurfacesATaskInAStatusThisBuildDoesNotKnow(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    "Movies",
		Properties:    []string{"title"},
	}
	unknown := func(t *testing.T) *distributedtask.Task {
		return buildTask(t, "t-unknown", distributedtask.TaskStatus("REBALANCING"), payload, nil)
	}
	failed := func(t *testing.T) *distributedtask.Task {
		return buildTask(t, "t-failed", distributedtask.TaskStatusFailed, payload,
			map[string]*distributedtask.Unit{"u1": {Status: distributedtask.UnitStatusFailed}})
	}

	tests := []struct {
		name  string
		tasks func(t *testing.T) []*distributedtask.Task
	}{
		{
			name: "next to a terminal attempt on the same property",
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{failed(t), unknown(t)}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "Movies", "title", "filterable", true,
				tasksMap(tc.tasks(t)...), time.Hour, nil)

			require.Equal(t, models.IndexStatusStatusPending, idx.Status,
				"the task still holds this collection at the backup gate")
			require.Zero(t, idx.Progress, "no progress is readable from a phase this build does not know")
		})
	}
}
