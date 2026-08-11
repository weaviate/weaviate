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

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// The gates block schema mutations and backups for every non-terminal status
// and name cancel as the remedy. Pins: the cancel handler matches the same
// set, so a task holding the gates is never answered with NO_OP. What it gets
// instead depends on the status — DTM accepts a cancel for STARTED and for a
// status this build cannot name, and refuses it for the coordination phases
// ([distributedtask.TaskStatus.IsCancellable]) — but "the gate is clear" is
// never one of the answers while a task is holding it.
func TestCancelMatchesEveryStatusTheGatesBlockOn(t *testing.T) {
	const collection = "Movies"

	decodable := func(id string, status distributedtask.TaskStatus, coll string) *distributedtask.Task {
		return buildTask(t, id, status, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeRepairFilterable,
			Collection:    coll,
			Properties:    []string{"title"},
		}, nil)
	}

	tests := []struct {
		name string
		// tasks is the full DTM snapshot the cancel handler reads.
		tasks []*distributedtask.Task
		// gateBlocks is the gates' own predicate for this status, checked
		// alongside the cancel outcome so both halves stay in sync.
		gateBlocks bool
		// wantCancelled is the task ID cancel must send to DTM, empty for none.
		wantCancelled string
		// wantRefusedID is the task a coordination-phase refusal must name.
		// Mutually exclusive with wantCancelled; both empty means NO_OP.
		wantRefusedID string
	}{
		{
			// The unknown status stands in for PREPARING and SWAPPING too: all
			// three reach the match through the same terminal check and only
			// ever fail together. It is refused for the same reason they are:
			// [distributedtask.TaskStatus.IsCancellable] is a literal == STARTED,
			// so a node that cannot name the status never cancels a migration a
			// newer node is still coordinating.
			name: "a status this build does not recognize",
			tasks: []*distributedtask.Task{
				decodable("t1", distributedtask.TaskStatus("REBALANCING"), collection),
			},
			gateBlocks:    true,
			wantRefusedID: "t1",
		},
		{
			// Cancel matching must case-fold collection names like the other gate lookups.
			name:          "started, payload names the collection in another case",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusStarted, "MOVIES")},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			// The unreadable-payload pass still has to find it: the refusal
			// names the task holding the gate, where a NO_OP would tell the
			// operator the gate is clear.
			name: "preparing, payload will not decode, collection in another case",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", "MOVIES", distributedtask.TaskStatusPreparing),
			},
			gateBlocks:    true,
			wantRefusedID: "t1",
		},
		{
			// Past its units on the requested tuple: DTM refuses the cancel,
			// so the handler must refuse it too rather than send an apply it
			// knows will be rejected.
			name:          "swapping on the requested property",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusSwapping, collection)},
			gateBlocks:    true,
			wantRefusedID: "t1",
		},
		{
			name:       "preparing on another collection",
			tasks:      []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusPreparing, "Reviews")},
			gateBlocks: true,
		},
		{
			name: "preparing, unreadable, on another collection",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", "Reviews", distributedtask.TaskStatusPreparing),
			},
			gateBlocks: true,
		},
		{
			// FAILED and CANCELLED reach the same terminal check, so one
			// terminal row is the whole set.
			name:  "finished is nothing to cancel",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusFinished, collection)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, task := range tc.tasks {
				require.Equalf(t, tc.gateBlocks, db.IsLiveReindexTaskStatus(task.Status),
					"the gates read %q as in-flight=%v; cancel below has to match the same set, "+
						"or the remedy the refusal names does not reach the task holding it",
					task.Status, tc.gateBlocks)
			}

			svc := &raceTaskService{tasks: tc.tasks}
			h := cancelHandlers(t, svc)

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			if tc.wantRefusedID != "" {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "a task past its units must be refused, got %T", responder)
				require.Contains(t, conflict.Payload.Error[0].Message, tc.wantRefusedID,
					"the refusal has to name the task still holding the gate")
				require.Empty(t, svc.cancelled,
					"a refused cancel must not reach DTM")
				return
			}

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "cancel must be accepted, got %T", responder)
			if tc.wantCancelled == "" {
				require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
				require.Empty(t, svc.cancelled)
				return
			}
			require.Equal(t, "CANCELLED", accepted.Payload.Status)
			require.Len(t, svc.cancelled, 1)
			require.Equal(t, tc.wantCancelled, svc.cancelled[0].ID)
		})
	}
}

// Pins: a live task with an unrecognized migration type is refused, not
// answered NO_OP.
func TestCancelAnswersUnknownMigrationTypeInsteadOfNoOp(t *testing.T) {
	const collection = "Movies"
	// Not in db.ReindexTargetIndexes, which is what "unknown to this build" means.
	const futureType = db.ReindexMigrationType("enable-vectorable")

	task := func(id string, mt db.ReindexMigrationType, status distributedtask.TaskStatus, prop string) *distributedtask.Task {
		return buildTask(t, id, status, db.ReindexTaskPayload{
			MigrationType: mt,
			Collection:    collection,
			Properties:    []string{prop},
		}, nil)
	}

	tests := []struct {
		name  string
		tasks []*distributedtask.Task
		// wantRefused expects the 409 naming the type; otherwise a 202 is expected.
		wantRefused bool
		// wantCancelled is the task ID cancel must send to DTM, empty for none.
		wantCancelled string
	}{
		{
			name:        "live unknown type on the requested property",
			tasks:       []*distributedtask.Task{task("t1", futureType, distributedtask.TaskStatusStarted, "title")},
			wantRefused: true,
		},
		{
			name: "a known task on the same property still wins the match",
			tasks: []*distributedtask.Task{
				task("t1", futureType, distributedtask.TaskStatusStarted, "title"),
				task("t2", db.ReindexTypeRepairFilterable, distributedtask.TaskStatusStarted, "title"),
			},
			wantCancelled: "t2",
		},
		{
			name:  "terminal unknown type blocks nothing",
			tasks: []*distributedtask.Task{task("t1", futureType, distributedtask.TaskStatusFinished, "title")},
		},
		{
			name:  "live unknown type on another property",
			tasks: []*distributedtask.Task{task("t1", futureType, distributedtask.TaskStatusStarted, "director")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: tc.tasks}
			h := cancelHandlers(t, svc)

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			if tc.wantRefused {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "an undecidable cancel must be refused, got %T", responder)
				require.Contains(t, conflict.Payload.Error[0].Message, string(futureType),
					"the refusal has to name the type so the operator knows which node to retry on")
				require.Empty(t, svc.cancelled)
				return
			}

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "cancel must be accepted, got %T", responder)
			if tc.wantCancelled == "" {
				require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
				require.Empty(t, svc.cancelled)
				return
			}
			require.Equal(t, "CANCELLED", accepted.Payload.Status)
			require.Len(t, svc.cancelled, 1)
			require.Equal(t, tc.wantCancelled, svc.cancelled[0].ID)
		})
	}
}
