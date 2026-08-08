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

// Three predicates answer questions about one task and have to agree.
// Pins: for a task in PREPARING/SWAPPING (live to the backup gate but past
// STARTED, which is all CancelTask accepts), cancel must refuse with a clear
// message rather than reach DTM and surface its rejection as NO_OP or a 500.
func TestCancelRefusesATaskThatHasPassedThePointOfCancellation(t *testing.T) {
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
		// wantConflict is true when cancel must refuse rather than answer NO_OP.
		wantConflict bool
		// wantCancelled is the task ID cancel must send to DTM, empty for none.
		wantCancelled string
	}{
		{
			name:         "preparing, payload decodes",
			tasks:        []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusPreparing, collection)},
			wantConflict: true,
		},
		{
			name: "a status this build does not recognize",
			tasks: []*distributedtask.Task{
				decodable("t1", distributedtask.TaskStatus("REBALANCING"), collection),
			},
			wantConflict: true,
		},
		{
			name: "preparing, payload will not decode",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", collection, distributedtask.TaskStatusPreparing),
			},
			wantConflict: true,
		},
		{
			name:  "preparing on another collection",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusPreparing, "Reviews")},
		},
		{
			name: "preparing, unreadable, on another collection",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", "Reviews", distributedtask.TaskStatusPreparing),
			},
		},
		{
			name:  "terminal task is nothing to cancel",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusFinished, collection)},
		},
		{
			// The gate refuses on either, but only one of them can be cancelled,
			// so the operator gets the cancel rather than the refusal.
			name: "a started task next to a swapping one",
			tasks: []*distributedtask.Task{
				decodable("t-swapping", distributedtask.TaskStatusSwapping, collection),
				decodable("t-started", distributedtask.TaskStatusStarted, collection),
			},
			wantCancelled: "t-started",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: tc.tasks}
			h := cancelHandlers(t, svc)

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			if tc.wantConflict {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok,
					"the gate refuses backups of this collection and names cancel as the remedy; "+
						"answering NO_OP leaves the operator looping between a refused backup and a "+
						"cancel that reports nothing to do, got %T", responder)
				require.Equal(t, pastCancellationRefusal(t), errorMessage(t, conflict.Payload))
				require.Empty(t, svc.cancelled,
					"DTM rejects a cancel in these states, so sending one turns a wrong answer into a 500")
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
			require.Equal(t, tc.wantCancelled, svc.cancelled[0].ID,
				"the cancellable task must win over the one DTM would reject")
		})
	}
}

func pastCancellationRefusal(t *testing.T) string {
	t.Helper()
	conflict, ok := reindexCancelPastCancellationPoint(&models.Principal{Username: "u1"}).(*schema.SchemaObjectsIndexesUpdateConflict)
	require.True(t, ok)
	return conflict.Payload.Error[0].Message
}
