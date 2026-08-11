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
// and name cancel as the remedy, and DTM accepts a cancel for every
// non-terminal status. Pins: the cancel handler matches the same set, so a
// PREPARING/SWAPPING/unknown-status task is cancelled rather than answered
// with NO_OP while the gates stay closed.
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
	}{
		{
			name:          "started",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusStarted, collection)},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			name:          "preparing, payload decodes",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusPreparing, collection)},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			name:          "swapping, payload decodes",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusSwapping, collection)},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			name: "a status this build does not recognize",
			tasks: []*distributedtask.Task{
				decodable("t1", distributedtask.TaskStatus("REBALANCING"), collection),
			},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			name: "preparing, payload will not decode",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", collection, distributedtask.TaskStatusPreparing),
			},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			// Collection names compare case-insensitively on every other gate
			// lookup; the cancel target match must fold the same way or the
			// remedy the refusals name misses the task holding them.
			name:          "started, payload names the collection in another case",
			tasks:         []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusStarted, "MOVIES")},
			gateBlocks:    true,
			wantCancelled: "t1",
		},
		{
			name: "preparing, payload will not decode, collection in another case",
			tasks: []*distributedtask.Task{
				unreadableTask("t1", "MOVIES", distributedtask.TaskStatusPreparing),
			},
			gateBlocks:    true,
			wantCancelled: "t1",
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
			name:  "finished is nothing to cancel",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusFinished, collection)},
		},
		{
			name:  "failed is nothing to cancel",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusFailed, collection)},
		},
		{
			name:  "cancelled is nothing to cancel",
			tasks: []*distributedtask.Task{decodable("t1", distributedtask.TaskStatusCancelled, collection)},
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
