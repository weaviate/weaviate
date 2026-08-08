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

	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// A migration past STARTED cannot be cancelled, and it does not always finish:
// if a node that owned part of it left the cluster, it sits in PREPARING or
// SWAPPING for good while holding every backup and restore refusal it holds.
// A refusal that answers "poll until it says ready" sends the operator into a
// loop with no exit named.
func TestPastCancellationRefusalsDoNotPromiseThatPollingEndsIt(t *testing.T) {
	tests := []struct {
		name      string
		responder middleware.Responder
	}{
		{
			name:      "the task names a collection",
			responder: reindexCancelPastCancellationPoint(&models.Principal{Username: "u1"}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conflict, ok := tc.responder.(*schema.SchemaObjectsIndexesUpdateConflict)
			require.Truef(t, ok, "past-cancellation refusals are 409s, got %T", tc.responder)

			msg := conflict.Payload.Error[0].Message
			require.Contains(t, msg, "RUNTIME_REINDEX_ENABLED=false",
				"a restart with the flag off is what lifts the backup and restore refusals "+
					"when the migration never finishes; it does not end the migration")
			require.Contains(t, msg, "STARTED",
				"the operator has to learn that cancel only works before this point")
			require.NotContains(t, msg, "until every index reports",
				"polling is not a remedy: a migration wedged by a departed node never reports ready")
		})
	}
}

// pastCancellationRefusal is the message reindexCancelPastCancellationPoint
// answers with, for the tests that assert two cancel paths agree on one
// wording.
func pastCancellationRefusal(t *testing.T) string {
	t.Helper()
	conflict, ok := reindexCancelPastCancellationPoint(&models.Principal{Username: "u1"}).(*schema.SchemaObjectsIndexesUpdateConflict)
	require.True(t, ok)
	return conflict.Payload.Error[0].Message
}

// Pins that the REST pre-check offers the same status-aware remedy as
// [db.ReindexProvider.CheckPropertyUpdate].
func TestPropertyMutationPreCheckCarriesTheSameRemedyAsTheApplyGate(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"name"},
	})
	require.NoError(t, err)

	cancelWorks := []string{
		`{"<indexType>":{"cancel":true}}`,
		"or wait for it to finish",
	}
	cancelRefused := []string{
		"past the point where cancel works",
		"can only be waited out",
		"wedges it here for good",
	}

	tests := []struct {
		status     distributedtask.TaskStatus
		wantText   []string
		refuseText []string
	}{
		{distributedtask.TaskStatusStarted, cancelWorks, cancelRefused},
		{distributedtask.TaskStatusPreparing, cancelRefused, cancelWorks},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			h := &schemaHandlers{
				reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
					db.ReindexNamespace: {{
						Namespace:      db.ReindexNamespace,
						TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
						Status:         tc.status,
						Payload:        payload,
					}},
				}},
			}

			reason := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "name")
			require.NotEmpty(t, reason)
			require.Contains(t, reason, "T_remedy")
			require.Contains(t, reason, string(tc.status))
			for _, want := range tc.wantText {
				require.Contains(t, reason, want)
			}
			for _, unwanted := range tc.refuseText {
				require.NotContains(t, reason, unwanted)
			}
		})
	}
}
