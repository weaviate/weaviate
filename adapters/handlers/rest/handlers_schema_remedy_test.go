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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestPropertyMutationPreCheckCarriesTheSameRemedyAsTheApplyGate pins that
// the REST pre-check's remedy sentence matches
// [db.ReindexProvider.CheckPropertyUpdate]'s for every status/payload shape.
func TestPropertyMutationPreCheckCarriesTheSameRemedyAsTheApplyGate(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"name"},
	})
	require.NoError(t, err)

	// Empty Properties means "all properties" to the pre-check, so a
	// whole-collection task reaches this refusal with no property to name.
	wholeCollectionPayload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
	})
	require.NoError(t, err)

	cancelCall := `cancel it via PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`
	cancelWhileRunning := []string{
		cancelCall + ", or wait for it to finish",
	}
	cancelPastUnits := []string{
		cancelCall,
		"its per-shard work is already done",
		"skips the schema change and leaves the property needing a rebuild",
	}
	cancelUnnameable := []string{
		"the cancel endpoint is keyed on one collection, property and index type",
		"it can only be waited out",
	}
	cancelUnknown := []string{
		"this build does not know that status",
		"read the task on a node that knows the status",
	}
	concat := func(sets ...[]string) []string {
		var out []string
		for _, set := range sets {
			out = append(out, set...)
		}
		return out
	}

	tests := []struct {
		name    string
		status  distributedtask.TaskStatus
		payload []byte
		want    []string
		notWant []string
	}{
		{"STARTED", distributedtask.TaskStatusStarted, payload, cancelWhileRunning, concat(cancelUnnameable, cancelUnknown)},
		{"PREPARING", distributedtask.TaskStatusPreparing, payload, cancelPastUnits, concat(cancelUnnameable, cancelUnknown)},
		{"SWAPPING", distributedtask.TaskStatusSwapping, payload, cancelPastUnits, concat(cancelUnnameable, cancelUnknown)},
		{"STARTED whole-collection", distributedtask.TaskStatusStarted, wholeCollectionPayload, cancelUnnameable, concat([]string{cancelCall}, cancelUnknown)},
		{"VALIDATING", distributedtask.TaskStatus("VALIDATING"), payload, cancelUnknown, concat([]string{cancelCall}, cancelUnnameable)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &schemaHandlers{
				reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
					db.ReindexNamespace: {{
						Namespace:      db.ReindexNamespace,
						TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
						Status:         tc.status,
						Payload:        tc.payload,
					}},
				}},
			}

			reason := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "name")
			require.NotEmpty(t, reason)
			require.Contains(t, reason, "T_remedy")
			require.Contains(t, reason, string(tc.status))
			for _, want := range tc.want {
				require.Contains(t, reason, want)
			}
			for _, unwanted := range tc.notWant {
				require.NotContains(t, reason, unwanted)
			}
		})
	}
}

// TestPropertyGateMessagesAreByteIdenticalAcrossLayers pins that the
// pre-check and apply gate produce byte-identical refusals across every
// status/payload-shape combination, not just matching substrings.
func TestPropertyGateMessagesAreByteIdenticalAcrossLayers(t *testing.T) {
	statuses := []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		distributedtask.TaskStatus("VALIDATING"),
	}

	marshal := func(t *testing.T, p db.ReindexTaskPayload) []byte {
		t.Helper()
		payload, err := json.Marshal(p)
		require.NoError(t, err)
		return payload
	}

	payloads := []struct {
		name  string
		build func(*testing.T) []byte
	}{
		{
			name: "the property by name",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"name"},
				})
			},
		},
		{
			name: "the property among several",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"other", "name"},
				})
			},
		},
		{
			// Empty Properties is "all properties" on both sides, but each
			// layer spells that out with different code — most likely row
			// to drift.
			name: "no properties means all of them",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
				})
			},
		},
		{
			name: "a payload written by an older binary",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{Collection: "C", Properties: []string{"name"}})
			},
		},
		{
			name:  "a payload that will not parse",
			build: func(t *testing.T) []byte { return []byte("{not json") },
		},
	}

	for _, p := range payloads {
		for _, status := range statuses {
			t.Run(p.name+"/"+string(status), func(t *testing.T) {
				task := &distributedtask.Task{
					Namespace:      db.ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
					Status:         status,
					Payload:        p.build(t),
				}

				h := &schemaHandlers{
					reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
						db.ReindexNamespace: {task},
					}},
				}
				preCheck := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "name")

				applyGate := (&db.ReindexProvider{}).CheckPropertyUpdate(
					"C", "name", []*distributedtask.Task{task})
				require.Error(t, applyGate)

				require.Equal(t, applyGate.Error(), preCheck)
			})
		}
	}
}

// TestTheCancelHelpersReadTheSharedIndexTypeMapping pins that both REST
// helpers delegate to db.ReindexTargetIndexes rather than forking their own
// copy of the mapping. (The mapping's own arms are pinned in the db package.)
func TestTheCancelHelpersReadTheSharedIndexTypeMapping(t *testing.T) {
	migrationTypes := []db.ReindexMigrationType{
		db.ReindexTypeChangeTokenization, // two index types
		db.ReindexTypeEnableRangeable,    // one
		db.ReindexMigrationType("invent-index"),
	}

	for _, mt := range migrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			targets := db.ReindexTargetIndexes(mt)

			indexTypes, known := indexTypesFromMigrationType(mt)
			require.Equal(t, targets, indexTypes)
			require.Equal(t, len(targets) > 0, known)

			for _, indexType := range []string{"searchable", "filterable", "rangeable"} {
				matches, known := migrationTypeTargetsIndex(mt, indexType)
				require.Equal(t, len(targets) > 0, known)
				require.Equal(t, slices.Contains(targets, indexType), matches)
			}
		})
	}
}
