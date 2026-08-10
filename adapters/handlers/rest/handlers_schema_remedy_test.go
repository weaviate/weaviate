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

// The REST pre-check answers the same question as the apply-path gate in
// [db.ReindexProvider.CheckPropertyUpdate], one hop earlier, and it is the
// first refusal a caller sees. It must therefore carry the same status-aware
// remedy: cancel wherever the cancel endpoint accepts it, with the cost of
// cancelling past the units spelled out, no URL for a task the endpoint
// cannot be keyed on, and no claim either way for a status this build does
// not know.
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

// The two property gates answer the same question one hop apart, and an
// operator who retries after the pre-check refusal must not get a second,
// differently-worded answer. Substring assertions elsewhere in this file
// catch drift in the remedy; this one catches drift anywhere in the sentence,
// including the prefix around it.
//
// Driven over both dimensions the two layers branch on — the task's status
// and the shape of its payload — because the layers reach their refusals by
// different code: the pre-check matches properties with an inline loop, the
// apply gate with db.ReindexPropsOverlap, and each renders the payload-level
// rejections from its own format string.
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
			// Empty Properties is "all properties" on both sides. The two
			// layers spell that out differently (an explicit case here, a
			// length check inside ReindexPropsOverlap there), so it is the
			// row most likely to drift.
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

// Both helpers in this package answer their question by reading
// db.ReindexTargetIndexes, so that one mapping decides which index key a
// printed cancel URL names, whether a cancel request matches a task, and
// which index types get cleaned off disk. This pins the delegation: a copy
// re-forked into either helper would print a URL that answers 202 NO_OP, or
// clean one index type of a migration that wrote two.
//
// The mapping's own arms are pinned in the db package, next to the mapping.
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
