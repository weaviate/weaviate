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
// remedy: cancel only while the task is STARTED, no promise that the wait ends
// once it is past that point, and no claim either way for a status this build
// does not know.
func TestPropertyMutationPreCheckCarriesTheSameRemedyAsTheApplyGate(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"name"},
	})
	require.NoError(t, err)

	cancelWorks := []string{
		`cancel it via PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`,
		"or wait for it to finish",
	}
	cancelRefused := []string{
		"past the point where cancel works",
		"can only be waited out",
		"wedges it here for good",
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
		status  distributedtask.TaskStatus
		want    []string
		notWant []string
	}{
		{distributedtask.TaskStatusStarted, cancelWorks, concat(cancelRefused, cancelUnknown)},
		{distributedtask.TaskStatusPreparing, cancelRefused, concat(cancelWorks, cancelUnknown)},
		{distributedtask.TaskStatusSwapping, cancelRefused, concat(cancelWorks, cancelUnknown)},
		{distributedtask.TaskStatus("VALIDATING"), cancelUnknown, concat(cancelWorks, cancelRefused)},
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
			for _, want := range tc.want {
				require.Contains(t, reason, want)
			}
			for _, unwanted := range tc.notWant {
				require.NotContains(t, reason, unwanted)
			}
		})
	}
}

// The two property gates exist as one helper plus two format strings that
// must stay word-for-word equal — the pre-check and the apply path answer the
// same question, and an operator who retries after the pre-check refusal must
// not get a second, differently-worded answer. Substring assertions elsewhere
// in this file catch drift in the remedy; this one catches drift anywhere in
// the sentence, including the prefix around it.
func TestPropertyGateMessagesAreByteIdenticalAcrossLayers(t *testing.T) {
	statuses := []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		distributedtask.TaskStatus("VALIDATING"),
	}

	payloadStruct := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"name"},
	}
	payload, err := json.Marshal(payloadStruct)
	require.NoError(t, err)

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			task := &distributedtask.Task{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
				Status:         status,
				Payload:        payload,
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

// The cancel URL the gate messages print is only correct while the index key
// it names is the one the cancel endpoint matches on. That mapping lives here
// in migrationTypeTargetsIndex and is mirrored in db.ReindexTargetIndexes so
// the db package can render the URL; this pins them together, since a
// divergence would print a URL that answers 202 NO_OP.
func TestReindexTargetIndexesAgreesWithTheCancelMatcher(t *testing.T) {
	migrationTypes := []db.ReindexMigrationType{
		db.ReindexTypeChangeAlgorithm,
		db.ReindexTypeRebuildSearchable,
		db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableRangeable,
		db.ReindexTypeRepairRangeable,
		db.ReindexTypeEnableFilterable,
		db.ReindexTypeEnableSearchable,
		db.ReindexTypeChangeTokenization,
		db.ReindexTypeChangeTokenizationFilterable,
		db.ReindexMigrationType("invent-index"),
	}

	for _, mt := range migrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			targets := db.ReindexTargetIndexes(mt)
			for _, indexType := range []string{"searchable", "filterable", "rangeable"} {
				matches, known := migrationTypeTargetsIndex(mt, indexType)
				require.Equal(t, len(targets) > 0, known)
				require.Equal(t, slices.Contains(targets, indexType), matches)
			}
		})
	}
}
