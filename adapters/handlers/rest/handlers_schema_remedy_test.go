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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// The REST pre-check answers the same question as the apply-path gate in
// [db.ReindexProvider.CheckPropertyUpdate], one hop earlier, and it is the
// first refusal a caller sees. It must therefore carry the same status-aware
// remedy: cancel only while the task is STARTED, and no promise that the wait
// ends once it is past that point.
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
		{distributedtask.TaskStatusSwapping, cancelRefused, cancelWorks},
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
