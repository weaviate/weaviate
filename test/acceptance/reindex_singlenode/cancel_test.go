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

package reindex_singlenode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testCancelReindex exercises the cancel verb on PUT
// /v1/schema/{class}/indexes/{prop}. Two cases:
//
//  1. Cancelling when no task is in flight → 202 with Status: NO_OP
//     (idempotent cancel: caller's (collection, property) was already
//     verified to exist, so "nothing to cancel" is surfaced as a no-op
//     rather than a 404 caller-error).
//  2. Cancelling an in-flight task → 202 with CANCELLED status, and the
//     task transitions to CANCELLED in /v1/tasks. Uses 3000 objects on
//     a from-scratch enable-filterable to give cancel a wide enough
//     window — the test polls /indexes until status is "pending" or
//     "indexing" before issuing the cancel, so the race against a too-
//     fast task is contained.
func testCancelReindex(t *testing.T, restURI string) {
	const className = "CancelTest"
	trueVal := true
	falseVal := false

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: &trueVal, IndexSearchable: &trueVal},
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// Bulk-create enough objects that enable-filterable on `score` takes
	// at least a few hundred ms — gives us a window to cancel reliably.
	const n = 3000
	for i := 0; i < n; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"name":  fmt.Sprintf("name_%d", i),
				"score": i,
			},
		}))
	}

	t.Run("CancelWhenNoTaskInFlight", func(t *testing.T) {
		// score has no in-flight reindex task; cancel is idempotent and
		// returns 202 with Status: NO_OP rather than 404. The body has
		// no TaskID because there is no task that was cancelled.
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, "score")
		req, err := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"filterable":{"cancel":true}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusAccepted, resp.StatusCode,
			"cancel with no in-flight task should 202 NO_OP, got: %s", string(body))
		var result models.IndexUpdateResponse
		require.NoError(t, json.Unmarshal(body, &result),
			"cancel-no-task response body should decode as IndexUpdateResponse: %s", string(body))
		require.Equal(t, "NO_OP", result.Status,
			"cancel-no-task should report Status: NO_OP, got body: %s", string(body))
		require.Empty(t, result.TaskID,
			"cancel-no-task should not name a TaskID, got body: %s", string(body))
	})

	t.Run("CancelInFlightTask", func(t *testing.T) {
		// Submit enable-filterable on score and wait until /indexes shows
		// it pending/indexing, then cancel.
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score", `{"filterable":{"enabled":true}}`)
		t.Logf("submitted task %s", taskID)

		require.Eventually(t, func() bool {
			resp := reindexhelpers.GetIndexes(t, restURI, className)
			for _, prop := range resp.Properties {
				if prop.Name != "score" {
					continue
				}
				for _, idx := range prop.Indexes {
					if idx.Type == "filterable" && (idx.Status == "indexing" || idx.Status == "pending") {
						return true
					}
				}
			}
			return false
		}, 30*time.Second, 100*time.Millisecond, "task did not appear as indexing/pending before cancel")

		// Issue the cancel.
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, "score")
		req, err := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"filterable":{"cancel":true}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Two acceptable outcomes, both at 202 (the cancel verb is
		// idempotent: a finished task is the same observable end-state
		// as a cancelled one):
		// - Status: CANCELLED + TaskID: cancel won the race.
		// - Status: NO_OP: task already terminal (FINISHED, FAILED, or
		//   CANCELLED) before our cancel landed; no STARTED task matched.
		// Both prove the contract; we only fail on unexpected codes.
		require.Equal(t, http.StatusAccepted, resp.StatusCode,
			"cancel must return 202; got %d body: %s", resp.StatusCode, string(body))
		var result models.IndexUpdateResponse
		require.NoError(t, json.Unmarshal(body, &result),
			"cancel response body should decode as IndexUpdateResponse: %s", string(body))
		switch result.Status {
		case "CANCELLED":
			require.Equal(t, taskID, result.TaskID,
				"cancel CANCELLED should name the cancelled task ID; body: %s", string(body))
			t.Logf("cancel returned 202 with status CANCELLED")

			// The task must reach CANCELLED status in /v1/tasks.
			require.Eventually(t, func() bool {
				resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
				if err != nil {
					return false
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				var tasks models.DistributedTasks
				if err := json.Unmarshal(body, &tasks); err != nil {
					return false
				}
				for _, task := range tasks["reindex"] {
					if task.ID == taskID {
						return task.Status == "CANCELLED"
					}
				}
				return false
			}, 30*time.Second, 200*time.Millisecond,
				"task should reach CANCELLED status")
		case "NO_OP":
			t.Logf("cancel raced with task completion; no STARTED task to cancel (acceptable)")
		default:
			t.Fatalf("unexpected cancel Status %q (expected CANCELLED or NO_OP); body: %s", result.Status, string(body))
		}
	})
}
