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
//  1. Cancelling when no task is in flight → 404 (validates the helper
//     correctly distinguishes "nothing to cancel" from a real failure).
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
		// score has no in-flight reindex task; cancel must 404.
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, "score")
		req, err := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"filterable":{"cancel":true}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"cancel with no in-flight task should 404, got: %s", string(body))
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

		// Two acceptable outcomes:
		// - 202 CANCELLED: cancel won the race
		// - 404: task already finished before our cancel landed
		// Both prove the contract; we only fail on unexpected codes.
		switch resp.StatusCode {
		case http.StatusAccepted:
			var result map[string]string
			require.NoError(t, json.Unmarshal(body, &result))
			require.Equal(t, "CANCELLED", result["status"])
			require.Equal(t, taskID, result["taskId"])
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
		case http.StatusNotFound:
			t.Logf("cancel raced with task completion; task finished first (acceptable)")
		default:
			t.Fatalf("unexpected status %d cancelling task: %s", resp.StatusCode, string(body))
		}
	})
}
