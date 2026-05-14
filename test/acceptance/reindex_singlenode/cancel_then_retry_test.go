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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testCancelThenRetry pins the CANCEL→retry journey: submit an enable-* reindex,
// cancel it while in flight, then re-submit the same migration. The second
// submit MUST finish, flip the schema flag to true, and the index MUST serve
// queries against the populated bucket.
//
// Structurally similar to DELETE→re-enable (testDeleteThenReEnable):
//
//   - DELETE→re-enable: removes the target bucket, leaves
//     .migrations/<dir>/tidied.mig on disk. Without cleanup, the second enable
//     short-circuits on rt.IsTidied()=true, re-flips the schema flag, and
//     reports success with an empty bucket — silent data loss.
//
//   - CANCEL→retry: aborts the iteration loop, leaves
//     .migrations/<dir>/{started.mig, payload.mig, progress.mig} on disk plus
//     the partial __reindex / __ingest sidecar bucket dirs. Without cleanup,
//     the second submit creates a *new* DTM task (so checkReindexConflict
//     does not catch it) but the OnAfterLsmInit path attempts to load buckets
//     whose state is the half-written aftermath of the previous run. Either
//     it loads stale data and the swap promotes a corrupt bucket, or the
//     "expected progress" tracker disagrees with the on-disk objects bucket
//     and the iteration silently no-ops, or one of the sidecar bucket
//     "rename: file exists" errors during RunSwapOnShard. All three failure
//     modes manifest the same way to the customer: the schema flag flips to
//     true but bm25() / equalFilter() / rangeFilter() returns zero hits.
//
// Three sub-tests, one per index type, each on its own collection so they
// can run independently inside the shared container.
func testCancelThenRetry(t *testing.T, restURI string) {
	t.Run("searchable", func(t *testing.T) {
		testCancelThenRetrySearchable(t, restURI)
	})
	t.Run("filterable", func(t *testing.T) {
		testCancelThenRetryFilterable(t, restURI)
	})
	t.Run("rangeable", func(t *testing.T) {
		testCancelThenRetryRangeable(t, restURI)
	})
}

// cancelObjectCount is the corpus size used for each cancel-then-retry
// sub-test. Large enough that enable-* takes hundreds of ms even on fast
// hardware, giving a reliable cancel window. Small enough that the eventual
// reindex finishes within the per-subtest timeout.
const cancelObjectCount = 5000

func testCancelThenRetrySearchable(t *testing.T, restURI string) {
	const class = "CancelRetrySearchable"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	// Bulk-create the corpus. Each object has the same searchable token
	// ("retryfox") plus a unique salt so we can later count post-migration
	// hits.
	for i := 0; i < cancelObjectCount; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"body": fmt.Sprintf("retryfox doc_%d", i)},
		}))
	}

	requestBody := `{"searchable":{"enabled":true,"tokenization":"word"}}`

	// Step 1: submit and cancel.
	cancelInFlightOrSkip(t, restURI, class, "body", "searchable", requestBody)

	// Step 2: re-submit. Crux of the test — without cleanup of started.mig,
	// the partial reindex/ingest sidecars, and the progress tracker, this
	// either fails loudly or worse, "succeeds" with an empty bucket.
	taskID := submitIndexUpdate(t, restURI, class, "body", requestBody)
	awaitReindexFinished(t, restURI, taskID)
	requireSearchableEnabled(t, class, "body")

	hits := bm25Hits(t, class, "retryfox")
	require.Equal(t, cancelObjectCount, hits,
		"post-CANCEL-then-retry: bm25('retryfox') must return all %d docs; got %d. "+
			"If 0, the second submit short-circuited on stale started.mig / progress.mig and "+
			"the bucket is empty — schema reports ready but customer queries are broken (Sev 1)",
		cancelObjectCount, hits)
}

func testCancelThenRetryFilterable(t *testing.T, restURI string) {
	const class = "CancelRetryFilterable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, IndexFilterable: &falseVal, IndexSearchable: &trueVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	// Every object shares the same name token so we can assert exact match
	// count post-migration.
	for i := 0; i < cancelObjectCount; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"name": "shared_name"},
		}))
	}

	requestBody := `{"filterable":{"enabled":true}}`

	cancelInFlightOrSkip(t, restURI, class, "name", "filterable", requestBody)

	taskID := submitIndexUpdate(t, restURI, class, "name", requestBody)
	awaitReindexFinished(t, restURI, taskID)
	requireFilterableEnabled(t, class, "name")

	hits := equalFilterHits(t, class, "name", "shared_name")
	require.Equal(t, cancelObjectCount, hits,
		"post-CANCEL-then-retry: filterable Equal('shared_name') must return %d; got %d. "+
			"If 0, the migration silently no-opped on stale started.mig / partial __reindex sidecars (Sev 1)",
		cancelObjectCount, hits)
}

func testCancelThenRetryRangeable(t *testing.T, restURI string) {
	const class = "CancelRetryRangeable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal, IndexRangeFilters: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	// Half the corpus has score=10, half has score=100. Range LessThan(50)
	// must match exactly half post-migration.
	for i := 0; i < cancelObjectCount; i++ {
		score := 10
		if i%2 == 0 {
			score = 100
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"score": score},
		}))
	}

	requestBody := `{"rangeable":{"enabled":true}}`

	cancelInFlightOrSkip(t, restURI, class, "score", "rangeable", requestBody)

	taskID := submitIndexUpdate(t, restURI, class, "score", requestBody)
	awaitReindexFinished(t, restURI, taskID)
	requireRangeableEnabled(t, class, "score")

	expected := cancelObjectCount / 2
	hits := rangeFilterHits(t, class, "score", 50)
	require.Equal(t, expected, hits,
		"post-CANCEL-then-retry: range LessThan(50) must return %d; got %d. "+
			"If 0, the migration silently no-opped on stale started.mig / partial __reindex sidecars (Sev 1)",
		expected, hits)
}

// cancelInFlightOrSkip submits an index update, polls /indexes until the
// task shows pending/indexing, then issues cancel. If the cancel races
// with task completion (404), the sub-test is logged as fast-completed and
// the caller falls through to the retry submit — which still exercises a
// useful adjacent path (re-submit after a same-shape FINISHED task) even
// though it's not the bug we're after.
//
// Returns true if cancel actually landed, false if the task finished before
// we could cancel.
func cancelInFlightOrSkip(t *testing.T, restURI, class, prop, indexType, requestBody string) bool {
	t.Helper()

	taskID := submitIndexUpdate(t, restURI, class, prop, requestBody)
	t.Logf("submitted first task %s for cancel", taskID)

	// Wait until the task is observable as pending/indexing on /indexes.
	// 30s is generous: with cancelObjectCount=5000 the task does start
	// within a few seconds on any sane hardware.
	require.Eventually(t, func() bool {
		resp := getIndexes(t, restURI, class)
		for _, p := range resp.Properties {
			if p.Name != prop {
				continue
			}
			for _, idx := range p.Indexes {
				if idx.Type == indexType && (idx.Status == "indexing" || idx.Status == "pending") {
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond,
		"task did not appear as indexing/pending before cancel")

	// Issue cancel.
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, class, prop)
	body := buildCancelBody(indexType)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result map[string]string
		require.NoError(t, json.Unmarshal(respBody, &result))
		require.Equal(t, "CANCELLED", result["status"])
		require.Equal(t, taskID, result["taskId"])

		// Wait for the task to reach a terminal CANCELLED/FAILED state in
		// /v1/tasks. Re-submitting too early can race against the DTM
		// scheduler tick that records the cancel — checkReindexConflict
		// would then see the old task as still "STARTED" and reject the
		// fresh submit with 409.
		require.Eventually(t, func() bool {
			tasksResp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
			if err != nil {
				return false
			}
			defer tasksResp.Body.Close()
			b, _ := io.ReadAll(tasksResp.Body)
			var tasks models.DistributedTasks
			if err := json.Unmarshal(b, &tasks); err != nil {
				return false
			}
			for _, task := range tasks["reindex"] {
				if task.ID == taskID {
					return task.Status == "CANCELLED" || task.Status == "FAILED" || task.Status == "FINISHED"
				}
			}
			return false
		}, 60*time.Second, 250*time.Millisecond,
			"first task did not reach a terminal state after cancel")
		t.Logf("first task %s reached terminal state after cancel", taskID)
		return true

	case http.StatusNotFound:
		// Cancel raced with task completion. Wait for the now-finished
		// task to be observable as FINISHED so the retry doesn't 409.
		t.Logf("cancel raced with completion of task %s; waiting for FINISHED", taskID)
		require.Eventually(t, func() bool {
			tasksResp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
			if err != nil {
				return false
			}
			defer tasksResp.Body.Close()
			b, _ := io.ReadAll(tasksResp.Body)
			var tasks models.DistributedTasks
			if err := json.Unmarshal(b, &tasks); err != nil {
				return false
			}
			for _, task := range tasks["reindex"] {
				if task.ID == taskID {
					return task.Status == "FINISHED" || task.Status == "FAILED" || task.Status == "CANCELLED"
				}
			}
			return false
		}, 60*time.Second, 250*time.Millisecond, "race-completed first task not terminal")
		return false

	default:
		t.Fatalf("unexpected status %d cancelling task %s: %s", resp.StatusCode, taskID, string(respBody))
		return false
	}
}

// buildCancelBody returns the cancel JSON body for a given index type. The
// API expects the same shape as the enable body, but with cancel:true.
func buildCancelBody(indexType string) string {
	switch indexType {
	case "searchable":
		return `{"searchable":{"cancel":true}}`
	case "filterable":
		return `{"filterable":{"cancel":true}}`
	case "rangeable":
		return `{"rangeable":{"cancel":true}}`
	}
	return ""
}

// TestSuppress ensures this file compiles in isolation. The actual entry
// point is the suite's subtest registered via
// t.Run("CancelThenRetry", testCancelThenRetry).
func TestSuppress_CancelThenRetry(t *testing.T) {
	assert.NotNil(t, testCancelThenRetry)
}

// TestCancelThenRetry is a standalone runnable entry point for the CANCEL-
// then-retry test. The suite-driven path (TestSingleNode_ReindexSuite +
// t.Run("CancelThenRetry", ...)) is the one CI exercises end-to-end alongside
// every other sub-test on a shared container, but it costs ~7 minutes per
// run because every preceding sub-test must finish first.
//
// This standalone test boots its own container with only this scenario, so
// `go test -run 'CancelThenRetry' ./test/acceptance/reindex_singlenode/`
// produces fast feedback during development and pinpoint bisects.
func TestCancelThenRetry(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 400 {
				lines = lines[len(lines)-400:]
			}
			t.Logf("=== Container logs (last 400 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	testCancelThenRetry(t, restURI)
}
