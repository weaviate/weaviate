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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestSingleNode_InFlightConvergentUpsertMustNotReportNoOp pins that a PUT
// converging on an in-flight task must report that task's ID, not NO_OP.
func TestSingleNode_InFlightConvergentUpsertMustNotReportNoOp(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
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
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	const className = "InFlightNoOpContractTest"
	falseVal := false

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &falseVal},
			{Name: "amount", DataType: []string{"int"}, IndexRangeFilters: &falseVal},
			{Name: "body", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// Enough objects that each migration outlives the second PUT — a fixture
	// that drains too fast turns that PUT into a legitimate NO_OP.
	const objectCount = 15000
	const batchSize = 500
	for start := 0; start < objectCount; start += batchSize {
		batch := make([]*models.Object, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			batch = append(batch, &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"score":  i,
					"amount": i,
					"body":   fmt.Sprintf("document number %d", i),
				},
			})
		}
		helper.CreateObjectsBatch(t, batch)
	}

	cases := []struct {
		name      string
		property  string
		indexType string
		body      string
		flag      func(*models.Property) bool
	}{
		{
			name: "filterable", property: "score", indexType: "filterable", body: `{}`,
			flag: func(p *models.Property) bool { return p.IndexFilterable != nil && *p.IndexFilterable },
		},
		{
			name: "rangeFilters", property: "amount", indexType: "rangeFilters", body: `{}`,
			flag: func(p *models.Property) bool { return p.IndexRangeFilters != nil && *p.IndexRangeFilters },
		},
		{
			name: "searchable", property: "body", indexType: "searchable", body: `{"tokenization":"word"}`,
			flag: func(p *models.Property) bool { return p.IndexSearchable != nil && *p.IndexSearchable },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// An assertion failure leaves the migration running, and both the
			// next subtest and the final class DELETE need it gone.
			defer reindexhelpers.CancelIndexRaw(t, restURI, className, tc.property, tc.indexType)

			require.False(t, indexFlagOf(t, restURI, className, tc.property, tc.flag),
				"precondition: %s.%s must start without a %s index", className, tc.property, tc.indexType)

			taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, tc.property, tc.indexType, tc.body)
			t.Logf("submitted %s task: %s", tc.indexType, taskID)

			// Fails the test (rather than silently degrading it) if the
			// migration drained before it was ever observed live.
			reindexhelpers.AwaitReindexLive(t, restURI, taskID)

			// Second, identical PUT — issued while the first task is in flight.
			resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, tc.property, tc.indexType, tc.body)
			flagAfter := indexFlagOf(t, restURI, className, tc.property, tc.flag)
			statusAfter, ok := reindexTaskStatusOf(t, restURI, taskID)

			require.True(t, ok, "task %s disappeared from /v1/tasks", taskID)
			require.Containsf(t, []string{"STARTED", "PREPARING", "SWAPPING"}, statusAfter,
				"fixture too small: task %s was already %q when the second PUT landed, so the "+
					"in-flight window was never exercised — increase objectCount", taskID, statusAfter)
			require.Falsef(t, flagAfter,
				"task %s is still %q but the %s flag already flipped to true — the in-flight "+
					"precondition of this case no longer holds", taskID, statusAfter, tc.indexType)

			var body models.IndexUpdateResponse
			require.NoError(t, json.Unmarshal([]byte(resp.Body), &body))
			require.NotEqualf(t, "NO_OP", body.Status,
				"convergent PUT during in-flight task %s (status %q) answered %d %s — NO_OP means "+
					"\"desired configuration already in place; no task submitted\", but the %s flag "+
					"is still false and the index does not exist yet, and the caller is left with no "+
					"taskId to poll",
				taskID, statusAfter, resp.StatusCode, resp.Body, tc.indexType)
			require.Equalf(t, http.StatusAccepted, resp.StatusCode,
				"a convergent PUT on an in-flight task must be 202 so the caller gets a handle, got %d %s",
				resp.StatusCode, resp.Body)
			require.Equalf(t, taskID, body.TaskID,
				"an accepted convergent PUT must name the in-flight task it joined, got %s", resp.Body)

			reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(240*time.Second))
			require.Eventually(t, func() bool {
				return indexFlagOf(t, restURI, className, tc.property, tc.flag)
			}, 60*time.Second, 200*time.Millisecond,
				"the %s flag must be true once task %s is FINISHED", tc.indexType, taskID)
		})
	}
}

// indexFlagOf reads a node-local index flag of a property via `flag`.
func indexFlagOf(t *testing.T, restURI, className, propName string, flag func(*models.Property) bool) bool {
	t.Helper()
	class, ok := reindexhelpers.FetchClass(restURI, className, true)
	require.True(t, ok, "reading local schema of %s", className)
	for _, prop := range class.Properties {
		if prop.Name == propName {
			return flag(prop)
		}
	}
	require.FailNow(t, fmt.Sprintf("property %q not found on %q", propName, className))
	return false
}

// reindexTaskStatusOf returns the current status of a reindex task.
func reindexTaskStatusOf(t *testing.T, restURI, taskID string) (string, bool) {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	require.True(t, ok, "reading /v1/tasks")
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task.Status, true
		}
	}
	return "", false
}
