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

package reindex_multinode

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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_EnableRangeable_FilterableFalseCompletes pins
// weaviate/0-weaviate-issues#325: on RF>1, enable-rangeable on an
// IndexFilterable=false property failed because each replica's
// OnMigrationComplete RAFted its own IndexRangeFilters flip, and each
// apply's cleanStaleMigrationDirs os.RemoveAll'd a still-in-flight
// sibling replica's migration dir. Deferring the flip to the
// all-units-terminal OnTaskCompleted gate
// (needsClusterWideFlipAtCompletion) eliminates the storm; this test
// also scans node logs for the two failure signatures directly, so a
// regression via a different code path is still caught.
func TestMultiNode_EnableRangeable_FilterableFalseCompletes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className = "GH325Regression"
		// Larger than the issue's own repro count: the race window
		// widens with data volume, so this stays a reliable regression
		// gate rather than a flaky coin flip.
		totalObjects = 30_000
	)
	falseVal := false
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:              "score",
			DataType:          []string{"int"},
			IndexFilterable:   &falseVal,
			IndexRangeFilters: &falseVal,
		},
	})
	defer deleteCollection(t, restURIOf(compose, 1), className)

	batchImportNumericGH325Regression(t, restURIOf(compose, 1), className, totalObjects, func(i int) int { return i })

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("GH325 regression: submitted enable-rangeable task %s", taskID)

	// Deliberately not using reindexhelpers.AwaitReindexFinished here: we
	// want the log-signature scan to run regardless of terminal status, so
	// a FAILED run gets full forensic output (which signature fired, on
	// which node) instead of a bare "should reach FINISHED" timeout.
	status, taskErr := pollReindexToTerminalGH325Regression(t, restURIOf(compose, 1), taskID, 180*time.Second)

	enoentHit, waitForUpdateHit := scanClusterLogsForGH325SignaturesRegression(ctx, t, compose)
	if enoentHit || waitForUpdateHit {
		t.Errorf("GH #325 regression: signature(s) present -- enoent=%v waitforupdate=%v "+
			"(see per-line log output above for the exact node/message)", enoentHit, waitForUpdateHit)
	}

	require.Equalf(t, "FINISHED", status,
		"GH #325 regression: enable-rangeable on IndexFilterable=false must complete "+
			"(RF3/3-shard, no concurrent writes); got status=%s err=%q", status, taskErr)

	// The whole point of the fix: the flip must have actually landed on
	// every replica, not just "the task didn't error."
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		cls := getClassFromNode(t, restURIOf(compose, nodeIdx), className)
		var flipped bool
		for _, prop := range cls.Properties {
			if prop.Name == "score" && prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				flipped = true
				break
			}
		}
		require.True(t, flipped, "node %d: IndexRangeFilters should be true after migration", nodeIdx)
	}
}

// pollReindexToTerminalGH325Regression polls until the task reaches a
// terminal state, returning the status and any error. A local poll
// (rather than reindexhelpers.AwaitReindexFinished) so the caller can
// run the log-signature scan before asserting.
func pollReindexToTerminalGH325Regression(t *testing.T, restURI, taskID string, timeout time.Duration) (status, errStr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var tasks models.DistributedTasks
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/tasks", restURI), &tasks) {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			if task.Status == "FAILED" || task.Status == "FINISHED" {
				status = task.Status
				errStr = task.Error
				return true
			}
		}
		return false
	}, timeout, 500*time.Millisecond, "reindex task %s should reach a terminal state within %s", taskID, timeout)
	return status, errStr
}

// scanClusterLogsForGH325SignaturesRegression fetches raw (unfiltered) logs
// from all 3 nodes and checks for the two GH#325 failure signatures,
// logging every matching line for forensic visibility on failure.
func scanClusterLogsForGH325SignaturesRegression(ctx context.Context, t *testing.T, compose *docker.DockerCompose) (enoentHit, waitForUpdateHit bool) {
	t.Helper()
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		if node == nil {
			continue
		}
		reader, err := node.Container().Logs(ctx)
		if err != nil {
			t.Logf("GH325 regression: failed to get logs for node %d: %v", i, err)
			continue
		}
		raw, _ := io.ReadAll(reader)
		reader.Close()
		logs := string(raw)

		if strings.Contains(logs, "deadline exceeded for waiting for update") {
			waitForUpdateHit = true
			for _, line := range strings.Split(logs, "\n") {
				if strings.Contains(line, "deadline exceeded for waiting for update") {
					t.Logf("GH325 regression: node %d WaitForUpdate signature: %s", i, line)
				}
			}
		}
		for _, sentinel := range []string{"reindexed.mig", "prepended.mig", "progress.mig"} {
			if !strings.Contains(logs, sentinel) || !strings.Contains(logs, "no such file or directory") {
				continue
			}
			for _, line := range strings.Split(logs, "\n") {
				if strings.Contains(line, sentinel) && strings.Contains(line, "no such file or directory") {
					enoentHit = true
					t.Logf("GH325 regression: node %d ENOENT signature: %s", i, line)
				}
			}
		}
	}
	return enoentHit, waitForUpdateHit
}

// batchImportNumericGH325Regression posts `total` objects in 200-object
// batches, consistency_level=ALL so the migration starts from a
// fully-replicated baseline.
func batchImportNumericGH325Regression(
	t *testing.T, restURI, className string, total int, scoreFor func(i int) int,
) {
	t.Helper()
	const batchSize = 200
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		objects := make([]map[string]interface{}, 0, end-start)
		for i := start; i < end; i++ {
			objects = append(objects, map[string]interface{}{
				"class": className,
				"id":    uuid.New().String(),
				"properties": map[string]interface{}{
					"name":  fmt.Sprintf("item-%d", i),
					"score": scoreFor(i),
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects})
		require.NoError(t, err)
		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"batch %d-%d failed: %s", start, end, string(respBody))

		var batchResp []struct {
			Result struct {
				Errors *struct {
					Error []struct {
						Message string `json:"message"`
					} `json:"error"`
				} `json:"errors,omitempty"`
				Status string `json:"status"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(respBody, &batchResp))
		for i, br := range batchResp {
			if br.Result.Errors != nil && len(br.Result.Errors.Error) > 0 {
				t.Fatalf("batch %d-%d object %d errored: %s",
					start, end, i, br.Result.Errors.Error[0].Message)
			}
		}
	}
}
