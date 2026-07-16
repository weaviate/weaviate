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
// https://github.com/weaviate/0-weaviate-issues/issues/325: enable-rangeable on
// a property with IndexFilterable=false previously failed on any RF>1
// cluster because the per-shard OnMigrationComplete hook RAFted the
// IndexRangeFilters=true flip once per replica (up to 9 concurrent
// same-property UpdateProperty commands on a 3-shard/RF3 collection). Each
// apply synchronously ran updatePropertyBuckets, which -- because
// IndexFilterable=false -- called cleanStaleMigrationDirs(prop,
// "filterable"); migrationDirsForPropertyIndex("filterable") includes the
// filterable_to_rangeable_<prop> prefix, so a still-in-flight replica's
// migration dir was os.RemoveAll'd by the first-finishing replica's flip.
// The DTM retry then wrote its next sentinel into the now-missing dir:
//
//	open .../.migrations/filterable_to_rangeable_<prop>_<gen>/{reindexed,
//	prepended,progress}.mig: no such file or directory
//
// with a companion schema-propagation stall:
//
//	runtime swap: on migration complete: updating property "score":
//	deadline exceeded for waiting for update: version got=0 want=<N>
//
// weaviate/weaviate#12206 (GH weaviate/weaviate#12189) moved the flip from
// per-shard OnMigrationComplete to the all-units-terminal OnTaskCompleted
// gate (needsClusterWideFlipAtCompletion), the same slot semantic
// migrations already use -- eliminating the concurrent-UpdateProperty
// storm this issue's mechanism rides on. This test pins that fix at the
// exact repro shape from the issue (3-shard RF3, IndexFilterable=false,
// no concurrent writes) and additionally scans every node's logs for the
// two failure signatures directly, so a regression that reintroduces the
// storm through a different code path (not just a reverted flip) is also
// caught.
//
// AB verdict (dev, 2026-07-16): reproduced 5/6 (83%) at this exact
// object count on stable/v1.38 @ 56d9926f7afd23074c93a88acfe9fa63808be23b
// (the SHA the issue itself cites), 0/10 clean on the #12206+#11986
// convergence branch @ cc412837a8. See
// Conversations/2026-07/2026-07-16-1927-investigate-0-weaviate-issues-325-a-b-on-pr-12206-does-the-d__ab-brief.md.
func TestMultiNode_EnableRangeable_FilterableFalseCompletes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className = "GH325Regression"
		// 30_000 matches the object count that reliably reproduced the
		// bug pre-fix (5/6 runs); the issue's own 3_000-object repro was
		// only ~57% and is too flaky for a CI regression gate. The issue
		// itself notes the race window widens with data volume -- this
		// is a deliberate over-provision so the test stays a faithful
		// adversary of the fixed code path rather than a coin flip.
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

// pollReindexToTerminalGH325Regression polls /v1/tasks until the named task
// reaches FAILED or FINISHED, returning the terminal status and (if FAILED)
// the task's error string. A local poll rather than
// reindexhelpers.AwaitReindexFinished so the caller can run the log-signature
// scan before asserting, giving a FAILED run full forensic output instead of
// just "should reach FINISHED".
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
// fully-replicated baseline (matches the issue's "no concurrent writes
// needed" repro -- the storm comes purely from the per-replica flip
// fan-out, not from write traffic racing the migration).
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
