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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_CancelClearsAcrossReplicas asserts that cancel-cleanup
// drains in-flight reindex trackers on every replica, that a subsequent
// backup succeeds, and that DELETE class removes the class dir on every
// node. Requires ≥3 nodes and cancel within ~1s of STARTED to exercise
// the limiter.Acquire-vs-ctx-cancel path.
func TestMultiNode_CancelClearsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	// S3/MinIO required: filesystem backend is per-node and refused by
	// the API for ≥2 nodes. REINDEX_CONCURRENCY=1 keeps the race window
	// wide enough to reproduce reliably on fast hardware.
	const backupBucket = "cancel-clears-bucket"
	compose, err := docker.New().
		WithWeaviateEnv("RUNTIME_REINDEX_ENABLED", "true").
		With3NodeCluster().
		WithBackendS3(backupBucket, "us-east-1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false").
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("REINDEX_CONCURRENCY", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className = "CancelClearsAcrossReplicas"
		propName  = "body"
		// 200k keeps the change-tokenization iteration alive long enough
		// that the cancel HTTP call lands mid-flight. Smaller values let
		// the migration finish first, and the cancel is refused with 409.
		dataset       = 200_000
		cancelTimeout = 30 * time.Second
		// Bounds on the cancel-race retry below. Both are load-bearing:
		// an attempt count alone would let a slow runner add minutes to a
		// CI shard budgeted at roughly ten of them.
		maxCancelAttempts = 3
		cancelRetryBudget = 4 * time.Minute
		// How long a submitted migration gets to reach STARTED with a unit
		// actually working before the attempt is written off.
		midFlightTimeout = 60 * time.Second
		// A refused cancel means the units are already done, so the task
		// owes a terminal status before the next attempt may submit.
		settleTimeout = 120 * time.Second
	)
	classDirLower := strings.ToLower(className)

	uri := restURIOf(compose, 1)
	trueVal := true
	createCollection(t, compose, uri, className, 3, 3, []*models.Property{
		{
			Name:            propName,
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})

	paths := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	batchImportMultiProp(t, uri, className, dataset, func(i int) map[string]interface{} {
		return map[string]interface{}{propName: paths[i%len(paths)]}
	})

	// Shard names do not depend on the migration, so read them before it
	// starts. Everything between STARTED and the cancel eats into the
	// window the cancel has to land in.
	allShards := collectShardNamesForClass(t, uri, className)
	require.GreaterOrEqual(t, len(allShards), 3,
		"sanity: expected ≥3 shards on a 3-shard class; got %v", allShards)

	// A cancel is accepted only while the task reads STARTED, and that is
	// decided on the leader inside a RAFT apply, one round trip after the
	// request leaves. No check the test performs first can hold that
	// state, so losing the race is a legal outcome of a correct server.
	// Retry instead: a documented refusal costs an attempt, not the build.
	nextTokenization := map[string]string{"word": "field", "field": "word"}
	current := "word"
	var (
		won            bool
		wonTaskID      string
		wonBaseline    []string
		attemptLog     []string
		stoppedOn      string
		winningAttempt int
	)
	budget := time.Now().Add(cancelRetryBudget)
	for attempt := 1; attempt <= maxCancelAttempts; attempt++ {
		if !time.Now().Before(budget) {
			stoppedOn = fmt.Sprintf("the %s wall-clock budget", cancelRetryBudget)
			break
		}

		// Tracker dirs of a migration that ran to completion on an earlier
		// attempt stay on disk until the next restart, and cancel-cleanup
		// preserves them on purpose. Read them here, outside the race
		// window, so the drain assertion can tell them apart from the dirs
		// the cancelled run has to remove. Empty on the first attempt, so
		// a run that wins straight away asserts exactly what it did before.
		baseline := scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName)

		// Tokenization-changing migration creates both searchable and
		// filterable trackers per (shard, replica).
		target := nextTokenization[current]
		submittedAt := time.Now()
		taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, propName, "searchable",
			fmt.Sprintf(`{"tokenization":%q}`, target))
		t.Logf("attempt %d: submitted change-tokenization task to %q: %s", attempt, target, taskID)

		// One /v1/tasks read answers both halves of the gate — the task is
		// STARTED and a unit is actually working — and the cancel is the
		// next statement. A tracker dir on disk answers neither half: it
		// outlives the migration that created it, so the migration may
		// well be over by the time the scan comes back.
		midFlight, observed := awaitTaskMidFlight(t, uri, taskID, midFlightTimeout)
		if midFlight {
			midFlightAt := time.Now()
			cancelled, arm := cancelReindexProperty(t, uri, className, propName, "searchable", taskID)
			if cancelled {
				won, winningAttempt, wonTaskID, wonBaseline = true, attempt, taskID, baseline
				attemptLog = append(attemptLog, fmt.Sprintf(
					"attempt %d: %s, mid-flight→cancel %s, %d pre-existing tracker dir(s)",
					attempt, arm, time.Since(midFlightAt), len(baseline)))
				break
			}
			observed = arm
		} else {
			observed = fmt.Sprintf("never reached mid-flight, last seen %s", observed)
		}

		// A lost attempt means the migration ran on to completion, so it
		// owes a terminal status: re-submitting under a non-terminal task
		// is refused with a 409 by the conflict check.
		terminal := awaitTaskTerminal(t, uri, taskID, settleTimeout)
		require.Equalf(t, "FINISHED", terminal,
			"attempt %d lost the cancel race (%s) and then ended %s. Only a completed migration is a race loss",
			attempt, observed, terminal)
		attemptLog = append(attemptLog, fmt.Sprintf(
			"attempt %d: lost — %s, submit→FINISHED %s, %d pre-existing tracker dir(s)",
			attempt, observed, time.Since(submittedAt), len(baseline)))

		// The migration completed, so the property now carries the target
		// tokenization and re-submitting it would be a no-op. Wait for the
		// flip to be readable before the next submit names the other one.
		require.Eventuallyf(t, func() bool {
			return tryGetPropertyTokenization(uri, className, propName) == target
		}, cancelTimeout, 100*time.Millisecond,
			"attempt %d finished, so %s.%s should read tokenization %q",
			attempt, className, propName, target)
		current = target
	}
	if stoppedOn == "" {
		stoppedOn = fmt.Sprintf("the %d-attempt limit", maxCancelAttempts)
	}

	// Everything below holds for a plain completion too, so a run where
	// the cancel never landed proves nothing about cancel-cleanup. This
	// is the only multi-node cancel-cleanup journey in the repo, so it
	// fails rather than skips: a skip here reads as green in CI.
	require.Truef(t, won,
		"no attempt landed a cancel; stopped on %s. The assertions below cannot tell a "+
			"cancel-driven cleanup from a completion-driven one, so this run proves nothing.\n  %s",
		stoppedOn, strings.Join(attemptLog, "\n  "))
	t.Logf("cancel landed on attempt %d of at most %d:\n  %s",
		winningAttempt, maxCancelAttempts, strings.Join(attemptLog, "\n  "))

	terminalStatus := awaitTaskTerminal(t, uri, wonTaskID, cancelTimeout)
	t.Logf("cancelled task %s reached %s", wonTaskID, terminalStatus)

	// Every replica on every node must drain the .migrations/*_body_*
	// dirs the cancelled run created, within cancelTimeout. Dirs that
	// were already there belong to an earlier attempt's completed
	// migration, which production keeps by design.
	preExisting := make(map[string]struct{}, len(wonBaseline))
	for _, dir := range wonBaseline {
		preExisting[dir] = struct{}{}
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var survivors []string
		for _, dir := range scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName) {
			if _, older := preExisting[dir]; !older {
				survivors = append(survivors, dir)
			}
		}
		assert.Emptyf(c, survivors,
			"cancel-cleanup left .migrations/*_%s_* dirs on %d replica slots:\n  %s",
			propName, len(survivors), strings.Join(survivors, "\n  "))
	}, cancelTimeout, 50*time.Millisecond)

	// Backup must succeed. canCommit refuses while any in-flight tracker
	// is present, so a green backup here proves the inflight registration
	// was cleared on every node.
	backupID := "cancel-clears-backup"
	require.NoError(t, createS3Backup(t, uri, className, backupID, backupBucket), "backup must succeed after cancel-cleanup drains")

	// DELETE class must succeed (MutationGuard treats CANCELLED tasks as
	// not-in-flight) and the on-disk class dir must disappear on every node.
	require.NoError(t, deleteClassExpectOK(t, uri, className), "DELETE class must succeed post-cancel")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		survivors := scanClassDirAllNodes(ctx, t, compose, classDirLower)
		assert.Emptyf(c, survivors,
			"DELETE class left /data/%s on %d node(s): %v",
			classDirLower, len(survivors), survivors)
	}, cancelTimeout, 50*time.Millisecond)
}

// awaitTaskMidFlight polls /v1/tasks until the named task is STARTED
// with at least one unit reporting IN_PROGRESS, and returns whether it
// got there plus the status that ended the wait.
//
// Both halves matter: STARTED alone is what the cancel needs, and a unit
// IN_PROGRESS is what proves the migration is still running rather than
// about to leave the state the cancel needs. Failing softly is the point
// — inside the retry loop a migration that outran the poll is a lost
// attempt, not a broken test.
//
// A unit's status, not a numeric progress floor: the DTM progress
// recorder throttles on a 3-second window, so a fast unit can emit one
// progress=0 update and nothing more before it completes.
func awaitTaskMidFlight(t *testing.T, restURI, taskID string, timeout time.Duration) (bool, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var tasks models.DistributedTasks
		if httpGetJSON(fmt.Sprintf("http://%s/v1/tasks", restURI), &tasks) {
			for _, task := range tasks["reindex"] {
				if task.ID != taskID {
					continue
				}
				if task.Status == "STARTED" {
					for _, unit := range task.Units {
						if unit.Status == "IN_PROGRESS" {
							return true, task.Status
						}
					}
					break
				}
				return false, task.Status
			}
		}
		if !time.Now().Before(deadline) {
			return false, fmt.Sprintf("nothing within %s", timeout)
		}
		<-ticker.C
	}
}

// awaitTaskTerminal polls /v1/tasks until the named task reaches a
// terminal status and returns it. Exists so the total migration duration
// can be logged next to the STARTED→cancel latency: the gap between
// those two is what decides whether this test still exercises the
// cancel, and it has to be readable in CI rather than inferred.
func awaitTaskTerminal(t *testing.T, restURI, taskID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var tasks models.DistributedTasks
			if err := json.Unmarshal(body, &tasks); err == nil {
				for _, task := range tasks["reindex"] {
					if task.ID != taskID {
						continue
					}
					switch task.Status {
					case "FINISHED", "FAILED", "CANCELLED":
						return task.Status
					}
				}
			}
		}
		require.Truef(t, time.Now().Before(deadline),
			"task %s should reach a terminal status within %s", taskID, timeout)
		<-ticker.C
	}
}

// cancelReindexProperty POSTs .../index/{indexType}/cancel and checks the
// cancel contract for taskID. Reports whether the cancel actually landed,
// and which of the three legal arms answered: all three are legal outcomes
// of the race, but only one of them leaves the caller's post-cancel
// assertions cancel-driven rather than completion-driven, and a caller that
// fails on the other two has to name the one it got.
func cancelReindexProperty(t *testing.T, restURI, className, propName, indexType, taskID string) (bool, string) {
	t.Helper()
	if indexType == "rangeable" {
		indexType = "rangeFilters"
	}
	cancelled := false
	arm := ""
	resp := reindexhelpers.CancelIndexRaw(t, restURI, className, propName, indexType)

	// The task can outrun the STARTED poll and the pre-cancel disk scan, so
	// its phase decides the code: 202 CANCELLED (still STARTED), 409 (every
	// unit finished, cluster-wide swap under way), 202 NO_OP (terminal).
	// Under 409 and NO_OP the caller's drain assertions are completion-
	// driven rather than cancel-driven, so log which one we got.
	switch resp.StatusCode {
	case http.StatusAccepted:
		var result models.IndexUpdateResponse
		require.NoErrorf(t, json.Unmarshal([]byte(resp.Body), &result),
			"cancel response should decode as IndexUpdateResponse: %s", resp.Body)
		switch result.Status {
		case "CANCELLED":
			require.Equalf(t, taskID, result.TaskID,
				"cancel CANCELLED must name the cancelled task; body: %s", resp.Body)
			cancelled = true
			arm = "202 CANCELLED"
		case "NO_OP":
			arm = "202 NO_OP (task was already terminal)"
		default:
			t.Fatalf("unexpected cancel Status %q (expected CANCELLED or NO_OP); body: %s",
				result.Status, resp.Body)
		}
	case http.StatusConflict:
		require.Containsf(t, resp.Body, taskID,
			"cancel 409 must name the task it refuses to cancel; body: %s", resp.Body)
		arm = "409 (task is past its units, cluster-wide swap under way)"
	default:
		t.Fatalf("unexpected cancel status %d (expected 202 or 409): %s", resp.StatusCode, resp.Body)
	}
	return cancelled, arm
}

// collectShardNamesForClass returns every distinct shard name owned by
// the given class across all nodes.
func collectShardNamesForClass(t *testing.T, restURI, className string) []string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var nodesResp struct {
		Nodes []struct {
			Shards []struct {
				Class string `json:"class"`
				Name  string `json:"name"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &nodesResp))

	seen := map[string]bool{}
	for _, node := range nodesResp.Nodes {
		for _, sh := range node.Shards {
			if sh.Class == className {
				seen[sh.Name] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}

// scanBodyMigrationsAllReplicas returns a "<nodeIdx>:<shard>/<dir>"
// identifier per .migrations/*_<propName>_* dir still on disk. Empty
// slice means every replica is clean.
func scanBodyMigrationsAllReplicas(
	ctx context.Context, t *testing.T, compose *docker.DockerCompose,
	classDirLower string, shards []string, propName string,
) []string {
	t.Helper()
	var survivors []string
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		survivors = append(survivors,
			scanBodyMigrationsOnNode(ctx, t, compose, classDirLower, shards, propName, nodeIdx)...)
	}
	return survivors
}

// scanBodyMigrationsOnNode is the single-node half of
// scanBodyMigrationsAllReplicas. One entry per directory, so callers can
// subtract the set a previous migration left behind.
func scanBodyMigrationsOnNode(
	ctx context.Context, t *testing.T, compose *docker.DockerCompose,
	classDirLower string, shards []string, propName string, nodeIdx int,
) []string {
	t.Helper()
	var survivors []string
	container := compose.GetWeaviateNode(nodeIdx).Container()
	for _, shard := range shards {
		migsPath := fmt.Sprintf("/data/%s/%s/lsm/.migrations", classDirLower, shard)
		cmd := []string{
			"sh", "-c",
			fmt.Sprintf(`ls -1 %s 2>/dev/null | grep -E '_%s($|_)' | head -50`,
				migsPath, propName),
		}
		// Demultiplexed, because the raw exec stream interleaves Docker's
		// per-frame headers with the output and their bytes would end up
		// inside the first directory name of every reply.
		code, reader, err := container.Exec(ctx, cmd, tcexec.Multiplexed())
		require.NoError(t, err, "exec on node %d for shard %s", nodeIdx, shard)
		out := new(strings.Builder)
		if reader != nil {
			_, _ = io.Copy(out, reader)
		}
		if code != 0 {
			continue
		}
		for _, dir := range strings.Fields(out.String()) {
			survivors = append(survivors, fmt.Sprintf("node%d:%s/%s", nodeIdx, shard, dir))
		}
	}
	return survivors
}

// scanClassDirAllNodes returns the 1-based node indexes where
// /data/<classDirLower> still exists. Empty slice means every node
// is clean.
func scanClassDirAllNodes(
	ctx context.Context, t *testing.T, compose *docker.DockerCompose, classDirLower string,
) []int {
	t.Helper()
	var survivors []int
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		container := compose.GetWeaviateNode(nodeIdx).Container()
		code, _, err := container.Exec(ctx, []string{"test", "-d", fmt.Sprintf("/data/%s", classDirLower)})
		require.NoError(t, err, "exec on node %d", nodeIdx)
		if code == 0 {
			survivors = append(survivors, nodeIdx)
		}
	}
	return survivors
}

// createS3Backup posts to /v1/backups/s3 and waits for SUCCESS: an in-flight
// backup sends Index.drop down the rename-aside keepFiles path, defeating the
// post-delete "class dir removed" assertion. Budget sized for a starved VM.
func createS3Backup(t *testing.T, restURI, className, backupID, bucket string) error {
	t.Helper()
	body := map[string]interface{}{
		"id":      backupID,
		"include": []string{className},
		"config":  map[string]interface{}{"Bucket": bucket},
	}
	reqBody, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/backups/s3", restURI),
		"application/json",
		bytes.NewReader(reqBody),
	)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backup create returned %d: %s", resp.StatusCode, string(respBody))
	}

	deadline := time.Now().Add(300 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		r, err := http.Get(fmt.Sprintf("http://%s/v1/backups/s3/%s", restURI, backupID))
		if err != nil {
			return fmt.Errorf("backup status: %w", err)
		}
		statusBody, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		var status struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		_ = json.Unmarshal(statusBody, &status)
		switch status.Status {
		case "SUCCESS":
			return nil
		case "FAILED":
			return fmt.Errorf("backup FAILED: %s", status.Error)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("backup did not reach SUCCESS/FAILED in 300s; last status: %s", status.Status)
		}
		<-ticker.C
	}
}

// deleteClassExpectOK sends DELETE /v1/schema/<class> and requires 200.
func deleteClassExpectOK(t *testing.T, restURI, className string) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("http://%s/v1/schema/%s", restURI, className), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE class returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
