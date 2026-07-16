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
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_ReindexSwapFailure_ClusterRollsBackToOld is the real
// multinode rollback journey QA finding 5 asked for: a REAL sub-task swap
// failure on one node drives the cluster-wide verdict to FAILED, and every
// replica converges back to the OLD tokenization. No verdict injection, no
// test-only production seam.
//
// The failure is induced by a genuine filesystem fault: after the target
// node finishes reindexing (its per-shard tracker dir exists), a regular
// FILE is planted at the exact path the swap's backup rename will target
// (property_<prop>_searchable__retokenize_backup_<gen>). runtimeSwap's
// os.Rename(<canonical dir>, <backup>) then fails with ENOTDIR — renaming a
// directory onto an existing regular file — exactly the class of mid-flight
// swap fault (#218 working-dir wiped, disk corruption, hardware fault) the
// pre-commit staging protocol exists to survive. Planting the obstruction at
// tracker-dir-appearance (well before the swap) makes the injection
// race-free: nothing touches the backup path until the swap runs.
//
// The other replicas stage their swap successfully, the ack barrier observes
// the failed unit, the task flips to FAILED, and the staged replicas roll
// back to OLD (autoCleanupAfterTerminal -> rollbackStagedSwapsLocally). A
// rolling restart then lets FinalizeCompletedMigrationsWithVerdict converge
// the failed node too. End state: every replica serves the OLD (word) data
// and the schema never flipped.
func TestMultiNode_ReindexSwapFailure_ClusterRollsBackToOld(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "SwapFailRollback"
	const propName = "text"
	classDirLower := strings.ToLower(className)
	restURI := compose.GetWeaviateNode(1).URI()

	// desiredCount=1 -> a single shard replicated on all three nodes; the
	// shard directory has the same name on every replica, so we can target
	// one replica's swap precisely. Searchable-only (no filterable) keeps the
	// migration to a single sub-task.
	falseVal, trueVal := false, true
	createCollection(t, compose, restURI, className, 1, 3, []*models.Property{
		{
			Name: propName, DataType: []string{"text"}, Tokenization: "word",
			IndexFilterable: &falseVal, IndexSearchable: &trueVal,
		},
	})
	defer deleteCollection(t, restURI, className)

	// Import a few hundred objects so the reindex window is comfortably wide.
	docs := make([]string, 0, len(testDocuments)*12)
	for i := 0; i < 12; i++ {
		docs = append(docs, testDocuments...)
	}
	importObjects(t, restURI, className, docs)

	baselines := make(map[string][]int)
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1], "baseline inconsistent for %q", q)
		require.Equalf(t, counts[0], counts[2], "baseline inconsistent for %q", q)
		require.Greaterf(t, counts[0], 0, "baseline %q must match at least one doc", q)
		baselines[q] = counts
	}

	shards := collectShardNamesForClass(t, restURI, className)
	require.Lenf(t, shards, 1, "desiredCount=1 must produce exactly one shard, got %v", shards)
	shard := shards[0]

	// The replica whose searchable swap we sabotage (a follower).
	const failNode = 2
	lsmPath := fmt.Sprintf("/data/%s/%s/lsm", classDirLower, shard)

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, propName,
		`{"searchable":{"tokenization":"field"}}`)

	// Plant the obstruction as soon as the failing node's searchable tracker
	// dir exists. The whole find+plant runs inside the container so no stream
	// framing has to be parsed; we just look for the PLANTED sentinel.
	plantCmd := fmt.Sprintf(
		`d=$(ls -1 %s/.migrations 2>/dev/null | grep -E '^searchable_retokenize_%s_[0-9]+$' | head -1); `+
			`[ -z "$d" ] && exit 1; `+
			`gen=${d##*_}; `+
			`b=%s/property_%s_searchable__retokenize_backup_$gen; `+
			`touch "$b" && [ -f "$b" ] && echo PLANTED "$b"`,
		lsmPath, propName, lsmPath, propName)
	var planted string
	require.Eventuallyf(t, func() bool {
		code, out := execOnNodeShell(ctx, t, compose, failNode, plantCmd)
		if code == 0 && strings.Contains(out, "PLANTED") {
			planted = strings.TrimSpace(out)
			return true
		}
		return false
	}, 120*time.Second, 50*time.Millisecond,
		"searchable retokenize tracker dir must appear on node %d so the swap-failure obstruction can be planted", failNode)
	t.Logf("planted swap-failure obstruction on node %d: %s", failNode, planted)

	// The cluster verdict must be FAILED: one unit's swap cannot complete.
	taskErr := awaitReindexFailed(t, restURI, taskID, 240*time.Second)
	t.Logf("reindex task reached FAILED as expected; error: %s", taskErr)

	// The schema flip must have been skipped — tokenization stays word on
	// every replica (the cluster-wide rollback of the migration decision).
	awaitTokenizationOnAllNodes(t, compose, className, propName, "word")

	// The staged replicas (everything except the sabotaged node) must roll
	// back to the OLD (word) data in memory, without a restart.
	require.Eventuallyf(t, func() bool {
		for _, q := range testBM25Queries {
			actual := perNodeBM25Counts(t, compose, className, q)
			for i := 0; i < 3; i++ {
				if i+1 == failNode {
					continue // the sabotaged replica recovers on restart below
				}
				if actual[i] != baselines[q][i] {
					return false
				}
			}
		}
		return true
	}, 120*time.Second, 500*time.Millisecond,
		"the staged replicas must roll back to the OLD (word) baseline counts after the task FAILED")

	// Remove the obstruction so the sabotaged node's restart-time finalize is
	// clean, then roll the cluster so every replica (including the sabotaged
	// one) converges through FinalizeCompletedMigrationsWithVerdict.
	_, _ = execOnNodeShell(ctx, t, compose, failNode, fmt.Sprintf("rm -f %s/property_%s_searchable__retokenize_backup_* 2>/dev/null; echo done", lsmPath, propName))
	rollingRestartCluster(ctx, t, compose)

	// Final convergence: every replica serves the OLD baseline and the schema
	// is still word.
	awaitTokenizationOnAllNodes(t, compose, className, propName, "word")
	var failures []string
	require.Eventuallyf(t, func() bool {
		failures = failures[:0]
		for _, q := range testBM25Queries {
			actual := perNodeBM25Counts(t, compose, className, q)
			for i := 0; i < 3; i++ {
				if actual[i] != baselines[q][i] {
					failures = append(failures,
						fmt.Sprintf("query=%q node%d expected=%d actual=%d", q, i+1, baselines[q][i], actual[i]))
				}
			}
		}
		return len(failures) == 0
	}, 120*time.Second, 500*time.Millisecond,
		"after the cluster rolled back and restarted, every replica must serve the OLD (word) baseline; mismatches:\n  %s",
		strings.Join(failures, "\n  "))
}

// execOnNodeShell runs `sh -c <cmd>` inside a node's container and returns the
// exit code plus combined output.
func execOnNodeShell(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeIdx int, cmd string) (int, string) {
	t.Helper()
	container := compose.GetWeaviateNode(nodeIdx).Container()
	code, reader, err := container.Exec(ctx, []string{"sh", "-c", cmd})
	require.NoErrorf(t, err, "exec on node %d", nodeIdx)
	out := new(strings.Builder)
	if reader != nil {
		_, _ = io.Copy(out, reader)
	}
	return code, out.String()
}

// awaitReindexFailed polls /v1/tasks until the reindex task reaches FAILED and
// returns its error string. Fatals if the task FINISHES instead — that means
// the injected swap failure did not take effect and the assertions below would
// pass vacuously.
func awaitReindexFailed(t *testing.T, restURI, taskID string, timeout time.Duration) string {
	t.Helper()
	var errMsg string
	require.Eventuallyf(t, func() bool {
		var tasks models.DistributedTasks
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/tasks", restURI), &tasks) {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			if task.Status == "FINISHED" {
				t.Fatalf("reindex task %s FINISHED, but a swap failure was injected — obstruction missed the swap window", taskID)
			}
			if task.Status == "FAILED" {
				errMsg = task.Error
				return true
			}
		}
		return false
	}, timeout, 200*time.Millisecond,
		"reindex task %s must reach FAILED after the injected swap failure", taskID)
	return errMsg
}
