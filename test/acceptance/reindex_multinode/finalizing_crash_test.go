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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_RollingRestartDuringFinalizing_PerReplicaConsistency asserts
// that a rolling restart during a change-tokenization's FINALIZING window
// never leaves replicas with divergent bucket state: the ack barrier holds
// MarkTaskFinalized until every node's swap has acked, so a node that dies
// mid-swap simply re-fires it via rehydrate rather than diverging.
//
// 50k objects keeps data out of the memtable, exercising the LSM
// segment-layout race this guards against.
func TestMultiNode_RollingRestartDuringFinalizing_PerReplicaConsistency(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "FinalizeCrashJourney"
		totalObjects = 50_000
	)
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}
	const expectedPathCount = totalObjects / 5 // exactly even bucketing

	trueVal := true
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"path": paths[i%len(paths)],
		}
	})

	// Pre-migration baseline. Every replica must return the expected
	// count before we start the migration — otherwise the post-restart
	// assertion is meaningless.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, got,
			"pre-migration node %d path count mismatch", nodeIdx)
	}

	// Submit the change-tokenization migration. This is the most
	// expensive shape because change-tokenization creates BOTH a
	// searchable and a filterable per-property reindex task per
	// shard — 6 inflight per node × 3 nodes = 18 in-flight units, and
	// the FINALIZING window is correspondingly the widest among the
	// migration types.
	uri := restURIOf(compose, 1)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	// Race the task to FINALIZING. The poll-cadence is 200ms (in the
	// helper) so we typically land inside the FINALIZING window with
	// hundreds of ms to spare for the rolling-restart trigger.
	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — beginning rolling restart inside this window", taskID, observed)
	// A fast runner can close the window before the restart; that is a
	// missed precondition, not a product failure.
	if observed == "FINISHED" {
		t.Skipf("the finalizing window closed before the rolling restart, so the ack barrier "+
			"could not be exercised; raise totalObjects (currently %d) to widen it", totalObjects)
	}

	// Rolling restart immediately. The intent is to catch one or
	// more nodes mid-swap or mid-ack so the ack barrier's
	// post-restart re-emission path is exercised end-to-end.
	rollingRestartCluster(ctx, t, compose)
	t.Log("rolling restart complete")

	// After rolling restart the task may still be FINALIZING (the ack
	// barrier blocking until every restarted node re-emits its ack via
	// the rehydrate path) — wait for FINISHED with the same 180s budget
	// the rest of the suite uses. A failure here means the recovery
	// path failed to re-emit acks on at least one node.
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))
	t.Log("task FINISHED post-rolling-restart")

	// Poll until every replica converges to expectedPathCount, then sample
	// stability below. AwaitReindexFinished only confirms node-1; a
	// just-restarted node may still be loading buckets
	// (reconciliation). A node that never converges fails here
	// loudly instead of being slept past. testcontainers reallocates ports
	// on stop+start, so restURIOf re-resolves on every call.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", paths[0])
			if err != nil || got != expectedPathCount {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond,
		"per-replica path count never converged to %d on all 3 replicas after rolling restart during FINALIZING",
		expectedPathCount)

	// Per-replica histogram: having converged above, hit each replica
	// directly 30 times (90 total) to verify post-convergence stability.
	// Every poll on every replica must return expectedPathCount; any other
	// value is a per-replica divergence (flap after convergence).
	const pollsPerReplica = 30
	histogram := make(map[string]map[int]int) // nodeID -> count -> N
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		nodeKey := fmt.Sprintf("node-%d", nodeIdx)
		histogram[nodeKey] = map[int]int{}
		for i := 0; i < pollsPerReplica; i++ {
			postURI := restURIOf(compose, nodeIdx)
			got, err := equalCount(postURI, className, "path", paths[0])
			if err != nil {
				histogram[nodeKey][-1]++
				continue
			}
			histogram[nodeKey][got]++
		}
	}

	// Render histogram on failure so the per-replica flap shape is
	// visible in CI logs.
	allCorrect := true
	for _, perNode := range histogram {
		for value, n := range perNode {
			if value != expectedPathCount || n != pollsPerReplica {
				allCorrect = false
			}
		}
	}
	if !allCorrect {
		msg := fmt.Sprintf("per-replica divergence on %q after rolling restart "+
			"during FINALIZING (expected %d, %d polls/replica):",
			"path", expectedPathCount, pollsPerReplica)
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			nodeKey := fmt.Sprintf("node-%d", nodeIdx)
			msg += fmt.Sprintf("\n  %s: %v", nodeKey, histogram[nodeKey])
		}
		t.Fatal(msg)
	}
}

// TestMultiNode_UngracefulStopDuringFinalizing_PerReplicaConsistency asserts
// that a SIGKILL on one node during FINALIZING never leaves replicas
// diverged. Unlike the graceful rolling-restart variant above, the
// ungraceful stop skips the flush path, so recovery finds the node's
// migration record at Merged with its local process state lost.
//
// Target the last node (index 2) so the surviving majority keeps serving
// RAFT while it's down — mirroring a single-pod OOM-kill in production.
//
// The kill must land before the task reaches FINISHED. SWAPPING is an
// acceptable observed status too, since the task can't finish while node 3
// is down; pinning the kill to PREPARING needs a swap-hold gate that doesn't
// exist yet (weaviate/etienne-claude-issues#544). After a further rolling
// restart every node must still serve the count and report tokenization
// field, so a migration committed across the kill survives.
func TestMultiNode_UngracefulStopDuringFinalizing_PerReplicaConsistency(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "FinalizeCrashKillJourney"
		totalObjects = 50_000
	)
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}
	const expectedPathCount = totalObjects / 5

	trueVal := true
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{"path": paths[i%len(paths)]}
	})

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPathCount, "pre-migration")

	uri := restURIOf(compose, 1)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — killing node 3 ungracefully inside this window", taskID, observed)

	// SIGKILL node 3 — k8s OOM / node-hardware-failure shape. The status is read
	// while it is down because the task cannot finish without it, so it shows
	// the phase the kill landed in.
	t.Log("SIGKILL node 3")
	zero := time.Duration(0)
	require.NoError(t, compose.StopAt(ctx, 2, &zero))
	tasks, ok := reindexhelpers.TryFetchTasks(uri)
	require.True(t, ok, "node 1 must list tasks while node 3 is down")
	var status string
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			status = task.Status
		}
	}
	// FINISHED means the kill landed after the task was already done, so the
	// scenario never ran; SWAPPING is fine, since node 3 being down blocks
	// FINISHED either way (no swap-hold gate exists yet to pin this to
	// PREPARING — weaviate/etienne-claude-issues#544).
	require.NotEqualf(t, "FINISHED", status,
		"the kill landed after the task already finished, so the scenario never ran; "+
			"raise totalObjects (currently %d) to widen it", totalObjects)
	require.NoError(t, compose.StartAt(ctx, 2))

	// Wait for FINISHED. Without the ack barrier, the schema flip
	// could already have committed before the kill, leaving node 3
	// with divergent on-disk state on restart — the per-replica
	// histogram below would catch that.
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(240*time.Second))
	t.Log("task FINISHED post-kill-and-restart")

	// AwaitReindexFinished only confirms node-1. The restarted node may still be reconciling.
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPathCount, "after the task finished")

	const pollsPerReplica = 30
	histogram := make(map[string]map[int]int)
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		nodeKey := fmt.Sprintf("node-%d", nodeIdx)
		histogram[nodeKey] = map[int]int{}
		for i := 0; i < pollsPerReplica; i++ {
			postURI := restURIOf(compose, nodeIdx)
			got, err := equalCount(postURI, className, "path", paths[0])
			if err != nil {
				histogram[nodeKey][-1]++
				continue
			}
			histogram[nodeKey][got]++
		}
	}

	allCorrect := true
	for _, perNode := range histogram {
		for value, n := range perNode {
			if value != expectedPathCount || n != pollsPerReplica {
				allCorrect = false
			}
		}
	}
	if !allCorrect {
		msg := fmt.Sprintf("per-replica divergence on %q after SIGKILL "+
			"during FINALIZING (expected %d, %d polls/replica):",
			"path", expectedPathCount, pollsPerReplica)
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			nodeKey := fmt.Sprintf("node-%d", nodeIdx)
			msg += fmt.Sprintf("\n  %s: %v", nodeKey, histogram[nodeKey])
		}
		t.Fatal(msg)
	}

	rollingRestartCluster(ctx, t, compose)
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPathCount, "after a second restart")
	awaitTokenizationOnAllNodes(t, compose, className, "path", "field")
}

func requireEveryReplicaServes(t *testing.T, compose *docker.DockerCompose,
	className, propName, value string, want int, phase string,
) {
	t.Helper()
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		node := nodeIdx
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			got, err := equalCount(restURIOf(compose, node), className, propName, value)
			if !assert.NoErrorf(ct, err, "node %d could not be queried", node) {
				return
			}
			assert.Equalf(ct, want, got, "node %d serves the wrong count for %s=%q", node, propName, value)
		}, 90*time.Second, 200*time.Millisecond,
			"node %d must serve %d objects for %s=%q %s", node, want, propName, value, phase)
	}
}
