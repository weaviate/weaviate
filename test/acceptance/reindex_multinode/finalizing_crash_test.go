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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
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
	if observed == "FINISHED" {
		// At 50k records the FINALIZING window can be sub-100ms on a
		// fast machine. The test would still verify the per-replica
		// histogram is correct (it always must be), but it wouldn't
		// exercise the ack barrier on the FINALIZING → FINISHED
		// transition. Surface this loudly so the next test run can
		// bump the dataset.
		t.Logf("WARNING: FINALIZING window already closed before rolling restart; "+
			"per-replica histogram still asserted, but the ack barrier was not stressed. "+
			"Consider increasing totalObjects (currently %d) to widen FINALIZING window.",
			totalObjects)
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

	// Pre-migration baseline.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, got, "pre-migration node %d", nodeIdx)
	}

	uri := restURIOf(compose, 1)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — killing node 3 ungracefully inside this window", taskID, observed)
	if observed == "FINISHED" {
		t.Logf("WARNING: FINALIZING window already closed; ack-barrier crash-path not stressed. " +
			"Consider increasing totalObjects to widen the window.")
	}

	// SIGKILL + restart node 3 — k8s OOM / node-hardware-failure shape.
	t.Log("SIGKILL + restart node 3")
	cycleNodeFastKill(ctx, t, compose, 2)

	// Wait for FINISHED. Without the ack barrier, the schema flip
	// could already have committed before the kill, leaving node 3
	// with divergent on-disk state on restart — the per-replica
	// histogram below would catch that.
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))
	t.Log("task FINISHED post-kill-and-restart")

	// Poll until every replica converges to expectedPathCount, then sample
	// stability below. AwaitReindexFinished only confirms node-1; the
	// SIGKILL'd-and-restarted node may still be loading buckets
	// (reconciliation). A node that never converges fails here
	// loudly instead of being slept past.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", paths[0])
			if err != nil || got != expectedPathCount {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond,
		"per-replica path count never converged to %d on all 3 replicas after SIGKILL during FINALIZING",
		expectedPathCount)

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
}
