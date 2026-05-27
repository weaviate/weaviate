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

// TestMultiNode_RollingRestartDuringFinalizing_PerReplicaConsistency
// asserts that a rolling restart landing inside the FINALIZING window
// of a change-tokenization migration does not produce per-replica
// divergent on-disk bucket state.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/214 (Gap A,
// "Repro 2: rolling-restart during FINALIZING"). The production
// reproduction was a per-replica `path = 7×32, 3×30, 2×23, 6×5`
// histogram on a steady probe after the cluster had reported the task
// FINISHED. Fixed by the per-shard post-completion ack barrier in
// https://github.com/weaviate/weaviate/pull/11318 (the ack-barrier
// commits), which gates MarkTaskFinalized on every node's
// OnGroupCompleted having returned and acked.
//
// With the ack barrier in place, no node can let the schema flip
// commit before its OnGroupCompleted has returned and its ack has
// landed in RAFT. A node that goes down between local swap and ack
// emission either:
//   - already finished the swap (sentinels present on disk; the
//     post-restart FinalizeCompletedMigrations + RecoveryAwareProvider
//     path re-emits the ack on the next scheduler tick), OR
//   - died mid-swap (LocalCallbacksDone reports false; OnGroupCompleted
//     re-fires via the rehydrate path; ack emitted after rehydrate +
//     swap completes).
//
// What this test asserts:
//
//   - The task transitions to FINISHED only after every replica's
//     bucket-pointer state matches the new tokenization.
//   - Per-replica histogram across 30 polls per replica is all the
//     expected count for the migrated property (no flap, no
//     divergence).
//
// Dataset size: 50k objects. Smaller datasets keep most data in the
// memtable and hide the LSM-segment-layout race regime where the bug
// originally manifested in production.
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
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
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
	taskID := reindexhelpers.SubmitIndexUpdate(t, uri, className, "path",
		`{"searchable":{"tokenization":"field"}}`)
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
	// (FinalizeCompletedMigrations). A node that never converges fails here
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

// TestMultiNode_UngracefulStopDuringFinalizing_PerReplicaConsistency
// asserts that a SIGKILL on a single node during the FINALIZING
// window does not produce per-replica divergent on-disk bucket state.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/214 (Gap A,
// "Repro 1: SIGKILL during FINALIZING"), fixed by the same
// post-completion ack barrier as the rolling-restart variant above.
//
// Where the rolling-restart test (above) catches the cluster mid-swap
// with a graceful shutdown sequence (testcontainers' Stop sends
// SIGTERM first), this variant uses an ungraceful 0-second-timeout
// Stop on a SINGLE node. That bypasses the on-shutdown flush path and
// lets the post-restart RecoveryAwareProvider +
// FinalizeCompletedMigrations have to handle a node whose
// .migrations/ directory is at the half-merged stage (no tidied.mig,
// no swapped.mig) with the local process state lost.
//
// The expected behavior with the ack barrier:
//  1. SIGKILL on the targeted node either lands before its ack was
//     emitted (LocalCallbacksDone reports the half-finalized state on
//     restart, OnGroupCompleted re-fires via rehydrate, swap completes,
//     ack emitted) OR after its ack was emitted (the FSM already has
//     the ack persisted via RAFT, the post-restart scheduler sees the
//     ack in the snapshot/log and does not re-emit).
//  2. The cluster-wide FINISHED transition waits until every node has
//     acked (per-node MissingPostCompletionAckNodes empty).
//  3. Per-replica histogram is uniform after FINISHED — no divergence.
//
// We target the LAST node in the cluster index (node 2 / index 2) so
// the surviving majority continues serving RAFT while node 2 is down,
// which mirrors the production single-pod failure case (one pod gets
// OOM-killed, the other two stay up).
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
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
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
	taskID := reindexhelpers.SubmitIndexUpdate(t, uri, className, "path",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — killing node 3 ungracefully inside this window", taskID, observed)
	if observed == "FINISHED" {
		t.Logf("WARNING: FINALIZING window already closed; ack-barrier crash-path not stressed. " +
			"Consider increasing totalObjects to widen the window.")
	}

	// Kill node 3 ungracefully — 0s timeout means Docker SIGKILLs
	// immediately, skipping the on-shutdown bucket flush. This is the
	// k8s OOM / node-hardware-failure shape.
	killTimeout := 0 * time.Second
	t.Log("SIGKILL on node 3 (timeout=0)")
	require.NoError(t, compose.StopAt(ctx, 2, &killTimeout))

	// Restart node 3. The post-restart path must reconcile its
	// half-finalized .migrations/ directory and re-emit the ack via
	// the rehydrate path if its pre-kill ack hadn't landed in RAFT.
	t.Log("restarting node 3")
	require.NoError(t, compose.StartAt(ctx, 2))

	// Wait for FINISHED. Without the ack barrier, the schema flip
	// could already have committed before the kill, leaving node 3
	// with divergent on-disk state on restart — the per-replica
	// histogram below would catch that.
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))
	t.Log("task FINISHED post-kill-and-restart")

	// Poll until every replica converges to expectedPathCount, then sample
	// stability below. AwaitReindexFinished only confirms node-1; the
	// SIGKILL'd-and-restarted node may still be loading buckets
	// (FinalizeCompletedMigrations). A node that never converges fails here
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
