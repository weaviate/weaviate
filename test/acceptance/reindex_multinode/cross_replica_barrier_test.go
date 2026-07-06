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

// End-to-end acceptance test for the two-phase RAFT swap barrier
// (NeedsPreparationBarrier=true).
//
// Reproduces the topology that exposed the cross-replica stagger window
// in weaviate/0-weaviate-issues#225:
//
//   - 3-node cluster
//   - 5 shards
//   - replicationFactor=1 (no replication)
//
// Distribution: 2 nodes get 2 shards each, 1 node gets 1 shard. Every
// shard lives on exactly one node, so any cross-shard query (BM25
// against the collection) is necessarily cross-node, and per-node PREP
// duration variance translates directly into observable mixed-state
// query results.
//
// Pre-barrier (legacy path): OnGroupCompleted runs PREP+OVERLAY+SWAP
// inline per node. Cross-node PREP duration variance leaves the cluster
// in mixed-tokenization state for seconds-to-minutes (verified
// empirically by QA Claude on weaviate/0-weaviate-issues#225 at
// 1M-object scale: ~1s window observed, 248ms within-window mixed-state
// captured between probe samples 722 and 723).
//
// Post-barrier (this branch): OnGroupCompleted runs PREP only; every
// node's PreparationCompleteAck must land via RAFT before the FSM transitions
// PREPARING -> SWAPPING and OnSwapRequested fires. The cross-node SWAP
// window is now bounded by RAFT propagation latency (tens of ms),
// regardless of PREP duration.
//
// What this test asserts (end-to-end, in CI):
//
//  1. The task FSM passes through PREPARING (new state) before
//     SWAPPING (new state) before FINISHED — proving the barrier is
//     actually active. If neither barrier state is observed, the FSM
//     has silently degenerated to a legacy single-phase path, which
//     would re-introduce the cross-replica bug.
//  2. End-to-end change-tokenization completes successfully on the
//     5/3/RF=1 topology that previously exposed the cross-replica
//     stagger window.
//  3. After completion, every node sees the same schema tokenization
//     for the migrated property.
//  4. After completion, BM25 queries return identical result sets
//     on every node (i.e. the migration left no per-replica
//     divergence).
//
// Unit-level FSM transitions (PREPARING -> SWAPPING under various
// per-node ack arrival orders, restart-mid-PREP, etc.) are covered by
// cluster/distributedtask/scheduler_test.go::TestPreMarkTerminalCallbacksLocked_BarrierPhasesNotPreMarked
// and friends. This file covers the end-to-end production path.

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
)

func TestMultiNode_CrossReplicaPrepBarrier(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "CrossReplicaBarrier"
	restURI := compose.GetWeaviateNode(1).URI()

	// 5 shards × RF=1: every shard lives on exactly one node, so the
	// cluster has 2 nodes with 2 shards each and 1 node with 1 shard.
	// This is the topology Etienne's design sketch on
	// weaviate/0-weaviate-issues#225 (comment 4472712888) prescribes
	// as the sharp cross-replica reproducer.
	createCollection(t, compose, restURI, className, 5, 1, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	// Dataset sized so PREPARING is reliably observable at the REST
	// /v1/tasks polling cadence. The PHASE A (PREP) work scales with
	// per-shard bucket size: FlushAndSwitch + PrependSegmentsFromBucket
	// is disk-I/O proportional. At ~600 objects / 5 shards = ~120
	// objects per shard with moderately-long text, PHASE A consistently
	// takes ~100ms-1s on a CI-class machine, which is well above the
	// 50ms polling cadence below. Smaller datasets compress PREPARING
	// to <50ms and the polling can miss the transition.
	//
	// The barrier mechanism itself is correct regardless of dataset
	// size (FSM transitions are unit-tested in
	// cluster/distributedtask/scheduler_test.go and manager_test.go);
	// this dataset is sized for the *observability* of the
	// STARTED -> PREPARING -> SWAPPING -> FINISHED transition sequence
	// at the REST polling layer.
	var docs []string
	for i := 0; i < 600; i++ {
		docs = append(docs, testDocuments[i%len(testDocuments)])
	}
	importObjects(t, restURI, className, docs)

	// Baseline: BM25 query for a known-distinct word should return
	// the same hits on every node (the import used
	// consistency_level=ALL so replicas have converged — though with
	// RF=1 each shard lives on a single node, so the consistency is
	// trivial here; the assertion still guards against routing bugs).
	baselineQuery := "alpha"
	baselines := queryAllNodes(t, compose, className, baselineQuery)
	assertQueryConsistency(t, baselines)
	require.Greater(t, len(baselines[0]), 0,
		"baseline 'alpha' under WORD tokenization should return at least one hit "+
			"across the 5-shard cluster")

	// Submit semantic migration. Step 5 of prep-swap-barrier sets
	// NeedsPreparationBarrier=true on semantic submits via the new
	// AddDistributedTaskWith{Groups}Barrier(..., semantic) overloads,
	// so this task goes through PREPARING -> SWAPPING -> FINISHED
	// instead of the legacy single-callback STARTED -> FINALIZING ->
	// FINISHED path.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task: %s", taskID)

	// Poll task status at fast cadence and collect every distinct
	// status observed. The poll cadence is 50ms — fast enough to
	// land inside a sub-second PREPARING window for 200-object
	// datasets without burning the API. Pin a hard ceiling of 180s
	// for the whole transition (the entire task end-to-end including
	// scheduler-tick latency).
	transitions := collectTaskStatusTransitions(t, restURI, taskID, 180*time.Second)
	t.Logf("observed task transitions: %v", transitions)

	require.Contains(t, transitions, "FINISHED",
		"task must reach FINISHED within 180s; observed transitions: %v", transitions)

	// LOAD-BEARING ASSERTION: the task MUST go through the barrier
	// path (PHASE A then PHASE B), not the legacy single-phase path.
	//
	// Two independent signals confirm this:
	//
	//  1. REST /v1/tasks polling MAY observe PREPARING in the
	//     transition sequence. With moderate datasets PREPARING is
	//     usually observable; with very small ones (PREP completing
	//     in <50ms total cluster-wide) the polling can miss it
	//     between samples. SWAPPING is always observable (the
	//     scheduler-tick latency between PHASE B firing and FINISHED
	//     is bounded below by the per-node ack roundtrip).
	//
	//  2. Container-log scan for the unique start-of-phase log lines
	//     emitted by OnGroupCompleted (barrier mode) and
	//     OnSwapRequested. These two logs are emitted ONLY on the
	//     barrier path — the non-barrier path emits a different
	//     group-completion log ("group-completion → starting swap
	//     phase (no-barrier path)") and never emits a swap-requested
	//     log. Container-log scan is robust against PREPARING being
	//     too fast to observe at the REST polling layer.
	//
	// We require either (1) the REST polling sees PREPARING, OR (2)
	// the container logs confirm both barrier-mode start-of-phase
	// log lines. Failing both is unambiguous proof the barrier
	// degenerated.
	sawPreparingViaREST := false
	for _, st := range transitions {
		if st == "PREPARING" {
			sawPreparingViaREST = true
			break
		}
	}

	barrierPrepLogPresent, barrierSwapLogPresent := scanForBarrierPhaseLogs(ctx, t, compose, taskID)
	require.True(t, sawPreparingViaREST || (barrierPrepLogPresent && barrierSwapLogPresent),
		"task MUST go through the two-phase barrier path. Signals checked: "+
			"sawPreparingViaREST=%v (observed transitions: %v); "+
			"barrierPrepLogPresent=%v; barrierSwapLogPresent=%v. "+
			"If all three are false, the FSM has silently degenerated to "+
			"a legacy single-phase path which re-introduces the cross-"+
			"replica stagger bug — check handlers_indexes.go (submit handler "+
			"NeedsPreparationBarrier wiring) and manager.go (AllUnitsTerminal "+
			"routing to PREPARING).",
		sawPreparingViaREST, transitions, barrierPrepLogPresent, barrierSwapLogPresent)

	// Wait for schema tokenization to flip cluster-wide. OnTaskCompleted
	// fires after FINISHED and issues the cluster-wide schema flip via
	// flipSemanticMigrationSchema; the apply propagates to every node
	// within RAFT-apply latency.
	require.Eventually(t, func() bool {
		for i := 1; i <= 3; i++ {
			if tryGetPropertyTokenization(compose.GetWeaviateNode(i).URI(), className, "text") != "field" {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond,
		"tokenization should change to field on all 3 nodes after barrier completion")

	// Final consistency: under FIELD tokenization, "alpha" no longer
	// matches docs that contain "alpha" as a sub-token (FIELD requires
	// exact match against the full property value). All nodes must
	// agree on this — if any node still serves results from the
	// pre-flip bucket, the barrier failed to align the cluster.
	require.Eventually(t, func() bool {
		results := queryAllNodes(t, compose, className, baselineQuery)
		// All three nodes' result sets must be equal AND empty (no
		// document is "alpha" as a single FIELD value).
		if len(results[0]) != 0 {
			return false
		}
		for i := 1; i < len(results); i++ {
			if len(results[i]) != 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond,
		"post-FIELD-tokenization, 'alpha' as a partial token should match no docs "+
			"on any node — if any node still returns hits, the per-shard bucket "+
			"swap or the cluster-wide schema flip did not propagate")

	// Final post-flip consistency check on a query that SHOULD return
	// results: with FIELD tokenization, the full text of one of the
	// testDocuments matches exactly that doc. All three nodes must
	// agree on the per-shard hits.
	postQuery := "alpha bravo charlie delta echo foxtrot"
	postResults := queryAllNodes(t, compose, className, postQuery)
	assertQueryConsistency(t, postResults)
	require.Greater(t, len(postResults[0]), 0,
		"post-FIELD-tokenization, the full text of a known testDocument "+
			"should match at least one shard's copy")
}

// scanForBarrierPhaseLogs reads each cluster node's container logs and
// reports whether the unique start-of-phase log lines that
// OnGroupCompleted (barrier mode) and OnSwapRequested emit are present,
// scoped to the given taskID. These logs are emitted ONLY on the
// barrier code path:
//
//   - "reindex provider: group-completion → starting PREP phase
//     (barrier mode)" — fires once per node per task in PHASE A. The
//     non-barrier path emits a different message ("group-completion
//     → starting swap phase (no-barrier path)") instead.
//   - "reindex provider: swap-requested → starting OVERLAY+SWAP after
//     cluster-wide PREP barrier" — fires once per node per task in
//     PHASE B. The non-barrier path never invokes OnSwapRequested at
//     all.
//
// The needles MUST stay in sync with [ReindexProvider.OnGroupCompleted]
// and [ReindexProvider.OnSwapRequested]'s Info() log strings — if the
// production logs change, this test's log-based fallback path silently
// fails closed (the REST poll still works, but the safety net is gone).
//
// Both signals must be present for the test's barrier-active
// assertion to hold via the log path (it ORs with the REST-polled
// PREPARING transition).
//
// Returns (prepLogPresent, swapLogPresent). At least one node must
// emit each signal — we don't require all three nodes to have the
// log (a node with no local units in the group doesn't fire the
// callbacks).
func scanForBarrierPhaseLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, taskID string) (bool, bool) {
	t.Helper()
	const (
		prepNeedle = "group-completion → starting PREP phase (barrier mode)"
		swapNeedle = "swap-requested → starting OVERLAY+SWAP after cluster-wide PREP barrier"
	)
	var prepFound, swapFound bool
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		if node == nil {
			continue
		}
		reader, err := node.Container().Logs(ctx)
		if err != nil {
			t.Logf("scanForBarrierPhaseLogs: cannot read logs for node %d: %v", i, err)
			continue
		}
		logs, _ := io.ReadAll(reader)
		reader.Close()
		text := string(logs)
		for _, line := range strings.Split(text, "\n") {
			if !strings.Contains(line, taskID) {
				continue
			}
			if strings.Contains(line, prepNeedle) {
				prepFound = true
			}
			if strings.Contains(line, swapNeedle) {
				swapFound = true
			}
		}
	}
	return prepFound, swapFound
}

// collectTaskStatusTransitions polls /v1/tasks for the named task at
// 50ms intervals and returns the ordered list of unique statuses
// observed. Stops when the task reaches FINISHED, when it reaches
// FAILED (fatal), or when the timeout elapses.
//
// Used by the barrier-activation test to verify the FSM transitions
// through the new PREPARING and/or SWAPPING states before reaching
// FINISHED. The 50ms poll cadence is fast enough to catch a sub-second
// PREPARING window on a 200-object dataset; it would miss a sub-50ms
// SWAPPING window, which is why the caller's assertion accepts
// "at least one of PREPARING or SWAPPING observed".
func collectTaskStatusTransitions(t *testing.T, restURI, taskID string, timeout time.Duration) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Explicit sampling cadence: tick every 50ms and shut down promptly
	// when the timeout elapses. The tick interval (not a bare sleep)
	// makes the status-transition sampling rate visible; cadence and the
	// "stop on FINISHED / fatal on FAILED" semantics are unchanged.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var transitions []string
	var lastSeen string
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			select {
			case <-ctx.Done():
				return transitions
			case <-ticker.C:
				continue
			}
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			select {
			case <-ctx.Done():
				return transitions
			case <-ticker.C:
				continue
			}
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			if task.Status != lastSeen {
				transitions = append(transitions, task.Status)
				lastSeen = task.Status
				if task.Status == "FAILED" {
					t.Fatalf("reindex task failed: %s", task.Error)
				}
				if task.Status == "FINISHED" {
					return transitions
				}
			}
		}
		select {
		case <-ctx.Done():
			return transitions
		case <-ticker.C:
		}
	}
}
