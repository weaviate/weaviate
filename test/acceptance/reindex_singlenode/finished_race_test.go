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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestSingleNode_FinishedStatusRaceWithSchemaFlag pins the contract between
// the distributed-task FINISHED status and the schema-flag flip for a semantic
// migration (change-tokenization) under the Journey 3 canonical wiring.
//
// Sequence:
//
//  1. The last unit's `RecordDistributedTaskUnitCompletion` runs (Raft apply).
//     In cluster/distributedtask/manager.go AllUnitsTerminal() → true and
//     task.Status flips to FINISHED. A poller against /v1/tasks now sees
//     FINISHED. The same apply also calls notifySchedulerWithLock which wakes
//     the scheduler reactively (cluster/distributedtask/scheduler.go wakeCh
//     path).
//  2. The scheduler's run loop runs OnTaskCompleted within RAFT-propagation +
//     scheduler-loop latency of the apply — typically low tens of ms with
//     reactive firing, much faster than the periodic tick interval.
//  3. OnTaskCompleted in adapters/repos/db/reindex_provider.go calls
//     applyPerPropertySchemaUpdate (RAFT-idempotent), which flips the
//     property's Tokenization. The flip is observable on /v1/schema once
//     that RAFT entry applies locally.
//
// Contract this test pins: after a poller observes FINISHED on /v1/tasks,
// the schema must catch up within a bounded window. We allow up to 5
// seconds, which is conservative against RAFT propagation hiccups on a
// loaded test runner; in practice it converges in well under 100ms.
//
// Why a window at all (rather than strict equality): the cluster-wide
// atomicity guarantee (no node sees a half-applied migration where some
// shards have swapped buckets but the schema hasn't flipped) requires the
// schema flip to happen *after* every node's local OnGroupCompleted has
// run the per-shard bucket swap. The schema flip is therefore one RAFT
// commit removed from the task-FINISHED transition. The previous
// implementation (commit f937532ea5) inlined the flip BEFORE FINISHED in
// processOneUnit, eliminating this window but introducing a worse
// cross-node window where the first node's swap flipped the schema flag
// for the entire cluster while peer nodes still served the old bucket.
//
// Callers that need to observe the post-migration state synchronously
// should poll /v1/indexes, which has a "finalize-window" override
// (mergeReindexStatus in adapters/handlers/rest/handlers_indexes.go) that
// surfaces "indexing@100%" during this brief window so the UI does not
// flash "ready" with the pre-migration schema.
func TestSingleNode_FinishedStatusRaceWithSchemaFlag(t *testing.T) {
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

	const className = "FinishedRaceTest"

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "filepath", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// A handful of small objects so the reindex is fast — we want the race
	// window to be narrow, the bug then is the FINISHED transition firing
	// before the swap.
	for i := 0; i < 5; i++ {
		obj := &models.Object{Class: className, Properties: map[string]interface{}{
			"filepath": fmt.Sprintf("/a/b/c/file_%d.go", i),
		}}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	taskID := submitIndexUpdate(t, restURI, className, "filepath",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Poll /v1/tasks every ~20ms. On the first observation of FINISHED,
	// immediately read the schema and assert the tokenization flipped.
	deadline := time.Now().Add(120 * time.Second)
	var sawFinishedAt time.Time
	for time.Now().Before(deadline) {
		status, err := fetchTaskStatus(restURI, taskID)
		require.NoError(t, err)
		if status == "FAILED" {
			t.Fatalf("task FAILED before reaching FINISHED")
		}
		if status == "FINISHED" {
			sawFinishedAt = time.Now()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.False(t, sawFinishedAt.IsZero(), "task never reached FINISHED")

	// Poll the schema for up to flipWindow after FINISHED was first observed.
	// The schema flip happens in ReindexProvider.OnTaskCompleted (one RAFT
	// commit after the task transitions to FINISHED). The reactive scheduler
	// wake-up + OnTaskCompleted body + RAFT-apply latency is typically well
	// under 100ms on a healthy cluster; allow 5s for headroom on a loaded
	// test runner. If it doesn't converge within this window, the wiring
	// is broken (e.g. notifier not installed, OnTaskCompleted not flipping,
	// RAFT commit silently rejected).
	const flipWindow = 5 * time.Second
	var lastObservedTokenization string
	var sawFieldAt time.Time
	pollDeadline := sawFinishedAt.Add(flipWindow)
	for time.Now().Before(pollDeadline) {
		cls := helper.GetClass(t, className)
		for _, prop := range cls.Properties {
			if prop.Name == "filepath" {
				lastObservedTokenization = prop.Tokenization
			}
		}
		if lastObservedTokenization == "field" {
			sawFieldAt = time.Now()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if sawFieldAt.IsZero() {
		t.Fatalf(
			"schema flag never flipped to %q within %v after FINISHED was first observed; "+
				"last observed tokenization=%q. The Journey 3 wiring "+
				"(ReindexProvider.OnTaskCompleted → applyPerPropertySchemaUpdate) "+
				"is not firing or its RAFT commit is not landing.",
			"field", flipWindow, lastObservedTokenization)
	}

	convergenceLag := sawFieldAt.Sub(sawFinishedAt)
	t.Logf("schema observed at target tokenization=%q after %v of FINISHED being visible on /v1/tasks",
		lastObservedTokenization, convergenceLag)

	// Sanity floor: the window must be SHORTER than the periodic scheduler
	// tick interval, otherwise reactive firing isn't actually firing and
	// we're observing the tick fallback path. The test container sets the
	// tick to 1s; we should comfortably observe convergence in under 1s.
	require.Less(t, convergenceLag, time.Second,
		"schema flip took >= 1s after FINISHED — reactive firing is not engaging, "+
			"the wake-up path is broken and we are observing the tick fallback")
}

// fetchTaskStatus returns the status string for the given reindex task, or
// the empty string if not present.
func fetchTaskStatus(restURI, taskID string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var tasks models.DistributedTasks
	if err := json.Unmarshal(body, &tasks); err != nil {
		return "", err
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task.Status, nil
		}
	}
	return "", nil
}
