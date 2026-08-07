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
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestSingleNode_FinishedStatusRaceWithSchemaFlag pins the ordering between
// the distributed-task FINISHED status and the schema-flag flip for a semantic
// migration (change-tokenization) under the canonical single-node wiring.
//
// The flip is not something FINISHED races: MarkTaskFinalized is only proposed
// after OnTaskCompleted has returned nil, and OnTaskCompleted's schema update
// is itself a RAFT apply. The flip therefore commits to the log strictly
// before FINISHED does, and on one node it is applied first.
//
// That ordering is what lets the index-status endpoint read both signals from
// the local FSM: for a migration that turns a per-property flag on, FINISHED
// with the flag off is a stale task rather than a pending swap. This test pins it end to end: at the first observation of
// FINISHED the schema must ALREADY report the new tokenization.
//
// The window between the units stopping and the flip is reported by the
// PREPARING and SWAPPING statuses, not by FINISHED. The test also reads
// `finishedAt` along the way: absent while the migration is in flight, and a
// sane moment once FINISHED (weaviate/0-weaviate-issues#501).
//
// What one node cannot prove: that the stamp came off the RAFT request
// rather than the applying node's own clock. Here the proposing node and
// the applying node are the same machine, so the two are indistinguishable
// by observation. The wire-vs-local-clock provenance is pinned at the FSM
// level instead, by TestManager_FinishedAt_StampedAtTheTerminalTransition,
// which sets the request timestamp an hour off the harness clock.
//
// It also reads FINISHED from /v1/tasks (which answers at the leader) and
// the schema from the local node. Sound only because leader and local are
// the same node here — promoted to the multi-node suite as written, this
// would flake on apply lag and blame the wiring for a read split.
func TestSingleNode_FinishedStatusRaceWithSchemaFlag(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.StartSingleNode(ctx)
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

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "filepath",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Poll /v1/tasks. On the first observation of FINISHED, immediately
	// capture the timestamp so the convergence-lag measurement below stays
	// accurate. Keep a tight 20ms tick — this test measures the sub-second
	// lag between FINISHED and the schema flip, so the cadence is
	// load-bearing.
	var (
		sawFinishedAt time.Time
		finished      *models.DistributedTask
		// Recorded rather than asserted inline: testify runs the condition
		// on its own goroutine, where a failed require would only surface
		// as a 120s timeout.
		inFlightWithAFinishTime string
		inFlightPolls           int
		sawFailed               bool
		fetchErr                error
	)
	require.Eventually(t, func() bool {
		task, err := fetchTask(restURI, taskID)
		if err != nil {
			fetchErr = err
			return true
		}
		if task == nil {
			return false
		}
		if task.Status == "FAILED" {
			// Stop polling and report below rather than failing here: the
			// task will never reach FINISHED, and a t.Fatalf on this
			// goroutine would surface as a 120s timeout instead.
			sawFailed = true
			return true
		}
		if task.Status != "FINISHED" {
			inFlightPolls++
			if task.FinishedAt != nil && inFlightWithAFinishTime == "" {
				inFlightWithAFinishTime = fmt.Sprintf("%s carried finishedAt=%v", task.Status, *task.FinishedAt)
			}
			return false
		}
		sawFinishedAt = time.Now()
		finished = task
		return true
	}, 120*time.Second, 20*time.Millisecond)
	require.NoError(t, fetchErr, "polling /v1/tasks failed")
	require.False(t, sawFailed, "task FAILED before reaching FINISHED")
	require.False(t, sawFinishedAt.IsZero(), "task never reached FINISHED")

	// Opportunistic: this is a 20 ms poll against a migration whose whole
	// coordination phase can be under 30 ms, so a run may see zero non-terminal
	// polls and check nothing here. Logged rather than required for that
	// reason. "Non-terminal ⇒ no finish time" is pinned deterministically at
	// the FSM (TestStructuralInvariant_FinishedAtIffTerminal, after every
	// apply) and at the render layer
	// (TestHandler_ListTasks_InFlightTaskOmitsFinishedAt).
	t.Logf("sampled %d in-flight polls of /v1/tasks", inFlightPolls)
	require.Empty(t, inFlightWithAFinishTime,
		"a migration in flight has not ended, so /v1/tasks must report no finish time for it")

	// The stamp must be the terminal transition, not a leftover from an
	// earlier phase and not a placeholder. Bounded below by the task's own
	// submit time (same clock, so no skew) and above by the moment we first
	// saw FINISHED, plus slack for host-vs-container clock drift.
	require.NotNil(t, finished.FinishedAt, "a FINISHED task must carry its finish time")
	finishedAt := time.Time(*finished.FinishedAt)
	require.False(t, finishedAt.IsZero(), "a FINISHED task's finish time must not be the zero time")
	require.False(t, finishedAt.Before(time.Time(finished.StartedAt)),
		"the task cannot have ended before it was submitted (startedAt=%v, finishedAt=%v)",
		finished.StartedAt, finishedAt)
	require.False(t, finishedAt.After(sawFinishedAt.Add(clockSkewSlack)),
		"finishedAt is in the future relative to the moment FINISHED became visible (%v vs %v)",
		finishedAt, sawFinishedAt)
	t.Logf("FINISHED carries finishedAt=%v (submitted at %v)", finishedAt, finished.StartedAt)

	// No polling: the flip is ordered before FINISHED in the RAFT log, so it
	// is already applied by the time FINISHED is observable. A retry loop here
	// would hide exactly the regression this test exists to catch.
	var observedTokenization string
	for _, prop := range helper.GetClass(t, className).Properties {
		if prop.Name == "filepath" {
			observedTokenization = prop.Tokenization
		}
	}
	require.Equal(t, "field", observedTokenization,
		"the schema flip commits before FINISHED does, so a task reported FINISHED "+
			"must never render against the pre-migration schema. Either "+
			"ReindexProvider.OnTaskCompleted -> applyPerPropertySchemaUpdate stopped "+
			"preceding MarkTaskFinalized, or FINISHED is being reported early.")
	t.Logf("schema already at tokenization=%q when FINISHED first became visible (observed at %v)",
		observedTokenization, sawFinishedAt)
}

// clockSkewSlack absorbs drift between the test host's clock and the
// container's. On a Linux host they are the same clock and the slack is
// unused; under a Docker Desktop VM they can differ by seconds.
const clockSkewSlack = 2 * time.Minute

// fetchTaskStatus returns the status string for the given reindex task, or
// the empty string if not present.
func fetchTaskStatus(restURI, taskID string) (string, error) {
	task, err := fetchTask(restURI, taskID)
	if err != nil || task == nil {
		return "", err
	}
	return task.Status, nil
}

// fetchTask returns the named reindex task as /v1/tasks renders it, or nil if
// the list does not carry it.
func fetchTask(restURI, taskID string) (*models.DistributedTask, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var tasks models.DistributedTasks
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, err
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return &task, nil
		}
	}
	return nil, nil
}
