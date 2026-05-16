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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/models"
)

// TestMultiNode_PostRestartMigration_NoStallPlateau pins GH
// 0-weaviate-issues#212 Issue B: after a rolling restart, a
// subsequent migration's scheduler must not stall at (N-1)/N progress
// for an indefinite period.
//
// Original symptom (from `1.38.0-dev-9c1591e.amd64`): a post-restart
// change-tokenization task plateaued at 6/9 = 66.7% for 10+ minutes
// observed, never resuming. All units on one weaviate-N pod stayed
// PENDING because that pod's scheduler stopped picking up new
// distributed-task units after the rolling restart.
//
// Current state on builds with `b6a5c6bd` + `faa2c780ec`: the plateau
// briefly shows up (the user reported ~52s on `1.38.0-dev-e1dfae0`)
// but the retry path now unblocks within a minute and the migration
// completes end-to-end in ~3 minutes. This regression test asserts
// that the plateau remains brief — not that it's absent. If the
// plateau persists longer than the budget below, the underlying
// scheduler-stall regression is back.
//
// The budget is intentionally tighter than awaitReindexFinished's
// default 180 s so a regression to "stalls forever (or for many
// minutes)" fails the test instead of relying on the worker pod's
// 10-min hang to eventually trip an unrelated timeout.
func TestMultiNode_PostRestartMigration_NoStallPlateau(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "PostRestartPlateau"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Phase 1: a successful migration BEFORE the restart, so the
	// shards have a non-empty .migrations/ history. Without this,
	// the post-restart migration would start from gen=1 with no
	// prior state to recover, which doesn't exercise the
	// scheduler-stall code path the bug originally hit (the bug
	// surfaced when the post-restart shard had a half-applied prior
	// migration's state on disk that wedged its DTM unit pickup).
	runChangeTokMigration(t, compose, className, "text", "field")

	// Phase 2: rolling restart of every pod.
	restartCluster(ctx, t, compose)

	// Phase 3: submit a fresh migration and time it. Below we use a
	// tighter completion budget than awaitReindexFinished's 180 s
	// default — if the scheduler-stall regression returns, the task
	// will plateau at (N-1)/N for the whole window and this Eventually
	// will time out cleanly with a useful error including the last
	// observed progress.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	t.Logf("submitted post-restart task: %s", taskID)

	// Track the longest stretch of time the task spent at a
	// non-terminal fractional progress without making forward
	// movement. If the task's max-progress doesn't change for
	// `plateauBudget`, the plateau-persists regression has hit;
	// fail the test BEFORE the outer Eventually's overall budget
	// runs out so the error message points at the right cause.
	//
	// Budgets are sized to catch indefinite stalls but not flake on
	// the known-bounded plateau that survives `b6a5c6bd` +
	// `faa2c780ec`. The user-reported plateau on the 1M-record demo
	// cluster was ~52 s; we run on the 25-record testDocuments which
	// is much smaller in iteration work but the RAFT-tick-bound
	// portion of the plateau doesn't shrink with data size. 120 s
	// plateau gives ~2× headroom over the 52 s observation while
	// being tight enough that the original "indefinite (10+ min)"
	// stall trips the assertion before the overall budget expires.
	const (
		overallBudget = 240 * time.Second
		plateauBudget = 120 * time.Second
	)

	type progressSample struct {
		t          time.Time
		atProgress string
	}
	var (
		startedAt       = time.Now()
		lastProgress    = "(never observed)"
		lastProgressAt  = startedAt
		plateauSamples  []progressSample
		taskFinished    bool
		finalTaskStatus string
	)

	for time.Since(startedAt) < overallBudget {
		status, progress, ok := tryFetchTaskStatusAndProgress(restURI, taskID)
		if ok {
			finalTaskStatus = status
			if status == "FINISHED" {
				taskFinished = true
				break
			}
			if status == "FAILED" {
				t.Fatalf("post-restart task transitioned to FAILED — this regression test is for STALL not FAIL: status=%s", status)
			}
			if progress != lastProgress {
				lastProgress = progress
				lastProgressAt = time.Now()
				plateauSamples = append(plateauSamples, progressSample{t: lastProgressAt, atProgress: progress})
			}
			if time.Since(lastProgressAt) > plateauBudget {
				// The plateau-persists regression we're guarding
				// against. Dump the progress trail so the operator
				// has a starting point.
				t.Logf("progress trail: %v", plateauSamples)
				t.Fatalf(
					"GH #212 Issue B regression: post-restart migration "+
						"plateaued at %q for %s (budget %s). The scheduler "+
						"stall fix in b6a5c6bd / faa2c780ec is no longer "+
						"keeping the plateau brief.",
					lastProgress, time.Since(lastProgressAt), plateauBudget,
				)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !taskFinished {
		t.Logf("progress trail: %v", plateauSamples)
		t.Fatalf(
			"GH #212 Issue B regression: post-restart migration did not "+
				"reach FINISHED within %s (last seen status=%q, last progress=%q "+
				"%s ago). Either the migration is stalling indefinitely or the "+
				"budget here needs revisiting for genuinely slow but progressing "+
				"runs — check the progress trail.",
			overallBudget, finalTaskStatus, lastProgress, time.Since(lastProgressAt),
		)
	}

	t.Logf("post-restart migration FINISHED in %s (plateau samples: %d)",
		time.Since(startedAt), len(plateauSamples))

	// Final sanity: every node must agree on the schema's resulting
	// tokenization. The actual data divergence assertion is already
	// covered by R1b — here we only care about the scheduler-stall
	// regression.
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	// We don't assert progress-sample count here — the migration on
	// 25 testDocuments can run faster than our 500 ms poll, so zero
	// samples is plausible on a fast machine. The plateau-budget
	// assertion above is the real guard against the bug.
}

// tryFetchTaskStatusAndProgress reads the named task from /v1/tasks and
// returns its current status and a human-readable summary of per-unit
// progress (used as the equality key for plateau detection). Returns
// `ok=false` when the request errors or the task is not in the
// response.
func tryFetchTaskStatusAndProgress(restURI, taskID string) (status, progress string, ok bool) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return "", "", false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", false
	}
	// Tolerant parse: only need the namespaced task list + per-unit
	// fields. The full DistributedTasks model in entities has more
	// nesting than necessary here, so unmarshal into a tight shape.
	var resp1 map[string][]struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Units  []struct {
			ID       string  `json:"id"`
			NodeID   string  `json:"nodeId"`
			Status   string  `json:"status"`
			Progress float32 `json:"progress"`
		} `json:"units"`
	}
	if err := json.Unmarshal(body, &resp1); err != nil {
		return "", "", false
	}
	for _, task := range resp1["reindex"] {
		if task.ID != taskID {
			continue
		}
		// Build a stable equality key from the per-unit status +
		// progress fingerprint. Two consecutive polls with the same
		// fingerprint mean "no forward motion".
		fp := ""
		for _, u := range task.Units {
			fp += fmt.Sprintf("%s=%s/%.2f|", u.ID, u.Status, u.Progress)
		}
		return task.Status, fp, true
	}
	return "", "", false
}
