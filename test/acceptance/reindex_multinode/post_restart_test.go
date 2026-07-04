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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_PostRestartMigration_NoStallPlateau asserts that a
// migration submitted after a rolling restart does not plateau at
// (N-1)/N progress for an indefinite period — the scheduler must
// resume picking up units on the restarted pod.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/212 (Issue B)
// — production reproduction (`1.38.0-dev-9c1591e.amd64`) was a
// post-restart change-tokenization task plateauing at 6/9 = 66.7%
// for 10+ minutes, all units on one weaviate-N pod stuck in PENDING
// because that pod's scheduler stopped picking up new
// distributed-task units after the rolling restart. Fixed in
// `b6a5c6bd` (retry path for context-canceled mid-swap) +
// `faa2c780ec` (cluster-wide conflict check in DTM AddTask).
//
// Current expected behaviour: the plateau briefly shows up (the user
// reported ~52s on `1.38.0-dev-e1dfae0`) but the retry path unblocks
// within a minute and the migration completes end-to-end in ~3
// minutes. This regression test asserts that the plateau remains
// brief — not that it's absent. If it persists longer than the
// budget below, the underlying scheduler-stall regression is back.
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

	createCollection(t, compose, compose.GetWeaviateNode(1).URI(), className, 3, 3, textProps("text"))
	// Re-resolve at defer time: rollingRestart replaces each container,
	// which testcontainers reallocates ports for. Capturing a URL at
	// defer-registration time would bake in a pre-restart port and the
	// cleanup DELETE would race a "connection refused".
	defer func() {
		deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)
	}()

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)

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
	//
	// Re-resolve the node-1 URI from the compose handle on every API
	// call: testcontainers reallocates ports across the
	// stop+start in restartCluster, so pre-restart URIs are stale.
	restURI := compose.GetWeaviateNode(1).URI()
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text",
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

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
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
		<-ticker.C
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

// TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas
// asserts that re-applying migrations after a rolling restart
// produces exact, agreeing counts across every replica — no empty
// buckets after the post-restart shard-init resumes the iteration.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/212 (Issue G)
// — production reproduction (`1.38.0-dev-3e78517`) was a 1M-record
// demo where Phase 8-final returned `path = 0 / 0 / 0` across all 3
// LB calls after forward migrations → rolling restart → reset →
// re-apply migrations. The searchable bucket was deterministically
// empty on every replica. Same FlushAndSwitch + CursorOnDisk race
// family as Issues C + D (see
// TestMultiNode_ConcurrentDifferentMigrations_ExactCountsPostSettle),
// resurfacing on the post-restart re-apply because the OnAfterLsmInitAsync
// resume path uses the same uuidObjectsIteratorAsync. Fixed by the
// Cursor() / flushAndSwitchMu pair (commits `1dba6d4718` +
// `5874ee8dde`).
//
// Shape:
//
//  1. Create class with `price` (int, rangeable off), `category`
//     (text, filterable off), `path` (text, tokenization=word).
//  2. Import 10k objects with consistency_level=ALL.
//  3. Run forward migrations: enable-rangeable + enable-filterable
//     + change-tokenization (word → field). All 3 in parallel,
//     await FINISHED.
//  4. Rolling restart (one node at a time, wait for ready between
//     each).
//  5. Re-apply forward migrations: change-tokenization (field → word)
//     in parallel with two NO-OP rebuilds (filterable.rebuild +
//     rangeable.rebuild) that share the same FlushAndSwitch +
//     iterator path as enable-* but don't require pre-disabling.
//  6. After settle, assert every replica's counts equal baseline.
//
// A failure here would mean the post-restart re-apply has a separate
// bug beyond the FlushAndSwitch race — likely something restart-
// specific in the recovery / FinalizeCompletedMigrations path.
//
// Shape:
//
//  1. Create class with `price` (int, rangeable off), `category`
//     (text, filterable off), `path` (text, tokenization=word).
//  2. Import 10k objects with consistency_level=ALL.
//  3. Run forward migrations: enable-rangeable + enable-filterable
//     + change-tokenization (word → field). All 3 in parallel,
//     await FINISHED.
//  4. Rolling restart (one node at a time, wait for ready between
//     each).
//  5. Re-apply forward migrations: change-tokenization (field → word)
//     in parallel with two NO-OP rebuilds (filterable.rebuild +
//     rangeable.rebuild) that share the same FlushAndSwitch +
//     iterator path as enable-* but don't require pre-disabling.
//  6. After settle, assert every replica's counts equal baseline.
//
// A failure here with my Cursor() fix already in place would mean the
// post-restart re-apply has a separate bug beyond the FlushAndSwitch
// race — likely something restart-specific in the recovery /
// FinalizeCompletedMigrations path.
func TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "PostRestartReapply"
	const totalObjects = 10_000

	trueVal, falseVal := true, false
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:              "price",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
		{
			Name:            "category",
			DataType:        []string{"text"},
			IndexFilterable: &falseVal,
			Tokenization:    "word",
		},
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	// Defer-resolved URI: rolling restart reallocates ports.
	defer func() {
		deleteCollection(t, restURIOf(compose, 1), className)
	}()

	const (
		priceLo            = 3000
		priceHi            = 4000
		expectedPriceCount = 501
		expectedCatCount   = 2500
		expectedPathCount  = 2000
	)
	categories := []string{"alpha", "beta", "gamma", "delta"}
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"price":    i * 2,
			"category": categories[i%len(categories)],
			"path":     paths[i%len(paths)],
		}
	})

	// === Phase 2 equivalent: forward migrations (the same shape as the
	// passing concurrent test). The Cursor() fix already guarantees
	// these settle correctly; we run them here as a precondition so the
	// rolling restart in Phase 4 has something interesting to recover.
	uri1 := restURIOf(compose, 1)
	{
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "price",
				`{"rangeable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "category",
				`{"filterable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tk = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "path",
				`{"searchable":{"tokenization":"field"}}`)
		}()
		wg.Wait()
		reindexhelpers.AwaitReindexFinished(t, uri1, tp, reindexhelpers.WithTimeout(180*time.Second))
		reindexhelpers.AwaitReindexFinished(t, uri1, tc, reindexhelpers.WithTimeout(180*time.Second))
		reindexhelpers.AwaitReindexFinished(t, uri1, tk, reindexhelpers.WithTimeout(180*time.Second))
	}

	// Pre-restart sanity: poll every replica until each returns baseline.
	// AwaitReindexFinished only confirms node-1; a replica's swap can lag,
	// so poll (50ms) instead of a fixed settle.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			uri := restURIOf(compose, nodeIdx)
			gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
			assert.NoError(c, err)
			assert.Equalf(c, expectedPriceCount, gotPrice,
				"pre-restart node %d price = %d (expected %d)", nodeIdx, gotPrice, expectedPriceCount)
			gotCat, err := equalCount(uri, className, "category", categories[0])
			assert.NoError(c, err)
			assert.Equalf(c, expectedCatCount, gotCat,
				"pre-restart node %d category = %d (expected %d)", nodeIdx, gotCat, expectedCatCount)
			gotPath, err := equalCount(uri, className, "path", paths[0])
			assert.NoError(c, err)
			assert.Equalf(c, expectedPathCount, gotPath,
				"pre-restart node %d path = %d (expected %d)", nodeIdx, gotPath, expectedPathCount)
		}
	}, 30*time.Second, 50*time.Millisecond)

	// === Phase 7 equivalent: rolling restart.
	t.Log("rolling restart")
	restartCluster(ctx, t, compose)

	// After restart re-resolve every URI on use; testcontainers
	// reallocates ports across stop+start.

	// Post-restart baseline: every replica still serves the
	// already-migrated buckets correctly.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		require.Equalf(t, expectedPriceCount, gotPrice,
			"post-restart node %d price = %d (expected %d) — restart corrupted the rangeable bucket", nodeIdx, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(uri, className, "category", categories[0])
		require.NoError(t, err)
		require.Equalf(t, expectedCatCount, gotCat,
			"post-restart node %d category = %d (expected %d) — restart corrupted the filterable bucket", nodeIdx, gotCat, expectedCatCount)
		gotPath, err := equalCount(uri, className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, gotPath,
			"post-restart node %d path = %d (expected %d) — restart corrupted the searchable bucket", nodeIdx, gotPath, expectedPathCount)
	}

	// === Phase 8-final equivalent: re-apply migrations as a
	// repeat-forward. Use three concurrent rebuild-style ops on the
	// already-migrated indexes: change-tokenization back-and-forth
	// (field → word for path), plus rangeable rebuild and filterable
	// rebuild on the other two props. All three go through the same
	// OnAfterLsmInitAsync iterator path that #212 Issues C/D/G hit.
	t.Log("submitting post-restart re-apply migrations (3 concurrent)")
	uri1 = restURIOf(compose, 1)
	{
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "price",
				`{"rangeable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "category",
				`{"filterable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			// Flip tokenization back to word (the pre-Phase-2 value).
			// This matches the migration shape from the original
			// production-scale repro.
			tk = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "path",
				`{"searchable":{"tokenization":"word"}}`)
		}()
		wg.Wait()
		t.Logf("submitted post-restart re-apply migrations: price=%s category=%s path=%s",
			tp, tc, tk)
		reindexhelpers.AwaitReindexFinished(t, uri1, tp, reindexhelpers.WithTimeout(180*time.Second))
		reindexhelpers.AwaitReindexFinished(t, uri1, tc, reindexhelpers.WithTimeout(180*time.Second))
		reindexhelpers.AwaitReindexFinished(t, uri1, tk, reindexhelpers.WithTimeout(180*time.Second))
	}

	// Final per-replica counts. The path query is the headline check for
	// Issue G (Frontend Claude saw `0 / 0 / 0` here). AwaitReindexFinished
	// only confirms node-1; poll all replicas (50ms) until convergence
	// instead of a fixed settle — a node stuck at the 0/0/0 shape never
	// converges and fails here loudly.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			uri := restURIOf(compose, nodeIdx)
			gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
			assert.NoError(c, err, "post-reapply price query node %d", nodeIdx)
			assert.Equalf(c, expectedPriceCount, gotPrice,
				"GH #212 Issue G regression: post-restart re-apply node %d price = %d (expected %d) — rangeable rebuild lost data after restart",
				nodeIdx, gotPrice, expectedPriceCount)

			gotCat, err := equalCount(uri, className, "category", categories[0])
			assert.NoError(c, err, "post-reapply category query node %d", nodeIdx)
			assert.Equalf(c, expectedCatCount, gotCat,
				"GH #212 Issue G regression: post-restart re-apply node %d category = %d (expected %d) — filterable rebuild lost data after restart",
				nodeIdx, gotCat, expectedCatCount)

			gotPath, err := equalCount(uri, className, "path", paths[0])
			assert.NoError(c, err, "post-reapply path query node %d", nodeIdx)
			assert.Equalf(c, expectedPathCount, gotPath,
				"GH #212 Issue G regression: post-restart re-apply node %d path = %d (expected %d) — change-tokenization lost data after restart (Phase-8-final shape)",
				nodeIdx, gotPath, expectedPathCount)
		}
	}, 30*time.Second, 50*time.Millisecond)

	// LB-side 3-call stability spot check.
	for i := 0; i < 3; i++ {
		gotPath, err := equalCount(restURIOf(compose, 1), className, "path", paths[0])
		require.NoError(t, err)
		assert.Equalf(t, expectedPathCount, gotPath,
			"GH #212 Issue G regression: post-restart LB-side path call #%d = %d (expected %d)",
			i+1, gotPath, expectedPathCount)
	}
}
