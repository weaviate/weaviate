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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestRestartDuringSwap verifies that a NEW write submitted between the moment
// the reindex unit is COMPLETED (and the container is restarted) and the moment
// OnGroupCompleted fires the swap on the next scheduler tick is NOT lost.
//
// The concern: ReindexProvider keeps a per-task cache of
// *ShardReindexTaskGeneric instances. These instances hold the registered
// double-write callbacks (OnAfterLsmInit registers them via
// registerDoubleWriteCallbacks). If Weaviate restarts after the reindex unit
// is COMPLETED but before OnGroupCompleted fires:
//
//   - The cache is empty (only populated in StartTask / processOneUnit).
//   - The scheduler does NOT call StartTask for terminal-units tasks
//     (scheduler.go: filterStartedTasks requires NodeHasNonTerminalUnits).
//   - OnGroupCompleted falls back to createReindexTasks → fresh task instances
//     with NO double-write callbacks registered.
//   - Writes during the swap-recovery window therefore go ONLY to the OLD main
//     bucket. After the swap replaces the main bucket with the ingest bucket,
//     those writes are lost.
//
// Test strategy:
//  1. Create a collection with tokenization=word.
//  2. Insert baseline objects.
//  3. Submit a change-tokenization reindex (word → field).
//  4. Poll /v1/tasks until the task is FINISHED (RAFT-level: all units
//     terminal). At this moment the swap has likely NOT yet completed
//     (see TestSingleNode_FinishedStatusRaceWithSchemaFlag — the swap
//     fires on the next scheduler tick, up to 1s later).
//  5. Stop the container immediately (SIGKILL via 0 timeout).
//  6. Start the container again.
//  7. As soon as the container is ready, write a NEW object with a unique
//     marker that requires FIELD tokenization to be retrievable as a single
//     token.
//  8. Wait for the schema tokenization to flip to "field" (swap complete).
//  9. Query with field-tokenization-style BM25 and EQUAL filter — the new
//     object must appear in the new bucket.
//
// If this test PASSES, the cache-loss is harmless (some other recovery path
// re-registers callbacks, e.g. via OnAfterLsmInit during shard startup).
// If this test FAILS, the cache-loss is a real bug: writes during the
// swap-recovery window are not double-written and are lost on swap.
func TestRestartDuringSwap(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		// 1s tick gives us up to ~1s after FINISHED is reported in RAFT
		// before OnGroupCompleted fires — comfortably wider than the time
		// needed to issue StopAt(... 0 timeout) and kill the container.
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
	container := compose.GetWeaviate().Container()

	// Dump container logs on failure.
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			// Filter for lines mentioning anything reindex/swap/migration related.
			var filtered []string
			for _, line := range strings.Split(string(logs), "\n") {
				lower := strings.ToLower(line)
				if strings.Contains(lower, "received http request") {
					continue
				}
				if strings.Contains(lower, "server.query") {
					continue
				}
				if strings.Contains(lower, "reindex") ||
					strings.Contains(lower, "swap") ||
					strings.Contains(lower, "migration") ||
					strings.Contains(lower, "distributed task") ||
					strings.Contains(lower, "ongroupcomp") ||
					strings.Contains(lower, "ontaskcomp") ||
					strings.Contains(lower, "ingest") ||
					strings.Contains(lower, "tokeniz") ||
					strings.Contains(lower, "shard ") ||
					strings.Contains(lower, "fallback") ||
					strings.Contains(lower, "callback") ||
					strings.Contains(lower, "finalize") ||
					strings.Contains(lower, "tidied") ||
					strings.Contains(lower, "prepended") ||
					strings.Contains(lower, "starting") ||
					strings.Contains(lower, "\"error\"") ||
					strings.Contains(lower, "\"warning\"") {
					filtered = append(filtered, line)
				}
			}
			t.Logf("=== Container logs (filtered, %d lines) ===\n%s",
				len(filtered), strings.Join(filtered, "\n"))
		}
	}()

	const className = "RestartDuringSwapTest"

	// Step 1: create collection with word tokenization.
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "description", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	// Step 2: insert baseline objects. Keep the count modest — the reindex
	// itself does not need to be slow for the test to work. Once unit
	// completion is in RAFT, the swap is deferred to the next tick.
	for i := 0; i < 20; i++ {
		obj := &models.Object{Class: className, Properties: map[string]interface{}{
			"description": fmt.Sprintf("baseline document number %d", i),
		}}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	// Step 3: submit the change-tokenization reindex.
	taskID := submitIndexUpdate(t, restURI, className, "description",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Step 4: poll /v1/tasks every ~20ms until FINISHED.
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
	require.False(t, sawFinishedAt.IsZero(), "task never reached FINISHED before deadline")
	t.Logf("task observed FINISHED at %v — initiating immediate container stop", sawFinishedAt)

	// Check schema right after FINISHED — if tokenization is already "field"
	// the swap already ran and we lost the race. We still proceed to test
	// the write path, but log it for diagnosis.
	preStopTokenization := getTokenization(t, className, "description")
	t.Logf("pre-stop tokenization (right after FINISHED): %q", preStopTokenization)

	// Step 5: stop the container immediately with 0 timeout (SIGKILL).
	zeroTimeout := time.Duration(0)
	require.NoError(t, compose.StopAt(ctx, 0, &zeroTimeout))
	t.Logf("container stopped")

	// Step 6: restart the container.
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	restURI = compose.GetWeaviate().URI()
	t.Logf("container restarted at %v (elapsed since stop trigger: %v)",
		time.Now(), time.Since(sawFinishedAt))

	// Step 7: write the NEW object IMMEDIATELY after the container is ready.
	// This is the race we are targeting: the OnGroupCompleted swap fires on
	// the next scheduler tick (1s after the scheduler's loop started post-
	// restart). We want to land the write before that tick.
	const marker = "uniquemarkerxyz12345restartduringswap"
	newObj := &models.Object{Class: className, Properties: map[string]interface{}{
		"description": marker,
	}}
	writeStart := time.Now()
	// Use a retrying CreateObject — the server may need a moment to fully
	// initialize the schema reader. But each attempt should be cheap.
	require.Eventually(t, func() bool {
		err := helper.CreateObject(t, newObj)
		if err == nil {
			return true
		}
		t.Logf("write attempt failed: %v (will retry)", err)
		return false
	}, 10*time.Second, 50*time.Millisecond, "post-restart write should eventually succeed")
	t.Logf("new object written at %v (took %v after restart)", time.Now(), time.Since(writeStart))

	// Step 8: wait for the schema tokenization to flip to "field"
	// (OnGroupCompleted has fired and the swap has completed).
	tokenizationFlipped := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		if getTokenization(t, className, "description") == "field" {
			tokenizationFlipped = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !tokenizationFlipped {
		// Diagnostic: dump the shard's lsm + .migrations dir so we can see
		// the on-disk state when the swap fails to fire.
		dumpShardState(ctx, t, container, className)
		t.Errorf("tokenization should eventually flip to field post-restart but stayed %q",
			getTokenization(t, className, "description"))
	} else {
		t.Logf("post-restart: tokenization flipped to field")
	}

	// Sanity: task should still be reported as FINISHED.
	finalStatus, err := fetchTaskStatus(restURI, taskID)
	require.NoError(t, err)
	t.Logf("post-restart task status: %q", finalStatus)

	// Step 9: query with FIELD-tokenization-style queries. The new object
	// must be retrievable via the (now active) field-tokenized bucket.
	//
	// BM25 with the full marker as a single token should match.
	bm25IDs := retokenizeBM25Query(t, "description", marker)
	assert.NotEmpty(t, bm25IDs,
		"post-restart BM25(%q) returned no results — write during swap-recovery window was lost",
		marker)

	// EQUAL filter with the full marker should match.
	equalIDs := retokenizeFilterQuery(t, "description", "Equal", marker)
	assert.NotEmpty(t, equalIDs,
		"post-restart Equal(description, %q) returned no results — write during swap-recovery window was lost",
		marker)
}

// dumpShardState lists the LSM dir, .migrations dir, and the sentinel files
// inside it for the first shard of the given class. Used as a diagnostic aid
// when the swap fails to fire.
func dumpShardState(ctx context.Context, t *testing.T, c testcontainers.Container, className string) {
	t.Helper()
	lsmGlob := fmt.Sprintf("/data/%s/", strings.ToLower(className))
	for _, cmd := range [][]string{
		{"sh", "-c", "ls -la " + lsmGlob},
		{"sh", "-c", "find " + lsmGlob + " -maxdepth 6 -type d -printf '%p\\n' 2>/dev/null | head -50"},
		{"sh", "-c", "find " + lsmGlob + " -maxdepth 6 -name '*.mig' -printf '%p\\n' 2>/dev/null"},
	} {
		code, reader, err := c.Exec(ctx, cmd)
		if err != nil {
			t.Logf("exec %v error: %v", cmd, err)
			continue
		}
		out, _ := io.ReadAll(reader)
		t.Logf("--- exec (code=%d) %v ---\n%s", code, cmd, string(out))
	}
}

// getTokenization returns the tokenization of the named property, or "" if not found.
func getTokenization(t *testing.T, className, propName string) string {
	t.Helper()
	cls := helper.GetClass(t, className)
	if cls == nil {
		return ""
	}
	for _, p := range cls.Properties {
		if p.Name == propName {
			return p.Tokenization
		}
	}
	return ""
}
