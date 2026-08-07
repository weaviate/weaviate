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
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestRestartAfterSwapCompletes SIGKILLs the node the moment a migration is
// reported FINISHED, then writes and queries. FINISHED lands strictly after
// every node has acked its bucket swap (MarkTaskFinalized is proposed only
// then, pinned by TestSingleNode_FinishedStatusRaceWithSchemaFlag), so the kill
// point is the completion boundary and the write below lands in the new bucket.
//
// SCOPE: this is not the swap-recovery window. That window — restart after the
// units are COMPLETED but before OnGroupCompleted fires — cannot be reached
// from outside the process. The FSM wakes the scheduler on the unit-completion
// apply instead of waiting for the next tick, so PREPARING and SWAPPING pass in
// a few milliseconds; a 20 ms poll of /v1/tasks across a full run never
// observed either. Reaching it needs an in-process hook that holds the swap
// open.
//
// Inside that window a write would be lost: ReindexProvider's per-task cache of
// *ShardReindexTaskGeneric holds the double-write callbacks, the cache is empty
// after a restart, the scheduler does not re-StartTask a task whose units are
// terminal, and OnGroupCompleted then builds fresh task instances with no
// callbacks registered. The write goes only to the old main bucket, which the
// swap replaces. Tracked as https://github.com/weaviate/weaviate/issues/10675;
// the fix is to register in-flight runtime tasks with the static
// ShardReindexerV3 at startup so OnAfterLsmInit fires during shard load. The
// static reindexer is NewShardReindexerV3Noop today, so that hook does not
// fire.
func TestRestartAfterSwapCompletes(t *testing.T) {
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
	// itself does not need to be slow for the test to work.
	for i := 0; i < 20; i++ {
		obj := &models.Object{Class: className, Properties: map[string]interface{}{
			"description": fmt.Sprintf("baseline document number %d", i),
		}}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	// Step 3: submit the change-tokenization reindex.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "description",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Step 4: poll /v1/tasks until FINISHED, then kill on that boundary. See
	// the SCOPE note above for why this is the completion boundary and not
	// the swap-recovery window.
	var killAt time.Time
	require.Eventually(t, func() bool {
		status, err := fetchTaskStatus(restURI, taskID)
		require.NoError(t, err)
		if status == "FAILED" {
			t.Fatalf("task FAILED before reaching FINISHED")
		}
		if status == "FINISHED" {
			killAt = time.Now()
			return true
		}
		return false
	}, 120*time.Second, 20*time.Millisecond)
	require.False(t, killAt.IsZero(), "task never reached FINISHED before deadline")
	t.Logf("task observed FINISHED at %v — initiating immediate container stop", killAt)

	// Step 5: stop the container immediately with 0 timeout (SIGKILL).
	zeroTimeout := time.Duration(0)
	require.NoError(t, compose.StopAt(ctx, 0, &zeroTimeout))
	t.Logf("container stopped")

	// Step 6: restart the container.
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	restURI = compose.GetWeaviate().URI()
	t.Logf("container restarted at %v (elapsed since stop trigger: %v)",
		time.Now(), time.Since(killAt))

	// Step 7: write the NEW object as soon as the container is ready. The swap
	// is already done, so this write has to survive on its own merits — it is
	// the post-restart write path being exercised, not a race.
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
	tokenizationFlipped := assert.Eventually(t, func() bool {
		return getTokenization(t, className, "description") == "field"
	}, 15*time.Second, 50*time.Millisecond)
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

	// Step 9: query with FIELD-tokenization-style queries. The swap ran before
	// the kill, so the write above belongs in the new bucket and both queries
	// must find it.
	t.Run("PostRestartWriteIsQueryableInTheNewBucket", func(t *testing.T) {
		bm25IDs := restartSwapBM25Query(t, className, "description", marker)
		assert.NotEmpty(t, bm25IDs,
			"post-restart BM25(%q) returned no results — the write did not land in the new bucket",
			marker)
		equalIDs := restartSwapFilterQuery(t, className, "description", "Equal", marker)
		assert.NotEmpty(t, equalIDs,
			"post-restart Equal(description, %q) returned no results — the write did not land in the new bucket",
			marker)
	})
}

// restartSwapBM25Query runs a BM25 query against an arbitrary class. The
// existing retokenizeBM25Query helper hardcodes the retokenize class name; we
// need our own.
func restartSwapBM25Query(t *testing.T, className, property, query string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}) {
				description
				_additional { id }
			}
		}
	}`, className, query, property)
	ids, err := runGraphQLQuery(t, className, gqlQuery)
	require.NoError(t, err)
	return ids
}

func restartSwapFilterQuery(t *testing.T, className, property, operator, value string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {operator: %s, path: [%q], valueText: %q}) {
				description
				_additional { id }
			}
		}
	}`, className, operator, property, value)
	ids, err := runGraphQLQuery(t, className, gqlQuery)
	require.NoError(t, err)
	return ids
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
