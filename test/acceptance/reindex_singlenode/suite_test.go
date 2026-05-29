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

// Package reindex_singlenode consolidates all single-node reindex acceptance
// tests into a single shared container. Each subtest uses a different collection
// name for isolation. One container restart at the end verifies all deferred
// finalizations together.
package reindex_singlenode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// TestSingleNode_ReindexSuite runs all single-node reindex tests on a shared
// container. Each subtest creates its own collection and runs its reindex
// operation. After all subtests complete, the container is restarted once to
// verify deferred finalization for all migrations.
func TestSingleNode_ReindexSuite(t *testing.T) {
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
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	// Collect shard names for post-restart filesystem checks.
	type shardInfo struct {
		className string
		shardName string
	}
	var shardInfos []shardInfo

	// --- Subtest 1: Map to Blockmax ---
	t.Run("MapToBlockmax", func(t *testing.T) {
		testBlockmaxMigration(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "ReindexTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"ReindexTest", sn})
	}

	// --- Subtest 2: Change Tokenization ---
	t.Run("ChangeTokenization", func(t *testing.T) {
		testChangeTokenization(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "RetokenizeTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"RetokenizeTest", sn})
	}

	// --- Subtest 3: Enable Rangeable ---
	t.Run("EnableRangeable", func(t *testing.T) {
		testEnableRangeable(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "EnableRangeableTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"EnableRangeableTest", sn})
	}

	// --- Subtest 4: Roaring Set Refresh ---
	t.Run("RoaringSetRefresh", func(t *testing.T) {
		testRoaringSetRefresh(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "RoaringSetRefreshTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"RoaringSetRefreshTest", sn})
	}

	// --- Subtest 5: Enable Filterable (from-scratch) ---
	t.Run("EnableFilterable", func(t *testing.T) {
		testEnableFilterable(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "EnableFilterableTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"EnableFilterableTest", sn})
	}

	// --- Subtest 6: Enable Searchable (from-scratch) ---
	t.Run("EnableSearchable", func(t *testing.T) {
		testEnableSearchable(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "EnableSearchableTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"EnableSearchableTest", sn})
	}

	// --- Subtest 7: Scope assertion (all six migration types) ---
	// Pins the blast radius of every migration type: each reindex must
	// stay inside the property named in the URL path. Uses its own
	// collections and cleans them up, so it runs after the correctness
	// tests above without interfering with the post-restart assertions.
	t.Run("ScopeAssertion", func(t *testing.T) {
		testReindexScopeAssertion(t, restURI)
	})

	// --- Subtest 8: API validation contract (no indexing involved) ---
	// Pins the HTTP contract for invalid / malformed requests: which
	// inputs must return 400, which must return 404, which 409. Uses
	// its own ephemeral collections and cleans them up. Cheap — no
	// indexing waits.
	t.Run("APIValidationContract", func(t *testing.T) {
		testReindexAPIValidation(t, restURI)
	})

	// --- Subtest 9: Cancel verb ---
	t.Run("CancelReindex", func(t *testing.T) {
		testCancelReindex(t, restURI)
	})

	// --- Subtest 10: Rangeable rebuild ---
	t.Run("RepairRangeable", func(t *testing.T) {
		testRepairRangeable(t, restURI)
	})

	// --- Subtest 11: DELETE-then-re-enable journey ---
	// Pins the journey: DELETE /properties/{prop}/index/{indexName} followed
	// by PUT enable-* must actually rebuild the bucket. Without the
	// migration-dir cleanup + the stale-sentinel defense, the second enable
	// short-circuits on the prior tidied sentinel, re-flips the schema flag
	// to true, and silently leaves the customer with an empty index.
	t.Run("DeleteThenReEnable", func(t *testing.T) {
		testDeleteThenReEnable(t, restURI)
	})

	// --- Subtest 12: CANCEL-then-retry journey ---
	// Structural sibling of DeleteThenReEnable on the CANCEL→retry axis.
	// Submits an enable-*, cancels it mid-flight, re-submits. The second
	// submit MUST actually build the index — not silently no-op on the
	// stale started.mig / progress.mig / partial __reindex sidecars left
	// behind by the cancelled run.
	t.Run("CancelThenRetry", func(t *testing.T) {
		testCancelThenRetry(t, restURI)
	})

	// --- Subtest 12b: change-tokenization on filterable-only ---
	// Frontend repro 2026-05-14: PUT {searchable:{tokenization:X}} on a
	// filterable-only text property 400'd with "searchable bucket not
	// found" and no alternative shape. The fix adds {filterable:{tokenization:X}}.
	t.Run("ChangeTokenizationFilterable", func(t *testing.T) {
		testChangeTokenizationFilterable(t, restURI)
	})

	// --- Subtest 13: DELETE→re-enable repeated 3x ---
	// Frontend repro 2026-05-14: after 3 enable→DELETE cycles on the same
	// property the 3rd enable finishes in 1.6s and the schema flag never
	// flips. Same Sev 1 family as DeleteThenReEnable but with state that
	// accumulates only past the first cycle. Pinning this here ensures
	// stale on-disk state never re-introduces the multi-round shape.
	t.Run("DeleteThenReEnableMultiCycle", func(t *testing.T) {
		testDeleteThenReEnableMultiCycle(t, restURI)
	})

	// --- Subtest 13b: GET /indexes bleed after DELETE→re-enable cycles ---
	// Frontend repro 2026-05-14 (https://github.com/weaviate/weaviate/issues/10675): after multiple
	// enable→DELETE cycles on the same property, GET /indexes shows a
	// phantom "indexing(1)" entry for the deleted index, "carrying over
	// from the previous FINISHED task". mergeReindexStatus's
	// finalize-window override reclassifies a stale FINISHED task as
	// "still finalizing" because it only gates on flagOn==false, not on
	// whether the swap that flipped the flag for THIS task has already
	// completed and been undone by DELETE.
	t.Run("DeleteThenReEnableIndexingBleed", func(t *testing.T) {
		testDeleteThenReEnableIndexingBleed(t, restURI)
	})

	// --- Subtest 13c: cycle-3 short-circuit after DELETE→re-enable ---
	// Frontend repro 2026-05-14 #10675: cycle 3's reindex finishes in
	// 1.6s while cycles 1 and 2 took 60-75s on the same dataset.
	// Combined with the schema flag never flipping to true on this
	// cycle, the silent-failure family is: iteration short-circuited
	// against stale on-disk or in-memory state, bucket is empty, BM25
	// errors with "indexSearchable not enabled". This test uses 500
	// objects so a short-circuit is unambiguously distinguishable from
	// a "fast because tiny dataset" cycle. Pins both the duration
	// ratio AND the BM25 hit count.
	t.Run("DeleteThenReEnableShortCircuit", func(t *testing.T) {
		testDeleteThenReEnableShortCircuit(t, restURI)
	})

	// --- Subtest 14: property-state × migration-type matrix ---
	// Programmatic enumeration of (data type × pre-state × PUT body shape).
	// Each cell asserts either (a) clean 202+FINISHED+queryable-bucket or
	// (b) 4xx with an actionable error. Cells classified as expectUnclear4xx
	// pin known dispatcher/validator bugs (e.g. filterable.tokenization
	// silently dropped, searchable.tokenization on filterable-only property
	// emits "searchable bucket not found"). They are intentionally RED. See
	// property_state_migration_matrix_test.go for the bug catalog.
	t.Run("PropertyStateMigrationMatrix", func(t *testing.T) {
		testPropertyStateMigrationMatrix(t, restURI)
	})

	// --- Subtest 15b: change-tok × DELETE journey class ---
	// Pins seven adjacent journeys around the new change-tokenization-filterable
	// migration (commit c98a3477ea) and how it interacts with the classic
	// change-tokenization (both indexes), DELETE, enable-*, cancel, and
	// back-to-back retokenizations. See
	// change_tok_delete_journeys_test.go for the journey catalog.
	t.Run("ChangeTokDeleteJourneys", func(t *testing.T) {
		testChangeTokDeleteJourneys(t, restURI)
	})

	// --- Subtest 15: torn "reindexed but not tidied" resume ---
	// Pins the journey where a prior reindex left the on-disk migration
	// in IsReindexed+!IsTidied state (real causes: I/O failure mid-
	// runtimeSwap, container kill between markReindexed and the first
	// swap step, etc.). The re-submit must NOT silently no-op on the
	// IsReindexed=true short-circuit in OnAfterLsmInitAsync; it must
	// either finish the swap or rebuild from scratch. If RED, schema
	// reports ready while queries return zero hits (Sev 1).
	t.Run("TornResumeReindexedNotTidied", func(t *testing.T) {
		testTornResumeReindexedNotTidied(t, compose)
	})

	// --- Shared restart: verify all deferred finalizations ---
	t.Run("PostRestartFinalize", func(t *testing.T) {
		t.Log("restarting weaviate container for deferred finalize verification")
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())

		// Post-restart: verify blockmax queries.
		testBlockmaxPostRestart(t)

		// Post-restart: verify tokenization queries.
		testChangeTokenizationPostRestart(t)

		// Post-restart: verify rangeable queries.
		testEnableRangeablePostRestart(t)

		// Post-restart: verify roaring set queries.
		testRoaringSetRefreshPostRestart(t)

		// Post-restart: verify enable-filterable / enable-searchable queries.
		testEnableFilterablePostRestart(t)
		testEnableSearchablePostRestart(t)

		// Post-restart: verify filesystem cleanup for all migrations.
		for _, si := range shardInfos {
			dirs := listLSMDirs(ctx, t, container, si.className, si.shardName)
			switch si.className {
			case "ReindexTest":
				assertNoSuffixedBuckets(t, dirs, "__blockmax_")
			case "RetokenizeTest":
				assertNoSuffixedBuckets(t, dirs, "__retokenize_")
				assertNoSuffixedBuckets(t, dirs, "__filt_retokenize_")
				assertBucketExists(t, dirs, "property_filepath_searchable")
				assertBucketExists(t, dirs, "property_description_searchable")
				assertBucketExists(t, dirs, "property_filepath")
				assertBucketExists(t, dirs, "property_description")
			case "EnableRangeableTest":
				assertNoSuffixedBuckets(t, dirs, "__rangeable_")
				assertBucketExists(t, dirs, "property_score_rangeable")
				assertBucketExists(t, dirs, "property_price_rangeable")
				assertBucketExists(t, dirs, "property_score")
				assertBucketExists(t, dirs, "property_price")
			case "RoaringSetRefreshTest":
				assertNoSuffixedBuckets(t, dirs, "__roaringset_")
			case "EnableFilterableTest":
				// Reindex/ingest staging dirs must not leak across restart.
				assertNoSuffixedBuckets(t, dirs, "__enable_filterable_")
				// The freshly-created filterable buckets must survive the
				// restart under their canonical names.
				assertBucketExists(t, dirs, "property_score")
				assertBucketExists(t, dirs, "property_available")
			case "EnableSearchableTest":
				assertNoSuffixedBuckets(t, dirs, "__enable_searchable_")
				assertBucketExists(t, dirs, "property_description_searchable")
			}
		}
	})
}

// =============================================================================
// Shared helpers
// =============================================================================
//
// HTTP-level reindex helpers (SubmitIndexUpdate, AwaitReindexFinished, etc.)
// live in test/acceptance/helpers/reindex and are used as
// `reindexhelpers.XYZ`. The helpers below are package-local because they
// reach into single-node–specific surfaces (LSM directory listing on a
// known shard, single-node GraphQL paths).

func getFirstShardName(t *testing.T, restURI, collection string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var nodesResp struct {
		Nodes []struct {
			Shards []struct {
				Class string `json:"class"`
				Name  string `json:"name"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &nodesResp))

	for _, node := range nodesResp.Nodes {
		for _, shard := range node.Shards {
			if shard.Class == collection {
				return shard.Name
			}
		}
	}
	return "" // not found — some subtests may not have created their collection yet
}

func runGraphQLQuery(t *testing.T, className, gqlQuery string) ([]string, error) {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors[0].Message)
	}
	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})
	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

func listLSMDirs(ctx context.Context, t *testing.T, c testcontainers.Container, col, shard string) []string {
	t.Helper()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	require.NoError(t, err, "exec ls on container")
	require.Equal(t, 0, code, "ls returned non-zero exit code")
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	var dirs []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
}

func assertNoSuffixedBuckets(t *testing.T, dirs []string, suffix string) {
	t.Helper()
	for _, d := range dirs {
		assert.False(t, strings.Contains(d, suffix),
			"unexpected leftover bucket directory: %s", d)
	}
}

func assertBucketExists(t *testing.T, dirs []string, bucketName string) {
	t.Helper()
	found := false
	for _, d := range dirs {
		if d == bucketName {
			found = true
			break
		}
	}
	assert.True(t, found, "expected bucket directory %q not found in %v", bucketName, dirs)
}
