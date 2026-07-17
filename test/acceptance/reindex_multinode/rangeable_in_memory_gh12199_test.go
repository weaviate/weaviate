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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// This file is the GH#12199 regression suite: with INDEX_RANGEABLE_IN_MEMORY=true,
// an enable-rangeable reindex must not serve empty range results from an
// unpopulated in-memory representation after the per-shard swap (no restart
// required). It also pins the two operator log signals added by the fix.
//
// Log substrings emitted by the fix (bucket_roaring_set_range.go):
//   - deferredINFOSubstr: fires once per bucket-open on the marked ingest
//     bucket's first disk-path read; presence confirms (b)'s marker path is
//     applied.
//   - fallbackWARNSubstr: the (c) disk-fallback WARN. Must NOT fire on the
//     fixed path (serving bucket has keepSegmentsInMemory=false).
//
// Per-mechanism revert-probe mapping:
//   - (b) alone       -> caught by the prepend GUARD, not the WARN/INFO
//     assertions: reverting only (b) leaves keepSegmentsInMemory=true on the
//     ingest bucket, so the guard rejects the backfill prepend and
//     AwaitReindexFinished fails with INV-RANGEABLE-REP-EQUALS-DISK before any
//     post-swap query runs.
//   - WARN/INFO pair  -> the WARN-absence + INFO-presence assertions serve two
//     roles: (i) proof (b) is applied at the fix SHA, and (ii) a tripwire for a
//     (b) AND guard DOUBLE revert - only then does the reindex complete with an
//     empty rep served, so the WARN appears and the INFO (marker unset)
//     disappears. A counts-only probe can't catch that double revert; these
//     assertions can.
//   - (c) alone       -> the Layer-1 four-case disk-fallback matrix (empty rep
//     and disk segments must fall back).
//   - guard alone     -> the prepend guard's own unit test (active-rep prepend
//     errors).
//   - option-(a) trap -> the fold-order value-integrity unit test.
const (
	deferredINFOSubstr = "in-memory acceleration deferred until the shard is reloaded"
	fallbackWARNSubstr = "rangeable in-memory index is empty"
)

// countInLogs returns the number of lines in a container's logs that contain
// substr. Used to assert presence/absence of the two weaviate/weaviate#12199
// log signals.
func countInLogs(ctx context.Context, t *testing.T, c interface {
	Logs(context.Context) (io.ReadCloser, error)
}, substr string,
) int {
	t.Helper()
	reader, err := c.Logs(ctx)
	require.NoError(t, err, "reading container logs")
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err, "draining container logs")
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// startSingleNodeRangeableInMemCluster starts a 1-node cluster with the in-mem
// rangeable knob on (GH#12199 precondition) and LOG_LEVEL=info so the deferred
// INFO / fallback WARN are visible in the logs the assertions grep.
func startSingleNodeRangeableInMemCluster(ctx context.Context, t *testing.T) (*docker.DockerCompose, func()) {
	t.Helper()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("INDEX_RANGEABLE_IN_MEMORY", "true").
		WithWeaviateEnv("LOG_LEVEL", "info").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	return compose, func() { require.NoError(t, compose.Terminate(ctx)) }
}

// scoreForGH12199 maps import index i to a score in [10_000, 60_000) step 5, so
// [30_000, 40_000] selects exactly 2001/10_000 objects (mirrors
// in_flight_rangeable_test.go; a half-built bucket returns a visibly wrong count).
func scoreForGH12199(i int) int { return 10_000 + i*5 }

const (
	gh12199Total    = 10_000
	gh12199RangeLo  = 30_000
	gh12199RangeHi  = 40_000
	gh12199Baseline = 2001
)

func rangeableScoreProps() []*models.Property {
	trueVal, falseVal := true, false
	return []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:              "score",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
	}
}

// TestRangeableInMemory_GH12199_SingleNode_AcceptanceAndRestartHandoff is the
// core acceptance repro (single-node, knob on, NO restart) with the triple
// assertion, followed by the post-restart in-memory hand-off case.
func TestRangeableInMemory_GH12199_SingleNode_AcceptanceAndRestartHandoff(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := startSingleNodeRangeableInMemCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	const className = "RangeableInMemAcceptance"
	// 3 shards on the single node => 3 rangeable ingest buckets, each marked
	// deferred, so the INFO fires per bucket.
	createCollection(t, compose, restURI, className, 3, 1, rangeableScoreProps())
	// Read the URI lazily at cleanup time: the restart below re-maps the host
	// port, so a URI captured now would be stale by the time this defer runs.
	defer func() { deleteCollection(t, compose.GetWeaviate().URI(), className) }()
	batchImportNumeric(t, restURI, className, gh12199Total, scoreForGH12199)

	// Golden baseline from the filterable path, before the migration.
	golden, err := rangeCount(restURI, className, "score", gh12199RangeLo, gh12199RangeHi)
	require.NoError(t, err)
	require.Equal(t, gh12199Baseline, golden, "pre-migration filterable baseline")

	// NO restart before the checks below - that's the core repro condition.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))

	// (1) Counts correct post-swap without restart (pre-fix served 0: empty
	// in-memory rep), (2) the (c) WARN must not fire on the fixed path
	// (serving bucket has keepSegmentsInMemory=false) - see the revert-probe
	// mapping above. Poll briefly for the per-shard swap + schema flip to
	// settle.
	awaitRangeCountSettledNoFallback(ctx, t, container, restURI, className,
		gh12199RangeLo, gh12199RangeHi, gh12199Baseline,
		"weaviate/weaviate#12199: post-swap range count must equal golden %d WITHOUT restart", gh12199Baseline,
		"(c) disk-fallback WARN must not fire on the fixed path (WARN present => (b) AND guard both reverted, empty rep served, weaviate/weaviate#12199)")

	// (3) The deferred-serving INFO must be present - only the (b) marker path
	// sets the marker (see revert-probe mapping above).
	assert.Positive(t, countInLogs(ctx, t, container, deferredINFOSubstr),
		"deferred-serving INFO must fire post-swap (marker set + first disk read); absence => (b) marker not applied, weaviate/weaviate#12199")

	// --- Post-restart in-memory hand-off case ---
	// After restart, boot-time population rebuilds the in-memory rep from disk,
	// so reads serve from the rep again and no (c) WARN fires (rep populated).
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	restURI = compose.GetWeaviate().URI()
	container = compose.GetWeaviate().Container()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/.well-known/ready", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 90*time.Second, 200*time.Millisecond, "node must be ready after restart")

	awaitRangeCountSettledNoFallback(ctx, t, container, restURI, className,
		gh12199RangeLo, gh12199RangeHi, gh12199Baseline,
		"post-restart range count must still equal golden %d (rep rebuilt at boot)", gh12199Baseline,
		"no (c) WARN after restart: the rep is rebuilt at boot and serves in-memory")
}

// TestRangeableInMemory_GH12199_MultiNode_InFlightStatisticalProbe is a 3-node
// statistical probe: fire range queries at every node throughout the migration
// window and count divergences, not single-shot - silent wrong results, not
// errors, are the failure mode.
func TestRangeableInMemory_GH12199_MultiNode_InFlightStatisticalProbe(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t,
		"INDEX_RANGEABLE_IN_MEMORY", "true", "LOG_LEVEL", "info")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RangeableInMemInFlight"
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, rangeableScoreProps())
	defer deleteCollection(t, restURIOf(compose, 1), className)
	batchImportNumeric(t, restURIOf(compose, 1), className, gh12199Total, scoreForGH12199)

	// Every replica agrees on the baseline before the migration.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		c, err := rangeCount(restURIOf(compose, nodeIdx), className, "score", gh12199RangeLo, gh12199RangeHi)
		require.NoError(t, err, "pre-migration baseline on node %d", nodeIdx)
		require.Equal(t, gh12199Baseline, c, "node %d baseline", nodeIdx)
	}

	counters, stopCh, wg := startRangeCountPolling(compose, className,
		gh12199RangeLo, gh12199RangeHi, gh12199Baseline, 3,
		func(nodeIdx, got int) {
			t.Logf("weaviate/weaviate#12199 in-flight divergence: node %d returned %d (expected %d)", nodeIdx, got, gh12199Baseline)
		}, nil)

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Keep probing until every replica converges to the correct rangeable count.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			c, err := rangeCount(restURIOf(compose, nodeIdx), className, "score", gh12199RangeLo, gh12199RangeHi)
			if err != nil || c != gh12199Baseline {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond, "all replicas must converge to %d", gh12199Baseline)
	close(stopCh)
	wg.Wait()

	t.Logf("in-flight probes: %d runs, %d transient errors, %d wrong counts",
		counters.queryRuns.Load(), counters.queryErrors.Load(), counters.wrongCounts.Load())

	// The schema flip committed on every node.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		cls := getClassFromNode(t, restURIOf(compose, nodeIdx), className)
		ok := false
		for _, prop := range cls.Properties {
			if prop.Name == "score" && prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				ok = true
				break
			}
		}
		assert.True(t, ok, "node %d: IndexRangeFilters should be true", nodeIdx)
	}

	assert.Zero(t, counters.wrongCounts.Load(),
		"weaviate/weaviate#12199: zero silent-wrong range counts during the in-flight enable-rangeable "+
			"window with INDEX_RANGEABLE_IN_MEMORY=true. A non-zero count means a replica "+
			"served from an empty in-memory rep instead of the disk path.")
}

// TestRangeableInMemory_GH12199_WriteWorkloadValueIntegrity migrates under
// concurrent live writes that change movers' value, then asserts a mover's NEW
// value query returns it and its OLD value query returns nothing - catches both
// fold-order corruption (option (a)) and the empty-rep bug (movers dropped).
func TestRangeableInMemory_GH12199_WriteWorkloadValueIntegrity(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := startSingleNodeRangeableInMemCluster(ctx, t)
	defer cleanup()
	restURI := compose.GetWeaviate().URI()

	const className = "RangeableInMemWriteWorkload"
	// Single shard keeps the mover UUIDs deterministic and the value assertions
	// simple; the value-integrity property is per-shard.
	createCollection(t, compose, restURI, className, 1, 1, rangeableScoreProps())
	defer deleteCollection(t, restURI, className)

	const (
		total      = 4_000
		moverCount = 500
		oldValue   = 20_000 // movers start here (in the [oldLo,oldHi] band)
		newValue   = 55_000 // movers are updated to here mid-migration
		stableVal  = 12_345 // non-movers; must never change
	)
	// Deterministic UUIDs so we can overwrite the movers by re-importing them.
	moverID := func(i int) string {
		return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("gh12199-mover-%d", i))).String()
	}

	// Seed: movers at oldValue, the rest at stableVal.
	seed := make([]map[string]interface{}, 0, total)
	for i := 0; i < total; i++ {
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("gh12199-obj-%d", i))).String()
		score := stableVal
		if i < moverCount {
			id = moverID(i)
			score = oldValue
		}
		seed = append(seed, map[string]interface{}{
			"class":      className,
			"id":         id,
			"properties": map[string]interface{}{"name": fmt.Sprintf("item-%d", i), "score": score},
		})
	}
	postObjects(t, restURI, seed)

	// Baseline: exactly moverCount objects have score == oldValue.
	c, err := rangeCount(restURI, className, "score", oldValue, oldValue)
	require.NoError(t, err)
	require.Equal(t, moverCount, c, "pre-migration movers at oldValue")

	// Start the migration, then wait for it to reach a live state before
	// overwriting the movers. SubmitIndexUpdate is asynchronous: posting the
	// updates immediately risks them landing before the task has even started,
	// which would let the backfill simply read newValue and pass without
	// exercising the double-write / fold-order scenario this test targets.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID)

	updates := make([]map[string]interface{}, 0, moverCount)
	for i := 0; i < moverCount; i++ {
		updates = append(updates, map[string]interface{}{
			"class":      className,
			"id":         moverID(i),
			"properties": map[string]interface{}{"name": fmt.Sprintf("item-%d", i), "score": newValue},
		})
	}
	postObjects(t, restURI, updates) // overwrite by same UUID, in-flight
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Post-swap value integrity, no restart. Poll for settle.
	require.Eventually(t, func() bool {
		atNew, e1 := rangeCount(restURI, className, "score", newValue, newValue)
		atOld, e2 := rangeCount(restURI, className, "score", oldValue, oldValue)
		return e1 == nil && e2 == nil && atNew == moverCount && atOld == 0
	}, 60*time.Second, 200*time.Millisecond,
		"post-swap: movers must serve their NEW value (%d at %d) and none at the OLD value %d",
		moverCount, newValue, oldValue)

	// Membership backstop: the stable (backfilled-only) objects are all present.
	atStable, err := rangeCount(restURI, className, "score", stableVal, stableVal)
	require.NoError(t, err)
	assert.Equal(t, total-moverCount, atStable, "non-mover objects must remain at their stable value")
}

// postObjects batch-imports objects (200 per batch) with consistency_level=ALL.
// Reused by the value-integrity test for both the seed and the in-flight
// overwrite (same UUIDs overwrite the prior value).
func postObjects(t *testing.T, restURI string, objects []map[string]interface{}) {
	t.Helper()
	const batchSize = 200
	for start := 0; start < len(objects); start += batchSize {
		end := start + batchSize
		if end > len(objects) {
			end = len(objects)
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects[start:end]})
		require.NoError(t, err)
		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "batch %d-%d failed: %s", start, end, string(respBody))

		var batchResp []struct {
			Result struct {
				Errors *struct {
					Error []struct {
						Message string `json:"message"`
					} `json:"error"`
				} `json:"errors,omitempty"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(respBody, &batchResp))
		for i, br := range batchResp {
			if br.Result.Errors != nil && len(br.Result.Errors.Error) > 0 {
				t.Fatalf("batch %d-%d object %d errored: %s", start, end, i, br.Result.Errors.Error[0].Message)
			}
		}
	}
}
