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

// This file is the weaviate/weaviate#12199 regression suite: with
// INDEX_RANGEABLE_IN_MEMORY=true, an enable-rangeable reindex must not serve
// empty range results after the per-shard swap (no restart required). Also
// pins the disk-fallback WARN and rebuild-at-finalize INFO log signals.
const (
	fallbackWARNSubstr        = "rangeable in-memory index is empty"
	rebuildFinalizeINFOSubstr = "rangeable in-memory index built at migration finalize"
)

// countInLogs returns the number of lines in a container's logs containing substr.
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

// assertRebuildFinalizeThenNoFallbackWarn requires the rebuild INFO at least
// once, then no fallback WARN after the last such INFO line.
func assertRebuildFinalizeThenNoFallbackWarn(ctx context.Context, t *testing.T, container interface {
	Logs(context.Context) (io.ReadCloser, error)
}, failMsgInfo, failMsgWarn string,
) {
	t.Helper()
	reader, err := container.Logs(ctx)
	require.NoError(t, err, "reading container logs")
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err, "draining container logs")

	lines := strings.Split(string(raw), "\n")
	lastInfoIdx := -1
	for i, line := range lines {
		if strings.Contains(line, rebuildFinalizeINFOSubstr) {
			lastInfoIdx = i
		}
	}
	require.GreaterOrEqual(t, lastInfoIdx, 0, failMsgInfo)

	for i, line := range lines[lastInfoIdx+1:] {
		if strings.Contains(line, fallbackWARNSubstr) {
			t.Fatalf("%s (found at log line %d, after the rebuild-finalize INFO at line %d)", failMsgWarn, lastInfoIdx+1+i, lastInfoIdx)
		}
	}
}

// startSingleNodeRangeableInMemCluster starts a 1-node cluster with the
// in-mem rangeable knob on and LOG_LEVEL=info so the rebuild-at-finalize
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

// scoreForGH12199 maps import index i to a score in [10_000, 60_000) step 5,
// so [30_000, 40_000] selects exactly 2001/10_000 objects.
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

// Pins weaviate/weaviate#12199: single-node acceptance repro (no restart),
// plus the post-restart in-memory hand-off case.
func TestRangeableInMemory_GH12199_SingleNode_AcceptanceAndRestartHandoff(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := startSingleNodeRangeableInMemCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	const className = "RangeableInMemAcceptance"
	// 3 shards => 3 rangeable buckets, each rebuilt at migration finalize,
	// so the rebuild-at-finalize INFO fires per bucket.
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

	// Poll briefly for the per-shard swap + schema flip to settle, then check
	// counts and the disk-fallback WARN.
	awaitRangeCountSettledNoFallback(ctx, t, container, restURI, className,
		gh12199RangeLo, gh12199RangeHi, gh12199Baseline,
		"weaviate/weaviate#12199: post-swap range count must equal golden %d WITHOUT restart", gh12199Baseline,
		"disk-fallback WARN must not fire on the fixed path; its presence means an empty in-memory rep was served (weaviate/weaviate#12199)")

	// Promoted buckets must serve range queries from memory immediately
	// after the swap, no restart.
	assertRebuildFinalizeThenNoFallbackWarn(ctx, t, container,
		"rebuild-at-finalize INFO must fire post-swap WITHOUT restart; absence means the in-memory rep was not rebuilt synchronously at migration finalize",
		"disk-fallback WARN must not fire after the rebuild-at-finalize INFO; its presence means a read fell back to disk after the rep was supposedly rebuilt")

	// Post-restart: boot-time population rebuilds the in-memory rep from disk,
	// so reads serve from the rep again and no fallback WARN fires.
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
		"no disk-fallback WARN after restart: the rep is rebuilt at boot and serves in-memory")
}

// Pins weaviate/weaviate#12199: 3-node statistical probe for silent wrong
// range counts during the in-flight enable-rangeable migration window.
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

// Pins weaviate/weaviate#12199: migrates under concurrent live writes that
// change movers' value, then asserts each mover serves its NEW value only.
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

	// Wait for the migration to be live before overwriting movers, or the
	// backfill could simply read newValue and the double-write scenario
	// wouldn't be exercised.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID)

	// AwaitReindexLive guarantees only a live task status, not backfill
	// cursor progress; movers are UUID-hashed pseudo-randomly across the
	// keyspace, so this overwrite likely lands mid-backfill without a hard
	// per-mover guarantee. The test asserts what must hold either way:
	// movers end at newValue, none at oldValue.
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
