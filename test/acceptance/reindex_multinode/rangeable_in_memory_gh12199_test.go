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
	"sync"
	"sync/atomic"
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
// an enable-rangeable reindex must NOT serve empty range results from an
// unpopulated in-memory representation after the per-shard swap (no restart
// required). It codifies Etienne's R2d empirical confirmation (0 vs 20003 golden,
// 0.011 MB empty rep) into permanent tests, and pins the two operator log signals
// added by the fix.
//
// Log substrings emitted by the fix (adapters/repos/db/lsmkv/bucket_roaring_set_range.go):
//   - deferredINFOSubstr: fires once per bucket-open at the first disk-path range
//     read on a bucket the reindex ingest path marked (keepSegmentsInMemory forced
//     off while the knob is on). Its PRESENCE confirms (b)'s marker path is applied
//     at the fix SHA: only the (b) marker path sets the marker.
//   - fallbackWARNSubstr: the (c) disk-fallback WARN. On the fixed path it must NOT
//     fire, because the post-swap serving bucket has keepSegmentsInMemory=false.
//
// Per-mechanism revert-probe mapping (verified empirically; QA round 1 correction):
//   - (b) alone       -> caught by the child-1 prepend GUARD at reindex time, NOT by
//     the WARN/INFO assertions below. Reverting ONLY (b) (drop the
//     WithKeepSegmentsInMemory(false) override + the deferred marker,
//     KEEP the guard) leaves the ingest bucket keepSegmentsInMemory=
//     true, so it builds an ACTIVE in-memory rep; the child-1 prepend
//     guard then rejects the backfill prepend and the reindex FAILS at
//     AwaitReindexFinished with the INV-RANGEABLE-REP-EQUALS-DISK
//     sentinel, before any post-swap query runs. That reindex failure
//     is the single (b)-alone red condition, and it is caught by this
//     acceptance repro at the AwaitReindexFinished step.
//   - WARN/INFO pair  -> the fallbackWARNSubstr-absence + deferredINFOSubstr-presence
//     assertions serve two roles: (i) positive happy-path proof that
//     (b) IS applied at the fix SHA, and (ii) a tripwire for a (b) AND
//     guard DOUBLE revert. Only when BOTH (b) and the guard are
//     reverted does the empty-rep-served state actually arise: the
//     reindex then completes, the post-swap read takes the (c)
//     fallback so the WARN APPEARS, and the marker is unset so the
//     deferred INFO DISAPPEARS (two conditions). A counts-only probe
//     cannot catch that double-revert while (c) is in the tree; these
//     assertions can. (The earlier "reverting (b) alone -> WARN+INFO
//     redness" narrative was wrong: that probe had disabled the guard
//     too. Corrected per QA round 1.)
//   - (c) alone       -> child-1 Layer-1 four-case matrix (empty rep + disk segments
//     must fall back), unit-scale, already red-green verified.
//   - guard alone     -> child-1 guard unit test (active-rep prepend errors).
//   - option-(a) trap -> child-1 fold-order value-integrity unit test.
const (
	deferredINFOSubstr = "in-memory knob deferred until next reload"
	fallbackWARNSubstr = "falling back to the disk range reader"
)

// countInLogs returns the number of lines in a container's logs that contain
// substr. Used to assert presence/absence of the two GH#12199 log signals.
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
// rangeable knob on. INDEX_RANGEABLE_IN_MEMORY=true is the precondition for
// GH#12199; LOG_LEVEL=info makes the deferred-serving INFO (and any (c) WARN)
// visible in the container logs the assertions grep.
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

// scoreForGH12199 maps import index i to a numeric score in [10_000, 60_000)
// with step 5, so a [30_000, 40_000] band selects exactly 2001 of 10_000 objects
// (mirrors in_flight_rangeable_test.go so a half-built bucket returns a visibly
// wrong count).
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

	// Enable rangeable and wait for the migration to fully finish. NO restart.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))

	// (1) Counts correct post-swap WITHOUT restart. The pre-fix image returns 0
	// here (empty in-memory rep served; Etienne R2d). Poll briefly for the
	// per-shard swap + schema flip to settle.
	require.Eventually(t, func() bool {
		c, e := rangeCount(restURI, className, "score", gh12199RangeLo, gh12199RangeHi)
		return e == nil && c == gh12199Baseline
	}, 60*time.Second, 200*time.Millisecond,
		"GH#12199: post-swap range count must equal golden %d WITHOUT restart", gh12199Baseline)

	// Drive a few more range reads so the deferred-serving INFO's first-disk-read
	// trigger has definitely run on every marked bucket.
	for i := 0; i < 5; i++ {
		_, _ = rangeCount(restURI, className, "score", gh12199RangeLo, gh12199RangeHi)
	}
	time.Sleep(2 * time.Second) // let the container flush stdout

	// (2) The (c) disk-fallback WARN must NOT fire on the fixed path: the serving
	// bucket has keepSegmentsInMemory=false, so ReaderRoaringSetRange never reaches
	// the (c) branch. Its presence would mean BOTH (b) and the prepend guard were
	// reverted (the empty-rep-served state); a (b)-alone revert fails the reindex at
	// the guard above and never reaches here.
	assert.Zero(t, countInLogs(ctx, t, container, fallbackWARNSubstr),
		"(c) disk-fallback WARN must not fire on the fixed path (WARN present => (b) AND guard both reverted, empty rep served, GH#12199)")

	// (3) The deferred-serving INFO MUST be present: only the (b) marker path sets
	// the marker, so its presence is the positive "(b) applied at the fix SHA" signal.
	assert.Positive(t, countInLogs(ctx, t, container, deferredINFOSubstr),
		"deferred-serving INFO must fire post-swap (marker set + first disk read); absence => (b) marker not applied, GH#12199")

	// --- Post-restart in-memory hand-off case ---
	// After a restart the rangeable bucket reopens via the normal shard path
	// (knob on), so boot-time population rebuilds the in-memory rep from the now
	// fully-populated disk segments. Range reads then serve from the rebuilt rep,
	// counts stay correct, and no (c) WARN fires (rep populated). This pins the
	// deferred-acceleration hand-off and the restart self-heal.
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

	require.Eventually(t, func() bool {
		c, e := rangeCount(restURI, className, "score", gh12199RangeLo, gh12199RangeHi)
		return e == nil && c == gh12199Baseline
	}, 60*time.Second, 200*time.Millisecond,
		"post-restart range count must still equal golden %d (rep rebuilt at boot)", gh12199Baseline)

	for i := 0; i < 5; i++ {
		_, _ = rangeCount(restURI, className, "score", gh12199RangeLo, gh12199RangeHi)
	}
	time.Sleep(2 * time.Second)
	assert.Zero(t, countInLogs(ctx, t, container, fallbackWARNSubstr),
		"no (c) WARN after restart: the rep is rebuilt at boot and serves in-memory")
}

// TestRangeableInMemory_GH12199_MultiNode_InFlightStatisticalProbe is the 3-node
// statistical probe per reference_migration_window_race_reproducer.md: fire range
// queries at every node throughout the migration window and count divergences
// (non-error, non-golden), not single-shot. Silent wrong results, not errors, are
// the failure mode.
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

	var (
		wrongCounts atomic.Int64
		queryRuns   atomic.Int64
		queryErrors atomic.Int64
		stopCh      = make(chan struct{})
		wg          sync.WaitGroup
	)
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		wg.Add(1)
		uri := restURIOf(compose, nodeIdx)
		idx := nodeIdx
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
				}
				c, err := rangeCount(uri, className, "score", gh12199RangeLo, gh12199RangeHi)
				queryRuns.Add(1)
				if err != nil {
					queryErrors.Add(1)
					continue
				}
				if c != gh12199Baseline {
					wrongCounts.Add(1)
					t.Logf("GH#12199 in-flight divergence: node %d returned %d (expected %d)", idx, c, gh12199Baseline)
				}
			}
		}()
	}

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
		queryRuns.Load(), queryErrors.Load(), wrongCounts.Load())

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

	assert.Zero(t, wrongCounts.Load(),
		"GH#12199: zero silent-wrong range counts during the in-flight enable-rangeable "+
			"window with INDEX_RANGEABLE_IN_MEMORY=true. A non-zero count means a replica "+
			"served from an empty in-memory rep instead of the disk path.")
}

// TestRangeableInMemory_GH12199_WriteWorkloadValueIntegrity runs the migration
// under concurrent live writes that CHANGE a subset of objects' value, then
// asserts VALUE integrity (not just membership): post-swap, a mover's NEW value
// query returns it and its OLD value query does not. Fold-order corruption (the
// rejected option (a)) would leave the old value winning; the empty-rep bug would
// drop the movers entirely. Both are value/membership-discriminating here.
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

	// Start the migration, then overwrite the movers to newValue WHILE it runs.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
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
