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

package columnar_index_test

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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	cwNumObjects   = 8000
	cwNumUpdates   = 1000
	cwUpdateOffset = 1000 // indices [1000, 2000) receive the marker PATCH
	cwWriters      = 4
	cwMark         = 777777
	// cwWindowCap bounds the window-spanning PATCH load: workers keep
	// patching until the indexColumnar flag flips, but never longer than
	// this (a hung migration must fail the test, not deadlock it).
	cwWindowCap = 240 * time.Second
)

// cwCounts holds one filtered-Aggregate sample. Comparable struct so
// samples can be checked against the expected value with ==.
type cwCounts struct {
	metaCount, vintCount, vintSum float64
}

// TestEnableColumnar_ConcurrentWritesDuringMigration is the black-box
// regression test for the markStarted→registerDoubleWriteCallbacks
// write-loss window (see TestEnableColumnar_ConcurrentWritesInCallback-
// RegistrationGap in adapters/repos/db for the deterministic gap-injection
// companion): a PATCH landing between the reindexStarted timestamp capture
// and the double-write callback registration was skipped by the backfill
// iterator AND not mirrored into the ingest bucket — permanently missing
// from the columnar bucket. Symptom: a columnar-served filtered aggregate
// (vint{count}) returns fewer rows than the object-path count (meta{count})
// for the same filter.
//
// The race is timing-dependent, so this test cannot force the loss on a
// fixed-good build — it asserts the invariant (columnar count AND values ==
// object-path state) under the same concurrent load that exposed the bug
// in QA's black-box repro. A live-control case (indexColumnar at creation)
// guards the synchronous write path under identical load.
//
// History: this test was deliberately committed RED. The residual
// stale-sum failure (count converged, sum short by a handful of patched
// objects) was the flip→disable TOCTOU at the migration end: a PATCH that
// ran AnalyzeObject before OnMigrationComplete flipped IndexColumnar
// carried HasColumnarIndex=false, and if its property-value-index update
// executed after runtimeSwap disabled the double-write mirror, the new
// value reached no columnar bucket — silently (the disabled mirror
// returns nil). A sibling window ([SwapBucketPointer, disable)) turned
// mirror writes into "bucket not found" PATCH 5xxs whose retries
// converged on the same permanent staleness (the object store already
// held the marker, so the retry's delta analysis skipped the property).
// Fixed by (a) the mirror callbacks falling back to the canonical bucket
// name across the swap and (b) runtimeSwap draining the shard's
// inverted-write gate after the schema flip before disabling the mirror.
// Deterministic companions: TestEnableColumnar_MirrorWriteInSwapWindow-
// ReachesSwappedBucket and TestEnableColumnar_PreFlipAnalyzedWrite-
// CompletesBeforeMirrorDisable in adapters/repos/db, plus
// TestSegmentGroup_PrependSegments_ColumnarSurvivesReload in lsmkv for
// the durable segment-name ordering of the prepended backfill.
func TestEnableColumnar_ConcurrentWritesDuringMigration(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		// default scheduler tick is too slow: the migration must start
		// while the concurrent PATCH load is still running
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer helper.ResetClient()

	defer func() {
		if t.Failed() {
			dumpLastContainerLogLines(ctx, t, compose, 200)
		}
	}()

	cases := []struct {
		name             string
		className        string
		enableDuringLoad bool
	}{
		{
			name:             "enable-columnar migration racing concurrent merge-updates",
			className:        "ColumnarMigrationWriteRace",
			enableDuringLoad: true,
		},
		{
			name:             "live control: columnar enabled at creation",
			className:        "ColumnarLiveControlWrites",
			enableDuringLoad: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			helper.CreateClass(t, makeCwClass(tc.className, !tc.enableDuringLoad))
			t.Cleanup(func() { helper.DeleteClass(t, tc.className) })

			importCwObjects(t, tc.className)

			// baseline before any mutation: a shortfall here is an
			// import problem, not the write-loss regression
			base, err := fetchCwCounts(restURI, tc.className)
			require.NoError(t, err)
			require.Equal(t, float64(cwNumObjects), base.metaCount, "import baseline broken")
			require.Equal(t, float64(cwNumObjects), base.vintCount, "import baseline broken")

			// finalVal returns the value object i must end at; filled per
			// branch below.
			var finalVal func(i int) int64

			if tc.enableDuringLoad {
				taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, tc.className, "vint",
					`{"columnar":{"enabled":true}}`)
				t.Logf("submitted enable-columnar task: %s", taskID)

				// The PATCH load must span the ENTIRE migration window
				// [trigger, flag-flip): a fixed burst completes in ~100ms
				// and ends before the DTM scheduler even starts the
				// migration, leaving the whole double-write window
				// untested (the blind spot QA's live repro exposed).
				start := time.Now()
				lastVals := runWindowSpanningPatches(t, restURI, tc.className)
				t.Logf("window-spanning merge-updates finished after %s", time.Since(start))

				reindexhelpers.AwaitReindexFinished(t, restURI, taskID,
					reindexhelpers.WithTimeout(180*time.Second))
				require.Eventually(t, func() bool {
					on, err := vintColumnarFlagOn(restURI, tc.className)
					return err == nil && on
				}, 60*time.Second, 500*time.Millisecond,
					"indexColumnar flag must flip to true after the migration")

				finalVal = func(i int) int64 {
					if i >= cwUpdateOffset && i < cwUpdateOffset+cwNumUpdates {
						return lastVals[i-cwUpdateOffset]
					}
					return int64(i)
				}
			} else {
				runConcurrentPatches(t, restURI, tc.className)
				finalVal = func(i int) int64 {
					if i >= cwUpdateOffset && i < cwUpdateOffset+cwNumUpdates {
						return cwMark
					}
					return int64(i)
				}
			}

			// expected final state: count is unaffected by PATCHes; the sum
			// reflects the last successfully written value per object.
			var expectedSum float64
			for i := 0; i < cwNumObjects; i++ {
				expectedSum += float64(finalVal(i))
			}
			expected := cwCounts{
				metaCount: cwNumObjects,
				vintCount: cwNumObjects,
				vintSum:   expectedSum,
			}

			// converge with a generous deadline first (rules out transient
			// lag), then demand stability over additional samples: a lost
			// write never recovers, so a persistent shortfall is the bug
			var got cwCounts
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				got, err = fetchCwCounts(restURI, tc.className)
				if err == nil && got == expected {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			require.NoError(t, err)
			if got != expected {
				// distinguish "columnar bucket stale/lossy" (object store has
				// the expected value, columnar does not) from "merge-update
				// lost entirely" before failing
				stale := objectStoreMismatchedIDs(t, restURI, tc.className, finalVal)
				t.Logf("object store: %d of %d patched objects missing their expected final value: %v",
					len(stale), cwNumUpdates, stale)
			}
			require.Equal(t, expected, got,
				"columnar-served filtered aggregate must equal the object-path state: "+
					"a vint{count} shortfall means concurrent writes were lost during the "+
					"enable-columnar migration; a vint{sum} mismatch with matching count "+
					"means the columnar bucket serves stale pre-update values")

			for sample := 0; sample < 5; sample++ {
				time.Sleep(300 * time.Millisecond)
				got, err = fetchCwCounts(restURI, tc.className)
				require.NoError(t, err)
				require.Equal(t, expected, got, "stability sample %d deviates", sample)
			}
		})
	}
}

func makeCwClass(name string, columnarAtCreate bool) *models.Class {
	return &models.Class{
		Class:             name,
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		Properties: []*models.Property{
			{Name: "grp", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(true)},
			{Name: "vint", DataType: schema.DataTypeInt.PropString(), IndexColumnar: boolPtr(columnarAtCreate)},
		},
	}
}

func importCwObjects(t *testing.T, className string) {
	t.Helper()
	for start := 0; start < cwNumObjects; start += 1000 {
		batch := make([]*models.Object, 0, 1000)
		for i := start; i < start+1000; i++ {
			batch = append(batch, &models.Object{
				Class:      className,
				ID:         uuidFor(i),
				Properties: map[string]interface{}{"grp": 1, "vint": i},
			})
		}
		helper.CreateObjectsBatch(t, batch)
	}
}

// runConcurrentPatches fires cwNumUpdates merge-PATCHes (vint -> cwMark)
// striped across cwWriters goroutines and blocks until all finish. Every
// PATCH must succeed (with retries): a silently failed PATCH would skew
// the expected aggregate and mask the regression under test.
func runConcurrentPatches(t *testing.T, restURI, className string) {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	var failures atomic.Int64
	for w := 0; w < cwWriters; w++ {
		w := w
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for i := cwUpdateOffset + w; i < cwUpdateOffset+cwNumUpdates; i += cwWriters {
				if err := patchVintWithRetry(restURI, className, i); err != nil {
					failures.Add(1)
					t.Logf("PATCH object %d failed after retries: %v", i, err)
				}
			}
		}, logger)
	}
	wg.Wait()
	require.Zero(t, failures.Load(), "all concurrent merge-updates must succeed")
}

// runWindowSpanningPatches keeps PATCHing the band [cwUpdateOffset,
// cwUpdateOffset+cwNumUpdates) in rounds, striped across cwWriters
// workers, until the property's indexColumnar flag flips to true (i.e.
// the migration completed end-to-end) or cwWindowCap elapses. Round r
// writes vint = cwMark + r, so every PATCH is a real value change (a
// repeat of the same value could be elided by merge delta analysis and
// would not exercise the double-write mirror).
//
// Every individual PATCH must succeed: NO retries. A 5xx mid-migration
// is itself the regression under test (the mirror's "bucket not found"
// failure mode), so it is recorded and failed loudly rather than
// papered over.
//
// Returns the last successfully written value per band index (the
// import value i if a doc was never successfully patched), from which
// the caller computes the exact expected aggregate.
func runWindowSpanningPatches(t *testing.T, restURI, className string) []int64 {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()

	lastVals := make([]int64, cwNumUpdates)
	for j := 0; j < cwNumUpdates; j++ {
		lastVals[j] = int64(cwUpdateOffset + j)
	}

	var stop atomic.Bool
	flagPollDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(flagPollDone)
		deadline := time.Now().Add(cwWindowCap)
		for !stop.Load() {
			if time.Now().After(deadline) {
				t.Logf("window cap %s reached before the indexColumnar flag flipped", cwWindowCap)
				stop.Store(true)
				return
			}
			if on, err := vintColumnarFlagOn(restURI, className); err == nil && on {
				stop.Store(true)
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}, logger)

	var (
		wg          sync.WaitGroup
		failures    atomic.Int64
		totalRounds atomic.Int64
		failuresMu  sync.Mutex
		failureMsgs []string
	)
	for w := 0; w < cwWriters; w++ {
		w := w
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for round := int64(1); !stop.Load(); round++ {
				val := int64(cwMark) + round
				for i := cwUpdateOffset + w; i < cwUpdateOffset+cwNumUpdates; i += cwWriters {
					if stop.Load() {
						return
					}
					if err := patchVintValue(restURI, className, i, val); err != nil {
						failures.Add(1)
						failuresMu.Lock()
						if len(failureMsgs) < 20 {
							failureMsgs = append(failureMsgs, fmt.Sprintf("object %d round %d: %v", i, round, err))
						}
						failuresMu.Unlock()
						continue
					}
					lastVals[i-cwUpdateOffset] = val
				}
				totalRounds.Add(1)
			}
		}, logger)
	}
	wg.Wait()
	stop.Store(true)
	<-flagPollDone

	t.Logf("window-spanning load: %d completed worker-rounds, %d PATCH failures",
		totalRounds.Load(), failures.Load())
	for _, msg := range failureMsgs {
		t.Logf("PATCH failure: %s", msg)
	}
	require.Zero(t, failures.Load(),
		"every PATCH issued during the [trigger, flag-flip) window must succeed — "+
			"a 5xx here is the double-write mirror failing to resolve its target bucket")
	require.GreaterOrEqual(t, totalRounds.Load(), int64(1),
		"the PATCH load must have completed at least one full round inside the migration window")
	return lastVals
}

func patchVintWithRetry(restURI, className string, idx int) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		if err = patchVint(restURI, className, idx); err == nil {
			return nil
		}
	}
	return err
}

func patchVint(restURI, className string, idx int) error {
	return patchVintValue(restURI, className, idx, cwMark)
}

func patchVintValue(restURI, className string, idx int, val int64) error {
	body, err := json.Marshal(map[string]interface{}{
		"class":      className,
		"id":         uuidFor(idx),
		"properties": map[string]interface{}{"vint": val},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPatch,
		fmt.Sprintf("http://%s/v1/objects/%s/%s", restURI, className, uuidFor(idx)),
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// fetchCwCounts samples meta{count} (object path) and vint{count sum}
// (columnar path once the flag is on) from ONE GraphQL response, so the
// two paths are compared on the same request.
func fetchCwCounts(restURI, className string) (cwCounts, error) {
	query := fmt.Sprintf(`{
		Aggregate {
			%s(where: {path: ["grp"], operator: Equal, valueInt: 1}) {
				meta { count }
				vint { count sum }
			}
		}
	}`, className)
	jsonBody, err := json.Marshal(map[string]interface{}{"query": query})
	if err != nil {
		return cwCounts{}, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return cwCounts{}, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return cwCounts{}, fmt.Errorf("reading response: %w", err)
	}
	var gqlResp struct {
		Data struct {
			Aggregate map[string][]struct {
				Meta struct {
					Count *float64 `json:"count"`
				} `json:"meta"`
				Vint struct {
					Count *float64 `json:"count"`
					Sum   *float64 `json:"sum"`
				} `json:"vint"`
			} `json:"Aggregate"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return cwCounts{}, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return cwCounts{}, fmt.Errorf("graphql errors: %v", gqlResp.Errors[0].Message)
	}
	rows := gqlResp.Data.Aggregate[className]
	if len(rows) != 1 {
		return cwCounts{}, fmt.Errorf("expected 1 aggregate row, got %d", len(rows))
	}
	row := rows[0]
	if row.Meta.Count == nil || row.Vint.Count == nil || row.Vint.Sum == nil {
		return cwCounts{}, fmt.Errorf("aggregate response misses fields: %s", string(body))
	}
	return cwCounts{
		metaCount: *row.Meta.Count,
		vintCount: *row.Vint.Count,
		vintSum:   *row.Vint.Sum,
	}, nil
}

// objectStoreMismatchedIDs reads every patched object through the plain
// object GET path and returns the indices whose vint is NOT the expected
// final value — i.e. merge-updates that never reached the object store
// (as opposed to updates lost only from the columnar bucket).
func objectStoreMismatchedIDs(t *testing.T, restURI, className string, finalVal func(int) int64) []int {
	t.Helper()
	var missing []int
	for i := cwUpdateOffset; i < cwUpdateOffset+cwNumUpdates; i++ {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/objects/%s/%s", restURI, className, uuidFor(i)))
		if err != nil {
			t.Logf("GET object %d: %v", i, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var obj struct {
			Properties map[string]interface{} `json:"properties"`
		}
		if err := json.Unmarshal(body, &obj); err != nil {
			t.Logf("GET object %d: unmarshal: %v", i, err)
			continue
		}
		v, ok := obj.Properties["vint"].(float64)
		if !ok || v != float64(finalVal(i)) {
			missing = append(missing, i)
		}
	}
	return missing
}

func vintColumnarFlagOn(restURI, className string) (bool, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s", restURI, className))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("get schema status %d: %s", resp.StatusCode, string(body))
	}
	var cls struct {
		Properties []struct {
			Name          string `json:"name"`
			IndexColumnar *bool  `json:"indexColumnar"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(body, &cls); err != nil {
		return false, err
	}
	for _, p := range cls.Properties {
		if p.Name == "vint" {
			return p.IndexColumnar != nil && *p.IndexColumnar, nil
		}
	}
	return false, fmt.Errorf("property vint not found in schema response")
}

func dumpLastContainerLogLines(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int) {
	t.Helper()
	reader, err := compose.GetWeaviate().Container().Logs(ctx)
	if err != nil {
		t.Logf("failed to get container logs: %v", err)
		return
	}
	defer reader.Close()
	logs, _ := io.ReadAll(reader)
	lines := strings.Split(string(logs), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	t.Logf("=== Container logs (last %d lines) ===\n%s", n, strings.Join(lines, "\n"))
}
