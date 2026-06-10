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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_EnableColumnar_AggregatesStableInFlight verifies the
// per-shard columnar readiness gate during an in-flight enable-columnar
// migration. Filtered aggregations are the (only) consumer of the
// columnar buckets, and during the migration window a shard's columnar
// bucket exists but is only partially populated. The readiness gate
// (isColumnarLocallyReady, see
// adapters/repos/db/aggregator/filtered_columnar.go) must route
// aggregations on a not-yet-ready shard to the object-scan fallback
// instead of serving partial columns.
//
// Concretely: filtered Aggregate {count, sum, mean} responses must equal
// the pre-migration baseline at EVERY point in time —
//
//  1. before the migration (object-scan path, columnar flag false),
//  2. during the migration (object-scan fallback via the readiness gate),
//  3. after the migration (columnar fast path).
//
// Any deviating non-error response means a query was served from a
// half-built columnar bucket: a silent-wrong-results bug, not a
// performance hiccup.
func TestMultiNode_EnableColumnar_AggregatesStableInFlight(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "InFlightColumnar"
	const (
		totalObjects = 2000
		// scoreFor below yields values in [10_000, 20_000) with step 5.
		// The band covers exactly the first half of the keyspace
		// (i in [0, 999]) so the aggregate is filtered (the columnar
		// fast path only serves filtered aggregations) yet matches a
		// large, deterministic subset.
		bandLo = 10_000
		bandHi = 14_995
	)
	scoreFor := func(i int) int { return 10_000 + i*5 }

	// Expected baseline computed from the data model, never from a
	// server response: count/sum/mean over the matched half. All values
	// are small integers, so float64 sums are exact and independent of
	// visit order (columnar scan order differs from object-scan order).
	var expected aggResult
	for i := 0; i < totalObjects; i++ {
		if s := scoreFor(i); s >= bandLo && s <= bandHi {
			expected.count++
			expected.sum += float64(s)
		}
	}
	expected.mean = expected.sum / expected.count // same expression the server uses

	trueVal, falseVal := true, false
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:            "score",
			DataType:        []string{"int"},
			IndexFilterable: &trueVal,
			IndexColumnar:   &falseVal,
		},
	})
	defer deleteCollection(t, restURIOf(compose, 1), className)

	batchImportNumeric(t, restURIOf(compose, 1), className, totalObjects, scoreFor)

	// Pre-migration baseline: every replica must serve the expected
	// aggregate BEFORE the migration starts. If not, import/replication
	// is broken and this is not a readiness-gate repro — fail loudly.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		got, err := filteredScoreAgg(restURIOf(compose, nodeIdx), className, bandLo, bandHi)
		require.NoError(t, err, "pre-migration baseline query on node %d failed", nodeIdx)
		require.Equal(t, expected, got,
			"node %d baseline = %+v, expected %+v (cluster import/replication is broken)",
			nodeIdx, got, expected)
	}

	// Per-node probe goroutines hammer the filtered Aggregate while the
	// migration runs. Transient HTTP/RAFT errors are tolerated ("I can't
	// answer" is acceptable mid-migration); a wrong VALUE is not.
	var (
		wrongResults atomic.Int64
		queryRuns    atomic.Int64
		queryErrors  atomic.Int64
		stopCh       = make(chan struct{})
		wg           sync.WaitGroup
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
				got, err := filteredScoreAgg(uri, className, bandLo, bandHi)
				queryRuns.Add(1)
				if err != nil {
					queryErrors.Add(1)
					t.Logf("node %d transient error: %v", idx, err)
				} else if got != expected {
					wrongResults.Add(1)
					t.Logf("READINESS GATE BREACH: node %d returned %+v (expected %+v)",
						idx, got, expected)
				}
			}
		}()
	}

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURIOf(compose, 1), className, "score",
		`{"columnar":{"enabled":true}}`)
	t.Logf("submitted enable-columnar task: %s", taskID)

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID,
		reindexhelpers.WithTimeout(180*time.Second))
	inFlightSamples := queryRuns.Load()
	t.Logf("samples taken before FINISHED was observed: %d (errors so far: %d)",
		inFlightSamples, queryErrors.Load())

	// Keep the probes running until every replica answers with the
	// baseline post-migration (the columnar fast path must serve the
	// exact same values), then stop them.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			got, err := filteredScoreAgg(restURIOf(compose, nodeIdx), className, bandLo, bandHi)
			if err != nil || got != expected {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond,
		"all replicas should serve the baseline aggregate after enable-columnar")
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d transient errors, %d wrong results",
		queryRuns.Load(), queryErrors.Load(), wrongResults.Load())

	// The whole point: ZERO wrong (non-error, non-baseline) aggregate
	// responses at any phase of the migration. A non-zero count means a
	// query was served from a partially-built columnar bucket instead of
	// the object-scan fallback.
	assert.Zero(t, wrongResults.Load(),
		"expected zero deviating aggregate responses during in-flight "+
			"enable-columnar migration. Mid-migration reads must fall back to "+
			"the object path (readiness gate) rather than serve partial columns.")

	// Schema flag committed on every node.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		cls := getClassFromNode(t, restURIOf(compose, nodeIdx), className)
		var ok bool
		for _, prop := range cls.Properties {
			if prop.Name == "score" && prop.IndexColumnar != nil && *prop.IndexColumnar {
				ok = true
				break
			}
		}
		assert.True(t, ok, "node %d: indexColumnar should be true after migration", nodeIdx)
	}
}

// aggResult holds the asserted fields of one filtered Aggregate response.
// Plain comparable struct so probe samples can be compared with ==.
type aggResult struct {
	count, sum, mean float64
}

// filteredScoreAgg runs a where-filtered Aggregate over the `score` int
// property (count/sum/mean) against one node. Errors are propagated so
// callers can distinguish transient failures from wrong values.
func filteredScoreAgg(restURI, className string, lo, hi int) (aggResult, error) {
	gqlQuery := fmt.Sprintf(`{
		Aggregate {
			%s(where: {
				operator: And,
				operands: [
					{path: ["score"], operator: GreaterThanEqual, valueInt: %d},
					{path: ["score"], operator: LessThanEqual, valueInt: %d}
				]
			}) {
				score { count sum mean }
			}
		}
	}`, className, lo, hi)
	jsonBody, err := json.Marshal(map[string]interface{}{"query": gqlQuery})
	if err != nil {
		return aggResult{}, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return aggResult{}, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return aggResult{}, fmt.Errorf("reading response: %w", err)
	}
	var gqlResp struct {
		Data struct {
			Aggregate map[string][]struct {
				Score struct {
					Count *float64 `json:"count"`
					Sum   *float64 `json:"sum"`
					Mean  *float64 `json:"mean"`
				} `json:"score"`
			} `json:"Aggregate"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return aggResult{}, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return aggResult{}, fmt.Errorf("graphql errors: %v", gqlResp.Errors[0].Message)
	}
	rows := gqlResp.Data.Aggregate[className]
	if len(rows) != 1 {
		return aggResult{}, fmt.Errorf("expected 1 aggregate row, got %d", len(rows))
	}
	s := rows[0].Score
	if s.Count == nil || s.Sum == nil || s.Mean == nil {
		return aggResult{}, fmt.Errorf("aggregate response misses score fields: %s", string(body))
	}
	return aggResult{count: *s.Count, sum: *s.Sum, mean: *s.Mean}, nil
}
