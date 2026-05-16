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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_EnableRangeable_NoPartialCountsInFlight pins GH
// 0-weaviate-issues#212 Issue C. During an in-flight enable-rangeable
// migration, range queries on a numeric property must NEVER return
// partial counts. The expected behaviour is one of:
//
//  1. Full baseline count served from the filterable bucket walk
//     (slow but correct) while the rangeable bucket is being built, OR
//  2. Full baseline count served from the rangeable bucket once the
//     swap has completed on every replica.
//
// Any count that is neither baseline-correct nor a transient HTTP/RAFT
// error (e.g. 33 476 vs 50 074) is a Sev-2 data inconsistency: the
// query path is serving from a half-built rangeable bucket instead of
// the still-intact filterable bucket.
//
// Root cause observed on `1.38.0-dev-e1dfae0` (Frontend Claude's retest):
// FilterableToRangeableStrategy.OnMigrationComplete RAFTs the
// `IndexRangeFilters=true` schema flip from inside per-shard runtimeSwap.
// As soon as ANY shard's replica completes its swap, the flag flips
// cluster-wide. Replicas that haven't completed their swap yet still
// have an empty (PreReindexHook-created) main rangeable bucket, but
// `pv.hasRangeableIndex=true` now routes range queries to that empty
// bucket → partial / zero counts on those replicas → LB aggregation
// returns a partial count.
//
// The fix is to promote enable-rangeable to a semantic migration so
// the schema flip moves from per-shard `OnMigrationComplete` to the
// cluster-wide `flipSemanticMigrationSchema` in `OnTaskCompleted` (the
// same pattern change-tokenization, enable-filterable and
// enable-searchable already use).
func TestMultiNode_EnableRangeable_NoPartialCountsInFlight(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "InFlightRangeable"
	// Numeric range chosen so the [lo, hi] band covers exactly the seeded
	// half of the keyspace; the OTHER half is outside the band and the
	// query is naturally selective. This makes the baseline a fixed
	// fraction of total imports (5 000 of 10 000), large enough that a
	// partial bucket walk returns a visibly wrong count, but small
	// enough that the range bitmap is comfortable.
	const (
		totalObjects = 10_000
		// Band exactly halves the keyspace.
		rangeLo = 30_000
		rangeHi = 40_000
		// scoreFor(i) below produces values in [10_000, 60_000) with
		// step 5; (40000-30000)/5 + 1 = 2001 values in band.
		expectedBaseline = 2001
	)
	scoreFor := func(i int) int { return 10_000 + i*5 }

	trueVal, falseVal := true, false
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:              "score",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
	})
	defer deleteCollection(t, restURIOf(compose, 1), className)

	// Batch-import to keep the test under 20-minute deadline. The numeric
	// property `score` lets us run range queries; we don't need text
	// content here but the helper expects a name field too.
	batchImportNumeric(t, restURIOf(compose, 1), className, totalObjects, scoreFor)

	// Capture baseline: every replica must agree on the count BEFORE the
	// migration starts. If they don't, this isn't an Issue C repro, the
	// import-replication path is broken — fail loudly.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		count, err := rangeCount(restURIOf(compose, nodeIdx), className, "score", rangeLo, rangeHi)
		require.NoError(t, err, "pre-migration baseline query on node %d failed", nodeIdx)
		require.Equal(t, expectedBaseline, count,
			"node %d baseline = %d, expected %d (cluster import/replication is broken)",
			nodeIdx, count, expectedBaseline)
	}

	// Start per-node polling goroutines. Each goroutine cycles through the
	// 3 LB-side endpoints (mimicking real-world LB round-robin) hitting
	// every node directly. We count every wrong, non-baseline,
	// non-transient-error result as a failure. The migration on 10 k
	// objects across 3 shards takes seconds, so 50 ms poll spacing yields
	// dozens of in-flight samples per goroutine.
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
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				count, err := rangeCount(uri, className, "score", rangeLo, rangeHi)
				queryRuns.Add(1)
				if err != nil {
					// Transient HTTP / RAFT-busy errors are tolerated; a
					// query that errors out is at least signalling "I
					// can't answer" rather than returning a wrong
					// number. We log but don't count them as failures.
					queryErrors.Add(1)
					t.Logf("node %d transient error: %v", idx, err)
				} else if count != expectedBaseline {
					wrongCounts.Add(1)
					t.Logf("ISSUE C REPRO: node %d returned partial count %d (expected %d)",
						idx, count, expectedBaseline)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}

	// Submit enable-rangeable. We do this via the same handler the demo
	// hits, so the on-the-wire shape matches the production failure.
	taskID := submitIndexUpdate(t, restURIOf(compose, 1), className, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("submitted enable-rangeable task: %s", taskID)

	// Block until the migration has fully reached FINISHED. We then keep
	// polling for a settle window — the schema flip RAFT can land on
	// other nodes a moment after FINISHED.
	awaitReindexFinished(t, restURIOf(compose, 1), taskID)

	// Settle window: keep polling for 3 s after FINISHED. This catches
	// the late-window race where one node's schema flip arrived but
	// another node's swap hasn't run yet.
	time.Sleep(3 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d transient errors, %d wrong counts",
		queryRuns.Load(), queryErrors.Load(), wrongCounts.Load())

	// Verify the schema flip eventually committed on every node.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		cls := getClassFromNode(t, restURIOf(compose, nodeIdx), className)
		var ok bool
		for _, prop := range cls.Properties {
			if prop.Name == "score" && prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				ok = true
				break
			}
		}
		assert.True(t, ok, "node %d: IndexRangeFilters should be true after migration", nodeIdx)
	}

	// Final per-replica baseline: every node now serves the full count.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		count, err := rangeCount(restURIOf(compose, nodeIdx), className, "score", rangeLo, rangeHi)
		require.NoError(t, err, "post-migration query on node %d failed", nodeIdx)
		assert.Equal(t, expectedBaseline, count,
			"node %d post-migration count = %d, expected %d",
			nodeIdx, count, expectedBaseline)
	}

	// The whole point: ZERO wrong (non-error, non-baseline) counts during
	// the in-flight window. A non-zero count here is a #212-Issue-C repro.
	assert.Zero(t, wrongCounts.Load(),
		"GH #212 Issue C: expected zero partial counts during in-flight "+
			"enable-rangeable migration. The migration must either still serve "+
			"the filterable bucket walk (correct, slow) or the fully-swapped "+
			"rangeable bucket (correct, fast). Mid-swap partial counts indicate "+
			"the schema flag flipped cluster-wide before every replica's swap "+
			"completed.")
}

// restURIOf is a tiny readability helper.
func restURIOf(compose *docker.DockerCompose, nodeIdx int) string {
	return compose.GetWeaviateNode(nodeIdx).URI()
}

// batchImportNumeric posts `total` objects in 200-object batches, each with
// a unique `name` and a `score` produced by scoreFor(i). consistency_level=
// ALL ensures the cluster is in a fully-replicated baseline state before
// the migration starts (a pre-migration baseline that flaps across nodes
// is a different bug, not Issue C — fail loudly above).
func batchImportNumeric(
	t *testing.T, restURI, className string, total int, scoreFor func(i int) int,
) {
	t.Helper()
	const batchSize = 200
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		objects := make([]map[string]interface{}, 0, end-start)
		for i := start; i < end; i++ {
			objects = append(objects, map[string]interface{}{
				"class": className,
				"id":    uuid.New().String(),
				"properties": map[string]interface{}{
					"name":  fmt.Sprintf("item-%d", i),
					"score": scoreFor(i),
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects})
		require.NoError(t, err)
		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"batch %d-%d failed: %s", start, end, string(respBody))

		var batchResp []struct {
			Result struct {
				Errors *struct {
					Error []struct {
						Message string `json:"message"`
					} `json:"error"`
				} `json:"errors,omitempty"`
				Status string `json:"status"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(respBody, &batchResp))
		for i, br := range batchResp {
			if br.Result.Errors != nil && len(br.Result.Errors.Error) > 0 {
				t.Fatalf("batch %d-%d object %d errored: %s",
					start, end, i, br.Result.Errors.Error[0].Message)
			}
		}
	}
}

// rangeCount runs a range-band query (>= lo AND <= hi) and returns the
// number of matched IDs. Errors are propagated so the poll loop can
// distinguish transient HTTP errors from wrong-count results.
func rangeCount(restURI, className, propName string, lo, hi int) (int, error) {
	// 1) Aggregate count avoids the implicit limit cap of Get. Using the
	//    `meta { count }` shape on a where-filtered Aggregate is the
	//    pattern the demo runs at LB-side and what Frontend Claude
	//    instrumented in the #212 repro.
	gqlQuery := fmt.Sprintf(`{
		Aggregate {
			%s(where: {
				operator: And,
				operands: [
					{path: [%q], operator: GreaterThanEqual, valueInt: %d},
					{path: [%q], operator: LessThanEqual, valueInt: %d}
				]
			}) {
				meta { count }
			}
		}
	}`, className, propName, lo, propName, hi)
	reqBody := map[string]interface{}{"query": gqlQuery}
	jsonBody, _ := json.Marshal(reqBody)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return 0, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("reading response: %w", err)
	}
	var gqlResp struct {
		Data struct {
			Aggregate map[string][]struct {
				Meta struct {
					Count int `json:"count"`
				} `json:"meta"`
			} `json:"Aggregate"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return 0, fmt.Errorf("graphql errors: %v", gqlResp.Errors[0].Message)
	}
	rows := gqlResp.Data.Aggregate[className]
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Meta.Count, nil
}
