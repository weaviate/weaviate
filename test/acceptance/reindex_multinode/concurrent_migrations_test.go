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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_ConcurrentDifferentMigrations_ExactCountsPostSettle pins
// GH 0-weaviate-issues#212 Issues C + D as observed by Frontend Claude on
// `1.38.0-dev-3e78517.amd64` (the merge sha of 64249a9efc):
//
//   - Phase-3 (post-fix settled) on the 1M-record demo cluster returned
//     `price=49,613` deterministically across all 3 replicas (expected
//     50,074) and `category` flapped `30,041 / 29,240 / 30,261` per call
//     (expected 30,342). None of the three category values matched the
//     baseline → ~300-1,000 records consistently dropped from the live
//     query plan after migration FINISHED.
//
// The 1M-record demo runs three migrations IN PARALLEL — enable-rangeable
// on `price`, enable-filterable on `category`, change-tokenization on
// `path`. The in-flight rangeable test
// [TestMultiNode_EnableRangeable_NoPartialCountsInFlight] runs a single
// migration in isolation and passes 10/10 locally; that means the bug
// surfaces only when multiple migrations share the same shard's objects
// bucket cursor / compaction lifecycle.
//
// Working hypothesis (to be confirmed by the failing test):
// `store.PauseObjectBucketCompaction` flips a boolean meta.active flag —
// it is not reference-counted. With 3 concurrent OnAfterLsmInitAsync
// loops, when the first migration finishes its iteration and runs
// `defer store.ResumeObjectBucketCompaction(ctx)`, compaction reactivates
// while the other two iterators are still walking the objects bucket on
// disk. A compaction-driven segment merge under an active CursorOnDisk
// can cause the iterator to skip the keys the now-deleted segment used
// to serve.
//
// Test shape mirrors Frontend Claude's Phase 2: build the same 3-prop
// schema, import 10k records (small but enough segments to trigger
// compaction during iteration), then submit the same three migrations
// in parallel and assert exact post-migration counts on every replica.
// The two assertions:
//
//  1. Every per-replica direct query returns the baseline count after
//     the migrations settle. A wrong count on any replica is the
//     bug.
//  2. LB-side sequential queries (the "demo pattern") never return less
//     than baseline. Mirrors the 3-LB-call Phase-3 spot check.
//
// If this test passes 10/10 locally but Frontend Claude's 1M-record
// repro still fails, the bug is data-scale-sensitive (e.g. a segment
// boundary that only forms at >100 k records) and we need to either:
//   - drive the local test harder (more objects / more migrations), or
//   - audit the iterator directly with synthetic compaction triggers
//     instead of relying on natural compaction.
func TestMultiNode_ConcurrentDifferentMigrations_ExactCountsPostSettle(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "ConcurrentMigsExact"
	const totalObjects = 10_000

	// `price`: int, IndexRangeFilters=false → will be enable-rangeable'd
	// `category`: text, IndexFilterable=false → will be enable-filterable'd
	// `path`: text, tokenization=field → will be change-tokenization'd to word
	trueVal, falseVal := true, false
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
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
			Tokenization:    "field",
		},
	})
	defer deleteCollection(t, restURIOf(compose, 1), className)

	// Numeric range chosen to cover exactly half the keyspace (i*2 covers
	// even ints 0..19998; band [3000, 4000] captures 501 of the 5000
	// even values >=3000 and <=4000 inclusive). Categories cycle through
	// 4 buckets; paths through 5. Each picks one value per record so the
	// expected counts are: priceBand=501, category0=2500, path0=2000.
	const (
		priceLo            = 3000
		priceHi            = 4000
		expectedPriceCount = 501  // (4000-3000)/2 + 1
		expectedCatCount   = 2500 // totalObjects / 4
		expectedPathCount  = 2000 // totalObjects / 5
	)
	categories := []string{"alpha", "beta", "gamma", "delta"}
	paths := []string{"/usr/bin", "/usr/lib", "/etc/conf", "/var/log", "/opt/app"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"price":    i * 2,
			"category": categories[i%len(categories)],
			"path":     paths[i%len(paths)],
		}
	})

	// Pre-migration baselines on every replica. If these don't match
	// expected, replication is the bug, not Issue C/D — fail loudly.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		require.NoError(t, err, "pre-mig price baseline on node %d", nodeIdx)
		require.Equal(t, expectedPriceCount, gotPrice,
			"node %d: pre-mig price baseline = %d, expected %d (replication broken)",
			nodeIdx, gotPrice, expectedPriceCount)

		// category: filterable not enabled yet, so equality filter would
		// need to scan objects. We skip the pre-mig category baseline —
		// it would force a different code path than the post-mig one.

		gotPath, err := equalCount(uri, className, "path", paths[0])
		require.NoError(t, err, "pre-mig path baseline on node %d", nodeIdx)
		require.Equal(t, expectedPathCount, gotPath,
			"node %d: pre-mig path baseline = %d, expected %d (replication broken)",
			nodeIdx, gotPath, expectedPathCount)
	}

	// Submit 3 migrations in parallel — same shape as Frontend Claude's
	// Phase 2 of the 1M-record demo.
	uri1 := restURIOf(compose, 1)
	var (
		priceTaskID, catTaskID, pathTaskID string
		wg                                 sync.WaitGroup
	)
	wg.Add(3)
	go func() {
		defer wg.Done()
		priceTaskID = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "price",
			`{"rangeable":{"enabled":true}}`)
	}()
	go func() {
		defer wg.Done()
		catTaskID = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "category",
			`{"filterable":{"enabled":true}}`)
	}()
	go func() {
		defer wg.Done()
		pathTaskID = reindexhelpers.SubmitIndexUpdate(t, uri1, className, "path",
			`{"searchable":{"tokenization":"word"}}`)
	}()
	wg.Wait()
	t.Logf("submitted parallel migrations: price=%s category=%s path=%s",
		priceTaskID, catTaskID, pathTaskID)

	// Block until all three reach FINISHED. The default 180s budget per
	// task is fine — on 10k records the migrations run in seconds, but
	// they can pile up serially through the scheduler.
	reindexhelpers.AwaitReindexFinished(t, uri1, priceTaskID, reindexhelpers.WithTimeout(180*time.Second))
	reindexhelpers.AwaitReindexFinished(t, uri1, catTaskID, reindexhelpers.WithTimeout(180*time.Second))
	reindexhelpers.AwaitReindexFinished(t, uri1, pathTaskID, reindexhelpers.WithTimeout(180*time.Second))

	// Brief settle so schema flips propagate to every node.
	time.Sleep(3 * time.Second)

	// Phase-3 equivalent: every replica, every prop, must return the
	// baseline. A wrong count on any (node, prop) pair is the bug.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)

		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		assert.NoError(t, err, "post-mig price query node %d", nodeIdx)
		assert.Equal(t, expectedPriceCount, gotPrice,
			"GH #212 Issue D regression: node %d post-mig price = %d, expected %d "+
				"(after concurrent enable-rangeable + enable-filterable + "+
				"change-tokenization, the rangeable bucket on this shard is "+
				"missing records)",
			nodeIdx, gotPrice, expectedPriceCount)

		gotCat, err := equalCount(uri, className, "category", categories[0])
		assert.NoError(t, err, "post-mig category query node %d", nodeIdx)
		assert.Equal(t, expectedCatCount, gotCat,
			"GH #212 Issue D regression: node %d post-mig category = %d, expected %d "+
				"(after concurrent migrations, the newly-created filterable "+
				"bucket on this shard is missing records)",
			nodeIdx, gotCat, expectedCatCount)

		gotPath, err := equalCount(uri, className, "path", paths[0])
		assert.NoError(t, err, "post-mig path query node %d", nodeIdx)
		assert.Equal(t, expectedPathCount, gotPath,
			"GH #212 Issue D regression: node %d post-mig path = %d, expected %d "+
				"(after concurrent migrations, the change-tokenization'd "+
				"searchable bucket on this shard is missing records)",
			nodeIdx, gotPath, expectedPathCount)
	}

	// LB-side spot-check: 3 sequential calls to the same node should all
	// return baseline. Mirrors Frontend Claude's Phase-3 LB calls.
	for i := 0; i < 3; i++ {
		gotPrice, err := rangeCount(uri1, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		assert.Equal(t, expectedPriceCount, gotPrice,
			"LB-side price call #%d = %d, expected %d", i+1, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(uri1, className, "category", categories[0])
		require.NoError(t, err)
		assert.Equal(t, expectedCatCount, gotCat,
			"LB-side category call #%d = %d, expected %d", i+1, gotCat, expectedCatCount)
	}
}

// batchImportMultiProp imports `total` objects in 200-object batches; each
// object's properties are built by propsFor(i). Uses consistency_level=ALL
// so the cluster reaches fully-replicated baseline before migrations start.
func batchImportMultiProp(
	t *testing.T, restURI, className string, total int, propsFor func(i int) map[string]interface{},
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
				"class":      className,
				"id":         uuid.New().String(),
				"properties": propsFor(i),
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

// equalCount runs a text Equal filter via Aggregate { meta { count } }.
func equalCount(restURI, className, propName, value string) (int, error) {
	gqlQuery := fmt.Sprintf(`{
		Aggregate {
			%s(where: {path: [%q], operator: Equal, valueText: %q}) {
				meta { count }
			}
		}
	}`, className, propName, value)
	return aggregateCount(restURI, className, gqlQuery)
}

// aggregateCount POSTs an Aggregate GraphQL query and returns the meta.count.
func aggregateCount(restURI, className, gqlQuery string) (int, error) {
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
