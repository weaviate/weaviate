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

package reindex_concurrent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestParallelEnableFilterableAndRangeable mirrors the frontend repro on
// weaviate/weaviate#10675: parallel enable-filterable + enable-rangeable
// on the same numeric property both end up FAILED with
// "progress.mig.000000001: no such file or directory".
//
// Root cause. Once a property has IndexFilterable=false and
// IndexRangeFilters=false (e.g. after DELETE filterable + DELETE
// rangeFilters), submitting both enable-filterable and enable-rangeable in
// parallel races on shared on-disk migration state. Whichever migration
// finishes first calls UpdateProperty; MergeProps preserves the
// still-false sibling flag (the other migration hasn't flipped it yet),
// so the RAFT-applied schema carries the in-progress migration's flag as
// false. On apply, Migrator.UpdateProperty → Shard.updatePropertyBuckets
// cleans the migration dirs for any index whose flag is now false —
// which removes the in-flight migration's working directory and the
// next markProgress fails with ENOENT.
//
// The fix gates this at submit time: any two reindex migrations on the
// same (collection, property) tuple conflict and the second submit gets
// a clean 409. Callers are expected to serialize the operations.
//
// The test asserts the contract:
//  1. At least one of the two parallel PUTs returns 202 (accepted).
//  2. The other PUT returns either 202 (if it slipped in before the first
//     task was visible in ListDistributedTasks) or 409 (rejected as a
//     conflict).
//  3. Whichever submits are accepted MUST FINISH (not FAIL). The frontend
//     repro had BOTH FAILED, which is the regression we are pinning.
//  4. After both migrations complete (serially if needed via retry on
//     409), both buckets must be queryable.
func TestParallelEnableFilterableAndRangeable(t *testing.T) {
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

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	// Dump container logs on failure — the bug lives in the indexing
	// pipeline, not in the HTTP layer, so the logs are how we debug.
	container := compose.GetWeaviate().Container()
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
			if len(lines) > 400 {
				lines = lines[len(lines)-400:]
			}
			t.Logf("=== Container logs (last 400 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	const (
		collection = "ParallelReindexSameProp"
		propName   = "score"
		numObjects = 500 // enough to exercise the iteration loop, not so many we wait minutes
	)

	// Step 0: create collection with the property starting in both-flags-true
	// state so DELETE has something to drop.
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: collection,
		Properties: []*models.Property{
			{
				Name:              propName,
				DataType:          []string{"int"},
				IndexFilterable:   &trueVal,
				IndexRangeFilters: &trueVal,
			},
		},
		Vectorizer: "none",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: false,
		},
	})

	objects := make([]*models.Object, numObjects)
	for i := range numObjects {
		objects[i] = &models.Object{
			Class:      collection,
			Properties: map[string]interface{}{propName: i},
		}
	}
	helper.CreateObjectsBatch(t, objects)
	t.Logf("imported %d objects with %s.%s", numObjects, collection, propName)

	// Step 1+2: DELETE both indexes. Order matches the frontend repro:
	// filterable first, then rangeFilters.
	deleteIndex(t, restURI, collection, propName, "filterable")
	deleteIndex(t, restURI, collection, propName, "rangeFilters")

	// Wait for both flags to flip to false. The DELETE handler returns 200
	// as soon as the RAFT command is committed, but the local migrator
	// may finish the bucket removal asynchronously. Polling here keeps
	// the next step deterministic.
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, collection)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name != propName {
				continue
			}
			filtFalse := p.IndexFilterable != nil && !*p.IndexFilterable
			rangeFalse := p.IndexRangeFilters != nil && !*p.IndexRangeFilters
			return filtFalse && rangeFalse
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"both IndexFilterable and IndexRangeFilters must be false after DELETE")

	// Step 3: fire both PUTs in parallel.
	type submitResult struct {
		label    string
		status   int
		taskID   string
		body     string
		started  time.Time
		finished time.Time
	}

	results := make([]submitResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r := submitResult{label: "enable-filterable", started: time.Now()}
		r.status, r.taskID, r.body = submitIndexUpdateRaw(t, restURI, collection, propName,
			`{"filterable":{"enabled":true}}`)
		r.finished = time.Now()
		results[0] = r
	}()
	go func() {
		defer wg.Done()
		r := submitResult{label: "enable-rangeable", started: time.Now()}
		r.status, r.taskID, r.body = submitIndexUpdateRaw(t, restURI, collection, propName,
			`{"rangeable":{"enabled":true}}`)
		r.finished = time.Now()
		results[1] = r
	}()
	wg.Wait()

	for _, r := range results {
		t.Logf("%s: status=%d taskID=%q body=%s", r.label, r.status, r.taskID, r.body)
	}

	// Step 4: classify the outcomes.
	//
	// Acceptable shapes:
	//   - both 202: the two submits raced before either was visible in
	//     ListDistributedTasks. Both must FINISH. (Today's pre-fix repro
	//     hits this and BOTH FAIL.)
	//   - one 202 + one 409: the second submit hit checkReindexConflict.
	//     The 202 must FINISH; the 409 must be retried after to verify
	//     the bucket is buildable on its own.
	//
	// Unacceptable:
	//   - any other status (400, 500, ...).
	//   - any FAILED task (the regression).
	accepted := []submitResult{}
	rejected := []submitResult{}
	for _, r := range results {
		switch r.status {
		case http.StatusAccepted:
			accepted = append(accepted, r)
		case http.StatusConflict:
			rejected = append(rejected, r)
		default:
			t.Fatalf("%s submit returned unexpected status %d: %s", r.label, r.status, r.body)
		}
	}
	require.GreaterOrEqual(t, len(accepted), 1, "at least one submit must be accepted")
	require.LessOrEqual(t, len(rejected), 1, "at most one submit may be rejected as conflict")
	if len(rejected) == 1 {
		assert.Contains(t, rejected[0].body, "overlapping properties",
			"409 body should explain the conflict in plain terms")
	}

	// Step 5: wait for accepted tasks to FINISH. Bug repro: BOTH transition
	// to FAILED with "marking reindex progress: ... no such file or
	// directory". After fix: both must FINISH.
	for _, r := range accepted {
		err := awaitTask(t, restURI, r.taskID, 3*time.Minute)
		require.NoError(t, err,
			"%s task %s must FINISH; if it FAILED with 'no such file or directory' "+
				"the parallel-cleanup race regressed (weaviate/weaviate#10675)",
			r.label, r.taskID)
	}

	// Step 6: if one submit was rejected as a 409 conflict, retry it now
	// — the other migration has finished, so the retry should be accepted
	// and finish cleanly. This verifies the user-facing serialization
	// contract: "wait for the first to finish, then retry the second".
	for _, r := range rejected {
		body := `{"filterable":{"enabled":true}}`
		if strings.Contains(r.label, "rangeable") {
			body = `{"rangeable":{"enabled":true}}`
		}
		// Poll until the retry succeeds (the previous task's FINISHED
		// status needs to propagate through the DTM scheduler before
		// checkReindexConflict stops blocking).
		var retryStatus int
		var retryTaskID, retryBody string
		require.Eventually(t, func() bool {
			retryStatus, retryTaskID, retryBody = submitIndexUpdateRaw(t, restURI, collection, propName, body)
			return retryStatus == http.StatusAccepted
		}, 60*time.Second, 1*time.Second,
			"retry of %s after the in-flight task finished must eventually be accepted; "+
				"last response was status=%d body=%s", r.label, retryStatus, retryBody)
		require.NoError(t, awaitTask(t, restURI, retryTaskID, 3*time.Minute),
			"retried %s task %s must FINISH", r.label, retryTaskID)
	}

	// Step 7: assert both schema flags flipped to true.
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, collection)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name != propName {
				continue
			}
			filtOn := p.IndexFilterable != nil && *p.IndexFilterable
			rangeOn := p.IndexRangeFilters != nil && *p.IndexRangeFilters
			return filtOn && rangeOn
		}
		return false
	}, 60*time.Second, 500*time.Millisecond,
		"both IndexFilterable and IndexRangeFilters must be true after both migrations finish")

	// Step 8: queries against both buckets must return data. The filterable
	// bucket gives us Equal (served by Aggregate count via the equal index);
	// the rangeable bucket gives us LessThan via Get with a where filter
	// (Aggregate's LessThan can resolve through the filterable bucket on
	// some plans, so the rangeable-bucket assertion uses Get + _additional).
	require.Greater(t, countWithEqualFilter(t, restURI, collection, propName, 0), 0,
		"Equal filter on %s.%s must return >=1 hit; if 0, the filterable bucket is empty "+
			"(parallel cleanup race left the migration dir torn)", collection, propName)
	require.Greater(t, getHitsWithLessThan(t, restURI, collection, propName, numObjects), 0,
		"LessThan filter on %s.%s via Get must return >=1 hit; if 0, the rangeable bucket is empty",
		collection, propName)
}

// =============================================================================
// Helpers specific to this test (others reused from concurrent_test.go).
// =============================================================================

// submitIndexUpdateRaw is a non-asserting variant of submitIndexUpdate that
// returns the raw status code + body so the caller can branch on 202 vs 409.
func submitIndexUpdateRaw(t *testing.T, restURI, collection, property, jsonBody string) (int, string, string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "index update request failed")
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(bodyBytes)

	if resp.StatusCode != http.StatusAccepted {
		return resp.StatusCode, "", bodyStr
	}
	var result map[string]string
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return resp.StatusCode, "", bodyStr
	}
	return resp.StatusCode, result["taskId"], bodyStr
}

// deleteIndex calls DELETE /v1/schema/{class}/properties/{prop}/index/{indexName}.
// indexName values: "filterable", "searchable", "rangeFilters" (the URL spelling).
func deleteIndex(t *testing.T, restURI, class, prop, indexName string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/properties/%s/index/%s",
		restURI, class, prop, indexName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"DELETE %s on %s/%s must return 200; got %d: %s",
		indexName, class, prop, resp.StatusCode, string(body))
}

// getHitsWithLessThan runs a GraphQL Get query with a LessThan filter on
// a numeric property and returns the number of returned objects.
// LessThan on int/number/date is served by the rangeable bucket, so a
// non-zero hit count post-migration verifies the bucket has been
// populated.
func getHitsWithLessThan(t *testing.T, restURI, collection, propName string, lessThan int) int {
	t.Helper()
	gqlBody := fmt.Sprintf(`{
		"query": "{ Get { %s(where: {path: [\"%s\"], operator: LessThan, valueInt: %d}, limit: 10000) { _additional { id } } } }"
	}`, collection, propName, lessThan)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader([]byte(gqlBody)),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(respBody, &result), "response: %s", string(respBody))

	if errs, ok := result["errors"].([]interface{}); ok && len(errs) > 0 {
		t.Logf("graphql Get LessThan errors: %v (body=%s)", errs, string(respBody))
		return 0
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return 0
	}
	getMap, ok := data["Get"].(map[string]interface{})
	if !ok {
		return 0
	}
	items, ok := getMap[collection].([]interface{})
	if !ok {
		return 0
	}
	return len(items)
}
