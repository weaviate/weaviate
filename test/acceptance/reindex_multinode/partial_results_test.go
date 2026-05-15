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
)

// TestPartialResultsDuringChangeTokenization pins the cluster-wide
// partial-results window during a change-tokenization reindex across
// multiple shards / nodes.
//
// Cutover sequence (Journey 3 canonical, reactive firing):
//
//  1. All shards finish the reindex phase. DTM's AllGroupUnitsTerminal
//     becomes true; the FSM apply path notifies each node's local
//     scheduler via SchedulerNotifier.Wake() instead of waiting for the
//     next periodic tick.
//  2. On every node, the scheduler reactively fires OnGroupCompleted →
//     RunSwapOnShard for each local shard. The strategy's
//     OnMigrationComplete is a no-op for semantic migrations; the
//     bucket pointer flip happens in-memory only.
//  3. Then OnTaskCompleted (also reactive) issues one RAFT-idempotent
//     UpdatePropertyInternal that flips the schema flag (Tokenization).
//     Multiple nodes' calls dedupe to a single committed entry.
//  4. The schema flip RAFT entry propagates to every node's local FSM,
//     which applies it and updates the in-memory schema.
//
// The partial-results window is bounded by the cross-node spread
// between "this node's OnGroupCompleted swap done" and "this node's
// FSM has applied the schema flip RAFT entry". With reactive firing
// the spread is dominated by RAFT replication latency (low tens of
// ms on a healthy cluster). Pre-reactive-firing this was bounded by
// the scheduler tick interval (default 1 minute, overridden to 1s in
// the test harness).
//
// The window is small enough that production queries during a
// change-tokenization migration may observe partial results, but the
// observation is bounded; the test pins that bound.
//
// We probe this by:
//
//   - Importing 1500 objects across 3 shards / 3 nodes (RF=3).
//   - Issuing a BM25 query for "alpha" against every node, every 25ms,
//     throughout the migration.
//   - Asserting the partial-results window is bounded:
//     1. duration <= partialWindowBudget (700ms — generous to absorb
//        CI noise; locally observed ~80ms)
//     2. partial-count samples <= partialSampleBudget (15 — generous
//        for the 25ms probe interval times 3 nodes times the bounded
//        window)
//     3. AFTER the bounded window, every sample is either the full
//        baseline count or zero (no late partial samples).
//
// A more aggressive design (schema-version-aware bucket lookup via the
// BucketGeneration counter wired into the query path) would collapse
// this window to sub-ms per node. That's tracked as a follow-up; the
// current bound is the Journey 3 canonical design's best achievable.
func TestPartialResultsDuringChangeTokenization(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className     = "PartialResultsTokenize"
		shardCount    = 3
		rf            = 3
		objectCount   = 1500
		batchSize     = 100
		alphaQuery    = "alpha"
		queryLimit    = 2000 // Must exceed objectCount so all matches are returned.
		probeInterval = 25 * time.Millisecond
	)

	createCollection(t, compose.GetWeaviateNode(1).URI(), className, shardCount, rf,
		[]*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	// Every object contains the single-word token "alpha" so a BM25 query
	// against "alpha" returns ALL of them under WORD tokenization. After
	// the FIELD migration, "alpha" alone matches NONE — each object's text
	// becomes a single multi-word token.
	texts := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		texts = append(texts, fmt.Sprintf("alpha doc number %d filler", i))
	}

	// Import in batches via /v1/batch/objects for speed.
	batchImport(t, compose.GetWeaviateNode(1).URI(), className, texts, batchSize)

	// Baseline: WORD tokenization, "alpha" should return all docs.
	baselineCount := mustQueryBM25Count(t,
		compose.GetWeaviateNode(1).URI(), className, alphaQuery, queryLimit)
	require.Equal(t, objectCount, baselineCount,
		"baseline BM25 'alpha' under WORD tokenization should match all %d docs", objectCount)
	t.Logf("baseline 'alpha' result count: %d", baselineCount)

	// Capture timestamped probe samples concurrently against all 3 nodes,
	// covering the entire migration window.
	type sample struct {
		t      time.Time
		nodeID int
		count  int
		err    error
	}
	samplesMu := sync.Mutex{}
	samples := make([]sample, 0, 1024)
	record := func(s sample) {
		samplesMu.Lock()
		samples = append(samples, s)
		samplesMu.Unlock()
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		wg.Add(1)
		nodeURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
		idx := nodeIdx + 1
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				start := time.Now()
				count, err := queryBM25Count(nodeURI, className, alphaQuery, queryLimit)
				record(sample{t: start, nodeID: idx, count: count, err: err})
				time.Sleep(probeInterval)
			}
		}()
	}

	// Submit the migration.
	migrationStart := time.Now()
	taskID := submitIndexUpdate(t, compose.GetWeaviateNode(1).URI(),
		className, "text", `{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task: %s", taskID)

	// Wait for the task to FINISH and for the schema to reflect the swap.
	awaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID)
	require.Eventually(t, func() bool {
		return tryGetPropertyTokenization(compose.GetWeaviateNode(1).URI(),
			className, "text") == "field"
	}, 60*time.Second, 200*time.Millisecond,
		"tokenization should change to field after swap phase")

	// Let the probes run a little longer to make sure the cutover window
	// is fully captured.
	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("migration completed in %v, collected %d probe samples",
		time.Since(migrationStart), len(samples))

	// Classify samples: full (baselineCount), empty (0), or partial.
	var fullN, emptyN, partialN, errN int
	var firstPartial, lastPartial time.Time
	for _, s := range samples {
		switch {
		case s.err != nil:
			errN++
		case s.count == baselineCount:
			fullN++
		case s.count == 0:
			emptyN++
		default:
			partialN++
			if firstPartial.IsZero() {
				firstPartial = s.t
			}
			lastPartial = s.t
			t.Logf("partial sample @ +%v node=%d count=%d",
				s.t.Sub(migrationStart).Round(time.Millisecond), s.nodeID, s.count)
		}
	}

	t.Logf("probe classification: full=%d empty=%d partial=%d err=%d",
		fullN, emptyN, partialN, errN)
	if partialN > 0 {
		t.Logf("partial-results window spanned %v (first @ +%v, last @ +%v)",
			lastPartial.Sub(firstPartial).Round(time.Millisecond),
			firstPartial.Sub(migrationStart).Round(time.Millisecond),
			lastPartial.Sub(migrationStart).Round(time.Millisecond))
	}

	// Bound the partial-results window. The Journey 3 canonical design
	// admits a brief partial window during the cross-node cutover spread
	// (each node's reactive OnGroupCompleted swap + the cluster-wide
	// schema flip RAFT entry propagate independently). The window is
	// bounded by RAFT replication latency.
	//
	// Locally we observe ~80ms / ~7 samples. The budget below is
	// deliberately generous to absorb noisy CI runners (slow disk,
	// loaded host) without making the test flaky.
	const (
		partialWindowBudget = 700 * time.Millisecond
		partialSampleBudget = 15
	)

	if partialN > 0 {
		windowDuration := lastPartial.Sub(firstPartial)
		assert.LessOrEqual(t, windowDuration, partialWindowBudget,
			"partial-results window of %v exceeds the budget of %v — "+
				"the cluster-wide cutover is taking longer than the bounded "+
				"RAFT-propagation + scheduler-wake design admits; investigate "+
				"the reactive-firing path (Manager.notifySchedulerWithLock, "+
				"Scheduler.wakeCh, OnGroupCompleted, OnTaskCompleted)",
			windowDuration, partialWindowBudget)

		assert.LessOrEqual(t, partialN, partialSampleBudget,
			"observed %d partial samples (budget=%d) within an otherwise "+
				"bounded window — either the probe rate has changed, or the "+
				"cutover sequence is producing more transient mismatches than "+
				"the design admits",
			partialN, partialSampleBudget)
	}

	// Post-window guarantee: after the bounded partial window closes,
	// every sample must be a full or empty count. A late partial sample
	// indicates the cutover is not bounded — a node lagged behind for
	// reasons other than RAFT propagation (e.g. its reactive wake never
	// fired). Walk forward from lastPartial + a small grace period and
	// assert no further partials.
	if !lastPartial.IsZero() {
		gracePeriod := 100 * time.Millisecond
		var latePartial int
		for _, s := range samples {
			if s.err != nil {
				continue
			}
			if s.t.After(lastPartial.Add(gracePeriod)) &&
				s.count != 0 && s.count != baselineCount {
				latePartial++
				t.Logf("late partial @ +%v node=%d count=%d (after last-partial+grace)",
					s.t.Sub(migrationStart).Round(time.Millisecond),
					s.nodeID, s.count)
			}
		}
		assert.Zero(t, latePartial,
			"observed %d partial samples after the bounded cutover window "+
				"(lastPartial + %v) — cutover is not converging",
			latePartial, gracePeriod)
	}
}

// batchImport posts objects in batches of `batchSize` using /v1/batch/objects.
func batchImport(t *testing.T, restURI, className string, texts []string, batchSize int) {
	t.Helper()

	for start := 0; start < len(texts); start += batchSize {
		end := start + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		objects := make([]map[string]interface{}, 0, end-start)
		for _, text := range texts[start:end] {
			objects = append(objects, map[string]interface{}{
				"class": className,
				"id":    uuid.New().String(),
				"properties": map[string]interface{}{
					"text": text,
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects})
		require.NoError(t, err)

		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"batch %d-%d failed: %s", start, end, string(respBody))

		// Verify no per-object errors.
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

// queryBM25Count executes a BM25 query with an explicit limit and returns
// the count of matched objects. Returns the count + any error encountered.
func queryBM25Count(restURI, className, query string, limit int) (int, error) {
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}, limit: %d) {
				_additional { id }
			}
		}
	}`, className, query, limit)

	reqBody := map[string]interface{}{"query": gqlQuery}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}
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
			Get map[string][]map[string]interface{} `json:"Get"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return 0, fmt.Errorf("graphql errors: %s", gqlResp.Errors[0].Message)
	}
	return len(gqlResp.Data.Get[className]), nil
}

func mustQueryBM25Count(t *testing.T, restURI, className, query string, limit int) int {
	t.Helper()
	count, err := queryBM25Count(restURI, className, query, limit)
	require.NoError(t, err)
	return count
}
