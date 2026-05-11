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

// TestPartialResultsDuringChangeTokenization documents the cluster-wide
// partial-results window that opens during a change-tokenization reindex
// across multiple shards / nodes.
//
// Failure mode (expected to be observable, hence this test starts RED):
//
//  1. All shards finish the reindex phase (barrier).
//  2. On each node, the scheduler tick fires OnGroupCompleted, which calls
//     RunSwapOnShard sequentially for every LOCAL shard. The FIRST shard's
//     swap on the FIRST node to fire performs OnMigrationComplete →
//     UpdatePropertyInternal → Raft. The new tokenization is now visible
//     cluster-wide.
//  3. Between that Raft apply and the moment every other shard / node
//     has finished its own RunSwapOnShard, queries are parsed under the
//     new tokenization while some shards still serve indexes built for
//     the OLD tokenization. The shard mixture is "partial" — neither the
//     full pre-migration result set nor the empty post-migration set.
//
// We probe this by:
//
//   - Importing enough objects (~1500) so each shard's reindex takes
//     measurably long (>1s) and the cluster-wide cutover spans more
//     than one scheduler tick.
//   - Issuing a single-word BM25 query for "alpha" against node 1
//     continuously throughout the migration.
//   - Asserting the result count is always either the FULL pre-migration
//     count or zero (the post-migration count for FIELD tokenization).
//     Any intermediate count is a partial-results observation.
//
// The test is expected to FAIL today, exposing the cluster-wide cutover
// gap. Once the gap is closed (e.g. by deferring the schema flip until
// every node has finished swapping locally), this test will become green.
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

	// The assertion: at every point in time the cluster should serve
	// either the pre-migration count or the post-migration count. Any
	// other value is a partial-cutover observation. This is expected to
	// FAIL on the current implementation.
	assert.Zero(t, partialN,
		"observed %d probe samples with partial result counts during the "+
			"cluster-wide tokenization cutover — queries served incomplete data",
		partialN)
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
