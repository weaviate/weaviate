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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// TestLiveQueriesDuringChangeTokenization pins the tight-bound
// behavior of the per-shard tokenization overlay introduced for
// 0-weaviate-issues#216 Gap B. Without the overlay, the FINALIZING
// window of a change-tokenization migration leaks a per-replica
// mismatch where the bucket pointer has flipped to NEW-tokenized data
// but the cluster-wide schema flag still serves the OLD tokenization
// to the query analyzer. Frontend Claude observed this as a
// `7→4→1→7→3→2` count flap on a steady BM25 probe across replicas —
// 6 consecutive non-baseline, non-zero samples spanning hundreds of
// milliseconds.
//
// With the overlay, each shard's query input tokenizes against the
// value matching its bucket content throughout the migration:
//
//   - Pre-swap: overlay=NEW, bucket=OLD (in-memory swap latency
//     bounds this to microseconds; query returns 0 not partial).
//   - Post-swap: overlay=NEW, bucket=NEW → query returns baseline.
//   - Post-flip: overlay self-clears or is explicitly cleared by
//     OnTaskCompleted; live schema=NEW matches bucket=NEW → query
//     returns baseline.
//
// The only partials this test can observe are:
//
//  1. Brief (~µs) per-shard windows during the bucket pointer flip.
//     At a 25ms probe interval × 3 nodes × 3 shards (=9 replicas) the
//     expected hit count per migration is <1.
//  2. Race between OnTaskCompleted's overlay clear and the schema
//     flip's local FSM apply — by construction, the clear happens
//     AFTER the schema flip commits on this node, and
//     TokenizationFor's self-clear-on-catchup branch produces the
//     same value as the explicit clear, so this window is effectively
//     zero.
//
// The bounded count is a regression budget: any number above `5`
// across the whole migration indicates the overlay is not threading
// correctly through some query call site (BM25Searcher, Searcher,
// aggregator) or the set/clear hooks in OnGroupCompleted /
// OnTaskCompleted are landing at the wrong point in the FSM.
//
// Coverage: forward (word→field), reverse (field→word), filterable
// variant. Each subtest creates a fresh collection on the shared
// 3-node cluster.
func TestLiveQueriesDuringChangeTokenization(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	// probeFn is the per-case query function: it takes a node URI and
	// the class name, returns (count, err). Different probe types
	// exercise different query paths (BM25 via bm25Searcher, Equal
	// filter via Searcher) — both of which are wired through the
	// per-shard tokenization overlay.
	type probeFn func(restURI, className string) (int, error)

	cases := []struct {
		name       string
		startTok   string // tokenization at class creation
		targetTok  string // tokenization after migration
		indexType  string // "searchable" (change-tokenization) or "filterable" (change-tokenization-filterable)
		probeQuery string // query string passed to the probe
		probe      probeFn
		// expectedAfter is the count the probe should yield after the
		// migration commits. We use it only as a sanity check; the
		// in-flight assertion is the partial-sample budget.
		expectedAfter int
	}{
		{
			name:       "forward_word_to_field_searchable",
			startTok:   "word",
			targetTok:  "field",
			indexType:  "searchable",
			probeQuery: "alpha",
			probe: func(uri, cn string) (int, error) {
				return queryBM25Count(uri, cn, "alpha", 2000)
			},
			expectedAfter: 0, // "alpha" alone matches none under FIELD
		},
		{
			name:       "reverse_field_to_word_searchable",
			startTok:   "field",
			targetTok:  "word",
			indexType:  "searchable",
			probeQuery: "alpha doc number 0 filler",
			probe: func(uri, cn string) (int, error) {
				return queryBM25Count(uri, cn, "alpha doc number 0 filler", 2000)
			},
			expectedAfter: 1, // full-text matches exactly one object under FIELD only
		},
		{
			name:       "forward_word_to_field_filterable",
			startTok:   "word",
			targetTok:  "field",
			indexType:  "filterable",
			probeQuery: "alpha",
			// Equal filter exercises the Searcher (not BM25Searcher)
			// path via the aggregator's buildAllowList. Under WORD
			// tokenization, Equal("alpha") on the filterable bucket
			// matches every doc; under FIELD it matches none.
			probe: func(uri, cn string) (int, error) {
				return equalCount(uri, cn, "text", "alpha")
			},
			expectedAfter: 0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runLiveQueryDuringChangeTokenizationCase(t, compose, tc.startTok,
				tc.targetTok, tc.indexType, tc.probeQuery, tc.probe, tc.expectedAfter)
		})
	}
}

func runLiveQueryDuringChangeTokenizationCase(
	t *testing.T,
	compose *docker.DockerCompose,
	startTok, targetTok, indexType, probeQuery string,
	probe func(restURI, className string) (int, error),
	expectedAfter int,
) {
	const (
		shardCount    = 3
		rf            = 3
		objectCount   = 1500
		batchSize     = 100
		probeInterval = 25 * time.Millisecond
		// partialSampleBudget is the post-#216 tight bound. Pre-fix,
		// TestPartialResultsDuringChangeTokenization allowed up to 15
		// partial samples in a 700ms window; this test asserts the
		// per-shard alignment achieves an order of magnitude tighter
		// budget. See test godoc for the upper-bound derivation.
		partialSampleBudget = 5
		partialWindowBudget = 150 * time.Millisecond
	)

	className := fmt.Sprintf("LiveQueryTok_%s_%s_%s", startTok, targetTok, indexType)

	createCollection(t, compose.GetWeaviateNode(1).URI(), className, shardCount, rf,
		[]*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: startTok},
		})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	// Generate `objectCount` documents each containing the literal token
	// "alpha" and the full-text phrase variant. Under WORD tokenization,
	// BM25 "alpha" matches all objects; under FIELD, "alpha" matches
	// none because the entire phrase is one token. The full-phrase
	// query matches one specific object under FIELD only.
	texts := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		texts = append(texts, fmt.Sprintf("alpha doc number %d filler", i))
	}
	batchImport(t, compose.GetWeaviateNode(1).URI(), className, texts, batchSize)

	// Baseline: at startTok, the probe query must return >0 objects so
	// the FINALIZING-window partial samples are visible against a
	// stable baseline. If the probe matches zero at startTok we'd
	// classify steady-state samples as partial, blowing the budget.
	baselineCount, err := probe(compose.GetWeaviateNode(1).URI(), className)
	require.NoError(t, err)
	require.Greater(t, baselineCount, 0,
		"baseline (%s tokenization) must return a stable count > 0 against probe %q "+
			"so partial samples during the migration are detectable", startTok, probeQuery)
	t.Logf("baseline (start=%s) count: %d", startTok, baselineCount)

	// Start probing every node every 25ms throughout the migration.
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
				count, perr := probe(nodeURI, className)
				record(sample{t: start, nodeID: idx, count: count, err: perr})
				time.Sleep(probeInterval)
			}
		}()
	}

	// Build the index-update JSON for the requested migration type.
	var indexUpdateJSON string
	switch indexType {
	case "searchable":
		indexUpdateJSON = fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTok)
	case "filterable":
		indexUpdateJSON = fmt.Sprintf(`{"filterable":{"tokenization":%q}}`, targetTok)
	default:
		t.Fatalf("unsupported indexType %q", indexType)
	}

	migrationStart := time.Now()
	taskID := submitIndexUpdate(t, compose.GetWeaviateNode(1).URI(), className, "text", indexUpdateJSON)
	t.Logf("submitted change-tokenization-%s task: %s", indexType, taskID)

	awaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID)
	require.Eventually(t, func() bool {
		return tryGetPropertyTokenization(compose.GetWeaviateNode(1).URI(),
			className, "text") == targetTok
	}, 60*time.Second, 200*time.Millisecond,
		"tokenization should change to %s after swap phase", targetTok)

	// Let probes run a little longer to capture the post-flip steady
	// state (so any racy late partial samples are observable).
	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("migration completed in %v, collected %d probe samples",
		time.Since(migrationStart), len(samples))

	// Classify samples relative to the start-tok baseline. Any value
	// in (0, baselineCount) is partial — the bucket and the query
	// analyzer disagreed at the moment of the probe.
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
			t.Logf("partial @ +%v node=%d count=%d (baseline=%d)",
				s.t.Sub(migrationStart).Round(time.Millisecond), s.nodeID, s.count, baselineCount)
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

	// Sanity check that the post-flip steady state is what we expect
	// (not just zero everywhere — that'd be silent failure).
	postCount, perr := probe(compose.GetWeaviateNode(1).URI(), className)
	require.NoError(t, perr, "post-migration probe must succeed")
	assert.Equal(t, expectedAfter, postCount,
		"post-migration (%s tokenization) probe count should equal %d", targetTok, expectedAfter)

	// The #216-fix assertion: partial samples must be rare AND
	// confined to a brief window. Both bounds matter:
	//
	//   - partialSampleBudget catches a wide-open misalignment (the
	//     overlay isn't being read on the query path at all).
	//   - partialWindowBudget catches a slow drain (the overlay clears
	//     correctly but bounded by something slow like RAFT rather
	//     than the in-memory pointer flip).
	assert.LessOrEqualf(t, partialN, partialSampleBudget,
		"observed %d partial samples (budget=%d) for %s→%s on %s — "+
			"per-shard tokenization overlay is not keeping the query "+
			"analyzer aligned with bucket content during the FINALIZING "+
			"window. Investigate the resolver wiring (Shard.TokenizationFor, "+
			"BM25Searcher.WithTokenizationResolver, Searcher.WithTokenizationResolver, "+
			"aggregator.New tokResolver parameter) and the set/clear hooks "+
			"(OnGroupCompleted, OnTaskCompleted).",
		partialN, partialSampleBudget, startTok, targetTok, indexType)

	if partialN > 0 {
		windowDuration := lastPartial.Sub(firstPartial)
		assert.LessOrEqualf(t, windowDuration, partialWindowBudget,
			"partial-results window of %v exceeds budget of %v for %s→%s on %s — "+
				"a partial sample arrived long after the in-memory swap completed; "+
				"either the overlay is being cleared prematurely (before the schema "+
				"flip lands) or the swap and clear are not in the expected FSM order.",
			windowDuration, partialWindowBudget, startTok, targetTok, indexType)
	}

	// Post-window guarantee: after the bounded partial window closes
	// (or, if there were zero partials, after the migration finished),
	// every sample must be a full or empty count. A late partial
	// indicates the cutover is not converging.
	var afterAnchor time.Time
	if !lastPartial.IsZero() {
		afterAnchor = lastPartial.Add(50 * time.Millisecond)
	} else {
		// No partials observed — anchor at migration start; any later
		// non-{0,baseline} sample is a late partial.
		afterAnchor = migrationStart
	}
	var latePartial int
	for _, s := range samples {
		if s.err != nil {
			continue
		}
		if s.t.After(afterAnchor) && s.count != 0 && s.count != baselineCount {
			latePartial++
			t.Logf("late partial @ +%v node=%d count=%d (after anchor)",
				s.t.Sub(migrationStart).Round(time.Millisecond), s.nodeID, s.count)
		}
	}
	assert.Zerof(t, latePartial,
		"observed %d partial samples after the bounded cutover window for %s→%s on %s — "+
			"cutover is not converging; either the overlay isn't being cleared "+
			"or the schema flip isn't propagating to every replica.",
		latePartial, startTok, targetTok, indexType)
}
