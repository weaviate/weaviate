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
)

// TestMultiNode_RepeatedParallelMigrationJourney_PerReplicaConsistency
// pins the gap that GH 0-weaviate-issues#212 Frontend Claude observed at
// 1M-record scale and that none of the existing acceptance tests catch:
//
//   - Phase 7 (post-restart) and Phase 8-final (post-restart-then-re-apply)
//     queries against `spec_sheet_path` return *different* counts on
//     each replica, with the same on-disk state across all 3 pods
//     (Frontend confirmed via kubectl exec on every pod). The expected
//     count is never returned. This is in-memory bucket-pointer
//     divergence after the rolling restart.
//
// What this test does differently than the existing
// TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas:
//
//  1. **Repeated journey** — does the full "3 parallel migrations + reset"
//     cycle 2 times PRE-restart. Existing test does one forward + one
//     restart + one re-apply, leaving the system at a much simpler
//     state pre-restart. Frontend's flow leaves multiple generations of
//     migration trackers + sidecar dirs on disk before the restart.
//  2. **Per-replica histogram** — runs the same query 30 times *per
//     replica directly* (90 total queries hitting all 3 replicas
//     evenly), mirroring Frontend's LB-fanout poll pattern. The
//     existing test runs each query once per replica, which a flap of
//     `7×32, 3×30, 2×23, 6×5` would only catch if the test's single
//     poll happens to hit the bad replica. With 30 samples per replica
//     we deterministically detect divergence.
//  3. **Higher object count** — 25k objects (up from 10k in
//     issue_g_post_restart_reapply_test.go). Frontend's bug is
//     LSM-segment-layout sensitive; small datasets keep most data in
//     the memtable and miss the on-disk segment race regime.
//
// A failure of this test indicates the per-replica divergence is real
// at <100k scale and exposes it for local debugging. A pass means we
// need to push further (higher object count, more journey iterations,
// or different prop mix) to reproduce.
func TestMultiNode_RepeatedParallelMigrationJourney_PerReplicaConsistency(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "RepeatedJourney"
		totalObjects = 25_000
		// 2 forward-and-reset cycles before the restart. Each cycle:
		//   forward: enable-rangeable + enable-filterable + change-tok(word→field)
		//   reset:   rangeable.rebuild + filterable.rebuild + change-tok(field→word)
		// After the cycles the schema is back at the original WORD
		// tokenization with all properties' indexes already through 2
		// round-trips. Then we restart and assert per-replica.
		preRestartCycles = 2
	)

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
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	const (
		priceLo = 3000
		priceHi = 4000
		// price = i*2 for i in [0, 25000); inclusive range [3000, 4000]
		// covers i in [1500, 2000] → 501 objects.
		expectedPriceCount = 501
		// 25k / 4 categories
		expectedCatCount = 6250
		// 25k / 5 paths
		expectedPathCount = 5000
	)
	categories := []string{"alpha", "beta", "gamma", "delta"}
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"price":    i * 2,
			"category": categories[i%len(categories)],
			"path":     paths[i%len(paths)],
		}
	})

	// First forward migration (parallel): the only one that's an
	// "enable" since the initial schema has filterable=false +
	// rangeable=false on those props. After this the props are
	// permanently filterable/rangeable so subsequent cycles use
	// "rebuild" instead.
	uri := restURIOf(compose, 1)
	t.Log("initial enable migrations (cycle 0)")
	{
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitIndexUpdate(t, uri, className, "price",
				`{"rangeable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = submitIndexUpdate(t, uri, className, "category",
				`{"filterable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tk = submitIndexUpdate(t, uri, className, "path",
				`{"searchable":{"tokenization":"field"}}`)
		}()
		wg.Wait()
		awaitReindexFinished(t, uri, tp)
		awaitReindexFinished(t, uri, tc)
		awaitReindexFinished(t, uri, tk)
	}
	time.Sleep(2 * time.Second)

	// Repeated journey: rebuild + tokenization round-trip, N times. The
	// path tokenization alternates field↔word; price+category use
	// rebuild (no-op data-wise but exercises the FlushAndSwitch and
	// generation-trim path identically).
	for cycle := 1; cycle <= preRestartCycles; cycle++ {
		// Alternate path tokenization. After enable above schema is at
		// "field"; cycle 1 → "word", cycle 2 → "field", etc.
		nextTok := "word"
		if cycle%2 == 0 {
			nextTok = "field"
		}
		t.Logf("repeated journey cycle %d: rebuilds + tok→%s", cycle, nextTok)
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitIndexUpdate(t, uri, className, "price",
				`{"rangeable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = submitIndexUpdate(t, uri, className, "category",
				`{"filterable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			tk = submitIndexUpdate(t, uri, className, "path",
				fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, nextTok))
		}()
		wg.Wait()
		awaitReindexFinished(t, uri, tp)
		awaitReindexFinished(t, uri, tc)
		awaitReindexFinished(t, uri, tk)
		time.Sleep(2 * time.Second)
	}

	// After preRestartCycles cycles, the path tokenization is:
	//   cycle 1 → word
	//   cycle 2 → field
	// For preRestartCycles=2 we land at "field" (every value in `paths`
	// is one single FIELD token; equalCount returns expectedPathCount).
	finalTok := "field"
	if preRestartCycles%2 == 1 {
		finalTok = "word"
	}
	t.Logf("post-cycles schema tokenization = %s; preparing rolling restart", finalTok)

	// Pre-restart per-replica sanity. Every replica must return the
	// baseline for every prop before we restart — otherwise the
	// post-restart assertion is meaningless.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		preURI := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(preURI, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		require.Equalf(t, expectedPriceCount, gotPrice,
			"PRE-restart node %d price = %d (expected %d)", nodeIdx, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(preURI, className, "category", categories[0])
		require.NoError(t, err)
		require.Equalf(t, expectedCatCount, gotCat,
			"PRE-restart node %d category = %d (expected %d)", nodeIdx, gotCat, expectedCatCount)
		gotPath, err := equalCount(preURI, className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, gotPath,
			"PRE-restart node %d path = %d (expected %d)", nodeIdx, gotPath, expectedPathCount)
	}

	// Rolling restart (one node at a time, wait for ready between
	// each). This is the same shape as Kubernetes StatefulSet rolling
	// updates, which is Frontend Claude's deployment model.
	t.Log("rolling restart cluster")
	rollingRestartCluster(ctx, t, compose)

	// Settle: testcontainers reallocates ports across stop+start, so
	// re-resolve URIs on every use below. Give shard-init time to
	// finalize the deferred migration dirs (FinalizeCompletedMigrations
	// + bucket loading).
	time.Sleep(5 * time.Second)

	// === Headline assertion: per-replica histogram.
	//
	// Hit each replica directly 30 times for the path query (90 total).
	// If the in-memory bucket pointer diverged across replicas, the
	// histogram will show distinct values per replica (Frontend's
	// `7×32, 3×30, 2×23, 6×5` shape). If the buckets are consistent,
	// every poll returns expectedPathCount.
	const pollsPerReplica = 30

	for _, prop := range []struct {
		name, kind, value string
		expected          int
	}{
		{"price", "range", "", expectedPriceCount},
		{"category", "equal", categories[0], expectedCatCount},
		{"path", "equal", paths[0], expectedPathCount},
	} {
		histogram := make(map[string]map[int]int) // nodeID -> count -> N
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			nodeKey := fmt.Sprintf("node-%d", nodeIdx)
			histogram[nodeKey] = map[int]int{}
			for i := 0; i < pollsPerReplica; i++ {
				postURI := restURIOf(compose, nodeIdx)
				var got int
				var err error
				switch prop.kind {
				case "range":
					got, err = rangeCount(postURI, className, prop.name, priceLo, priceHi)
				case "equal":
					got, err = equalCount(postURI, className, prop.name, prop.value)
				}
				if err != nil {
					// Treat a query error as a distinct histogram bucket
					// at -1 so it's visible in the failure log.
					histogram[nodeKey][-1]++
					continue
				}
				histogram[nodeKey][got]++
			}
		}

		// Pass condition: every (node, poll) returned the expected count.
		allCorrect := true
		for _, perNode := range histogram {
			for value, n := range perNode {
				if value != prop.expected || n != pollsPerReplica {
					allCorrect = false
				}
			}
		}

		if !allCorrect {
			// Render histogram in the failure message so the per-replica
			// flap shape is visible — this is the smoking gun for
			// per-replica bucket-pointer divergence.
			lines := []string{
				fmt.Sprintf("GH #212 per-replica divergence on %q (expected %d, %d polls/replica):",
					prop.name, prop.expected, pollsPerReplica),
			}
			for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
				nodeKey := fmt.Sprintf("node-%d", nodeIdx)
				lines = append(lines,
					fmt.Sprintf("  %s: %+v", nodeKey, histogram[nodeKey]))
			}
			assert.Fail(t, "per-replica divergence", "%s", joinLines(lines))
		}
	}

	// === LB-side spot check (independent of per-replica direct
	// queries). 9 round-robin calls across nodes — Frontend uses ~90
	// LB calls in their poll loop, but 9 is enough to catch a clear
	// `0/3/2` per-replica divergence pattern.
	t.Log("LB-side 9-call spot check across nodes")
	for i := 0; i < 9; i++ {
		nodeIdx := (i % 3) + 1
		lbURI := restURIOf(compose, nodeIdx)
		gotPath, err := equalCount(lbURI, className, "path", paths[0])
		assert.NoError(t, err, "LB-side path query #%d (node %d)", i+1, nodeIdx)
		assert.Equalf(t, expectedPathCount, gotPath,
			"GH #212 LB-side path query #%d (node %d) = %d, expected %d",
			i+1, nodeIdx, gotPath, expectedPathCount)
	}
}

func joinLines(lines []string) string {
	var b []byte
	for i, l := range lines {
		if i > 0 {
			b = append(b, '\n')
		}
		b = append(b, l...)
	}
	return string(b)
}
