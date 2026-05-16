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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas
// pins GH 0-weaviate-issues#212 Issue G as observed by Frontend Claude
// on `1.38.0-dev-3e78517` (the merge sha of 64249a9efc):
//
//   - Phase 8-final on the 1M-record demo returned `path = 0 / 0 / 0`
//     across all 3 LB calls after: forward migrations → rolling restart
//     → reset → re-apply migrations. The searchable bucket was
//     deterministically empty on every replica.
//
// The Phase 8-final sequence is "rolling restart followed by 3
// concurrent forward migrations on the same shard's objects bucket",
// which is the same FlushAndSwitch + CursorOnDisk race that drove
// Issues C + D (see the
// TestMultiNode_ConcurrentDifferentMigrations_ExactCountsPostSettle
// regression). The post-restart shard-init path then resumes via
// OnAfterLsmInitAsync, which uses the same uuidObjectsIteratorAsync —
// so the same Cursor() fix at the iterator should cover this scenario
// too. This test pins that behaviour.
//
// Shape:
//
//  1. Create class with `price` (int, rangeable off), `category`
//     (text, filterable off), `path` (text, tokenization=word).
//  2. Import 10k objects with consistency_level=ALL.
//  3. Run forward migrations: enable-rangeable + enable-filterable
//     + change-tokenization (word → field). All 3 in parallel,
//     await FINISHED.
//  4. Rolling restart (one node at a time, wait for ready between
//     each).
//  5. Re-apply forward migrations: change-tokenization (field → word)
//     in parallel with two NO-OP rebuilds (filterable.rebuild +
//     rangeable.rebuild) that share the same FlushAndSwitch +
//     iterator path as enable-* but don't require pre-disabling.
//  6. After settle, assert every replica's counts equal baseline.
//
// A failure here with my Cursor() fix already in place would mean the
// post-restart re-apply has a separate bug beyond the FlushAndSwitch
// race — likely something restart-specific in the recovery /
// FinalizeCompletedMigrations path.
func TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "PostRestartReapply"
	const totalObjects = 10_000

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
	// Defer-resolved URI: rolling restart reallocates ports.
	defer func() {
		deleteCollection(t, restURIOf(compose, 1), className)
	}()

	const (
		priceLo            = 3000
		priceHi            = 4000
		expectedPriceCount = 501
		expectedCatCount   = 2500
		expectedPathCount  = 2000
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

	// === Phase 2 equivalent: forward migrations (the same shape as the
	// passing concurrent test). The Cursor() fix already guarantees
	// these settle correctly; we run them here as a precondition so the
	// rolling restart in Phase 4 has something interesting to recover.
	uri1 := restURIOf(compose, 1)
	{
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitIndexUpdate(t, uri1, className, "price",
				`{"rangeable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = submitIndexUpdate(t, uri1, className, "category",
				`{"filterable":{"enabled":true}}`)
		}()
		go func() {
			defer wg.Done()
			tk = submitIndexUpdate(t, uri1, className, "path",
				`{"searchable":{"tokenization":"field"}}`)
		}()
		wg.Wait()
		awaitReindexFinished(t, uri1, tp)
		awaitReindexFinished(t, uri1, tc)
		awaitReindexFinished(t, uri1, tk)
	}
	time.Sleep(3 * time.Second)

	// Pre-restart sanity: every replica returns baseline.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		require.Equal(t, expectedPriceCount, gotPrice,
			"pre-restart node %d price = %d (expected %d)", nodeIdx, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(uri, className, "category", categories[0])
		require.NoError(t, err)
		require.Equal(t, expectedCatCount, gotCat,
			"pre-restart node %d category = %d (expected %d)", nodeIdx, gotCat, expectedCatCount)
		gotPath, err := equalCount(uri, className, "path", paths[0])
		require.NoError(t, err)
		require.Equal(t, expectedPathCount, gotPath,
			"pre-restart node %d path = %d (expected %d)", nodeIdx, gotPath, expectedPathCount)
	}

	// === Phase 7 equivalent: rolling restart.
	t.Log("rolling restart")
	restartCluster(ctx, t, compose)

	// After restart re-resolve every URI on use; testcontainers
	// reallocates ports across stop+start.

	// Post-restart baseline: every replica still serves the
	// already-migrated buckets correctly.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		require.Equalf(t, expectedPriceCount, gotPrice,
			"post-restart node %d price = %d (expected %d) — restart corrupted the rangeable bucket", nodeIdx, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(uri, className, "category", categories[0])
		require.NoError(t, err)
		require.Equalf(t, expectedCatCount, gotCat,
			"post-restart node %d category = %d (expected %d) — restart corrupted the filterable bucket", nodeIdx, gotCat, expectedCatCount)
		gotPath, err := equalCount(uri, className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, gotPath,
			"post-restart node %d path = %d (expected %d) — restart corrupted the searchable bucket", nodeIdx, gotPath, expectedPathCount)
	}

	// === Phase 8-final equivalent: re-apply migrations as a
	// repeat-forward. Use three concurrent rebuild-style ops on the
	// already-migrated indexes: change-tokenization back-and-forth
	// (field → word for path), plus rangeable rebuild and filterable
	// rebuild on the other two props. All three go through the same
	// OnAfterLsmInitAsync iterator path that #212 Issues C/D/G hit.
	t.Log("submitting post-restart re-apply migrations (3 concurrent)")
	uri1 = restURIOf(compose, 1)
	{
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitIndexUpdate(t, uri1, className, "price",
				`{"rangeable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			tc = submitIndexUpdate(t, uri1, className, "category",
				`{"filterable":{"rebuild":true}}`)
		}()
		go func() {
			defer wg.Done()
			// Flip tokenization back to word (the pre-Phase-2 value).
			// This is the same migration shape Frontend Claude ran
			// in Phase 8.
			tk = submitIndexUpdate(t, uri1, className, "path",
				`{"searchable":{"tokenization":"word"}}`)
		}()
		wg.Wait()
		t.Logf("submitted post-restart re-apply migrations: price=%s category=%s path=%s",
			tp, tc, tk)
		awaitReindexFinished(t, uri1, tp)
		awaitReindexFinished(t, uri1, tc)
		awaitReindexFinished(t, uri1, tk)
	}
	time.Sleep(3 * time.Second)

	// Final per-replica counts. The path query is the headline check
	// for Issue G (Frontend Claude saw `0 / 0 / 0` here).
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(uri, className, "price", priceLo, priceHi)
		assert.NoError(t, err, "post-reapply price query node %d", nodeIdx)
		assert.Equalf(t, expectedPriceCount, gotPrice,
			"GH #212 Issue G regression: post-restart re-apply node %d price = %d (expected %d) — rangeable rebuild lost data after restart",
			nodeIdx, gotPrice, expectedPriceCount)

		gotCat, err := equalCount(uri, className, "category", categories[0])
		assert.NoError(t, err, "post-reapply category query node %d", nodeIdx)
		assert.Equalf(t, expectedCatCount, gotCat,
			"GH #212 Issue G regression: post-restart re-apply node %d category = %d (expected %d) — filterable rebuild lost data after restart",
			nodeIdx, gotCat, expectedCatCount)

		gotPath, err := equalCount(uri, className, "path", paths[0])
		assert.NoError(t, err, "post-reapply path query node %d", nodeIdx)
		assert.Equalf(t, expectedPathCount, gotPath,
			"GH #212 Issue G regression: post-restart re-apply node %d path = %d (expected %d) — change-tokenization lost data after restart (Phase-8-final shape)",
			nodeIdx, gotPath, expectedPathCount)
	}

	// LB-side 3-call stability spot check.
	for i := 0; i < 3; i++ {
		gotPath, err := equalCount(restURIOf(compose, 1), className, "path", paths[0])
		require.NoError(t, err)
		assert.Equalf(t, expectedPathCount, gotPath,
			"GH #212 Issue G regression: post-restart LB-side path call #%d = %d (expected %d)",
			i+1, gotPath, expectedPathCount)
	}
}
