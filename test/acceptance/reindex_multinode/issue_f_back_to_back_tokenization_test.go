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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// TestMultiNode_BackToBackChangeTokenization_RoundTripCounts pins GH
// 0-weaviate-issues#212 Issue F as observed by Frontend Claude on the
// `74e6c1d` build (post C+D fix):
//
//   - Phase 5 returned `path=1` (expected 7) — the change-tokenization
//     back-to-original-state on the searchable bucket failed to fully
//     rebuild the bucket. Just one record matched the equality query
//     instead of the expected baseline count.
//   - Phase 8-final later returned `path=7` (expected 5) — the third
//     change-tokenization (forward again) didn't take effect; the
//     searchable bucket stayed at the prior tokenization.
//
// Both anomalies suggest the change-tokenization round-trip leaves
// the searchable bucket in a partial/stale state after the second
// migration. With the C+D fix (`Cursor()` in the object iterator) the
// FlushAndSwitch race is gone, so the data is reaching the reindex
// bucket — the bug must be downstream (runtimeSwap, bucket-pointer,
// or analyzer-overlay against the existing reindex/ingest sidecar
// dirs from the prior migration).
//
// Shape (in-process, single restart-less journey):
//
//  1. Create class with a single text property `path`, tokenization=word.
//  2. Import 10k objects with hyphenated multi-token values
//     (e.g. "alpha-foo", "beta-bar", …). Pick the values so:
//     - The word-tokenized bucket gives N_word matches for a chosen
//     multi-token Equal query (matches every doc whose path
//     contains both tokens).
//     - The field-tokenized bucket gives N_field matches for the
//     same Equal query (matches only exact-string docs).
//     - N_word != N_field.
//  3. Verify the baseline count under WORD tokenization on every
//     replica equals N_word.
//  4. Submit change-tokenization to FIELD. Await FINISHED. Sleep 3 s.
//  5. Verify every replica returns N_field.
//  6. Submit change-tokenization back to WORD. Await FINISHED. Sleep 3 s.
//  7. Verify every replica returns N_word again — and importantly that
//     the count is not 0, 1, or some degenerate partial value, which
//     is the Phase-5 / Phase-8-final shape.
//
// The exact-value assertions are the headline check. The "≥ 2" sanity
// check below also catches the Phase-5 `path=1` degenerate shape even
// if my count math is off by a few in either direction.
func TestMultiNode_BackToBackChangeTokenization_RoundTripCounts(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "BackToBackTokenization"
	const totalObjects = 10_000

	trueVal := true
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() {
		deleteCollection(t, restURIOf(compose, 1), className)
	}()

	// Five distinct path values, hyphenated so WORD tokenization
	// produces 2 tokens per path (color + "shared") and FIELD gives 1
	// token per path. Each path appears 2000 times.
	//   path values:
	//     "red-shared", "green-shared", "blue-shared", "yellow-shared", "purple-shared"
	//
	// Equality query: path = "red-shared".
	//   FIELD tokenization → exact match → 2000 docs (only the red ones)
	//   WORD tokenization → AND-of-tokens ["red", "shared"] → 2000 docs
	//     (only red ones contain "red"; everyone contains "shared", AND is 2000)
	//
	// To make N_word != N_field we need a query that distinguishes. The
	// classic trigger from the 1M demo: use a value that exists only
	// as a SUBSTRING in some paths under WORD-tokenized matching.
	//
	// Switch to: query Equal "shared" (a single token).
	//   FIELD → exact "shared" → 0 matches (no path equals just "shared")
	//   WORD  → token "shared"     → 10000 matches (every path contains it)
	//
	// 0 vs 10000 makes the assertions trivial. The Phase-5 `path=1`
	// shape would show up as 0 or 1 from the partial bucket regardless
	// of which side we query under.
	paths := []string{"red-shared", "green-shared", "blue-shared", "yellow-shared", "purple-shared"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"path": paths[i%len(paths)],
		}
	})

	const (
		queryToken         = "shared" // single token, distinguishes WORD vs FIELD
		expectedWordCount  = totalObjects
		expectedFieldCount = 0
	)

	// === Step 3: baseline — WORD tokenization in place.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		require.NoError(t, err)
		require.Equal(t, expectedWordCount, got,
			"pre-mig WORD baseline on node %d = %d (expected %d) — replication or tokenization is broken",
			nodeIdx, got, expectedWordCount)
	}

	// === Step 4: change-tokenization to FIELD.
	uri1 := restURIOf(compose, 1)
	task1 := submitIndexUpdate(t, uri1, className, "path",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("change-tokenization → field: task=%s", task1)
	awaitReindexFinished(t, uri1, task1)
	time.Sleep(3 * time.Second)

	// === Step 5: every replica must serve the FIELD count.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		require.NoError(t, err)
		require.Equal(t, expectedFieldCount, got,
			"post-FIELD-tok node %d = %d (expected %d)",
			nodeIdx, got, expectedFieldCount)
	}

	// === Step 6: change-tokenization back to WORD.
	task2 := submitIndexUpdate(t, uri1, className, "path",
		`{"searchable":{"tokenization":"word"}}`)
	t.Logf("change-tokenization → word (round-trip): task=%s", task2)
	awaitReindexFinished(t, uri1, task2)
	time.Sleep(3 * time.Second)

	// === Step 7: every replica must serve the WORD baseline again.
	// A `path=1` shape (Frontend Claude's Phase 5) would surface as
	// `got=1` here — the assertion captures it via both the exact-count
	// check and the "must be > N/2" defense-in-depth check.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		assert.NoError(t, err)
		assert.Equalf(t, expectedWordCount, got,
			"GH #212 Issue F regression: round-trip WORD count on node %d = %d (expected %d) "+
				"— back-to-back change-tokenization broke the searchable bucket",
			nodeIdx, got, expectedWordCount)
		assert.Greaterf(t, got, expectedWordCount/2,
			"GH #212 Issue F degenerate-count regression: node %d returned %d (less than half of %d). "+
				"This is the Phase-5 `path=1`-shape failure — the round-trip migration left the bucket "+
				"with effectively no data even though the task reached FINISHED",
			nodeIdx, got, expectedWordCount)
	}

	// LB-side 3-call stability for the round-trip state.
	for i := 0; i < 3; i++ {
		got, err := equalCount(uri1, className, "path", queryToken)
		require.NoError(t, err)
		assert.Equalf(t, expectedWordCount, got,
			"LB-side round-trip WORD count call #%d = %d (expected %d)",
			i+1, got, expectedWordCount)
	}
}
