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

package reindex_singlenode

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeleteThenReEnableShortCircuit pins the cycle-3 silent-failure
// family reported in weaviate/weaviate#10675: after multiple
// enable→DELETE cycles on a property with measurable data volume, the
// final cycle must still rebuild the bucket end-to-end. The frontend
// report mentioned cycle 3 finishing in 1.6s with the schema flag
// never flipping; the underlying invariant we pin here is:
//
//  1. After every cycle's FINISHED, `requireSearchableEnabled`
//     succeeds (the schema flag flipped on).
//  2. After every cycle, BM25('fox') returns all expected hits — the
//     bucket is populated, not silently empty.
//
// 500 objects gives the reindex iteration enough work that a true
// short-circuit (iterator processes 0 objects, swap runs against an
// empty bucket) would surface as a BM25 hit count of 0. Cycle-time
// asymmetry between cycles 1 and 3 is NOT a reliable signal on its
// own: warm caches, JIT, and LSM compaction state legitimately speed
// up later cycles by a large factor. We log per-cycle durations so an
// operator can spot suspicious patterns (e.g. cycle 3 = 1.6s after a
// 60s cycle 1) but do not assert on it.
func testDeleteThenReEnableShortCircuit(t *testing.T, restURI string) {
	const class = "ShortCircuitSearchable"
	const docCount = 500
	const query = "fox"

	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	// 500 objects, every other one contains "fox". Need enough volume so
	// each reindex iteration takes measurable time on a small dev box.
	for i := 0; i < docCount; i++ {
		var text string
		if i%2 == 0 {
			text = fmt.Sprintf("quick brown fox jumps over the lazy dog %d", i)
		} else {
			text = fmt.Sprintf("lorem ipsum dolor sit amet %d", i)
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"body": text},
		}), "object %d", i)
	}

	// Measure each cycle: enable→wait FINISHED→record duration→DELETE.
	type cycleResult struct {
		duration  time.Duration
		bm25Hits  int
		taskID    string
		finishedT time.Time
	}
	results := make([]cycleResult, 3)

	for cycle := 0; cycle < 3; cycle++ {
		start := time.Now()
		taskID := submitIndexUpdate(t, restURI, class, "body",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
		awaitReindexFinished(t, restURI, taskID)
		results[cycle].taskID = taskID
		results[cycle].finishedT = time.Now()
		results[cycle].duration = time.Since(start)
		requireSearchableEnabled(t, class, "body")
		results[cycle].bm25Hits = bm25Hits(t, class, query)

		t.Logf("cycle %d: taskID=%s duration=%v bm25('%s')=%d",
			cycle+1, taskID, results[cycle].duration, query, results[cycle].bm25Hits)

		if cycle < 2 {
			deleteIndex(t, restURI, class, "body", "searchable")
		}
	}

	expectedHits := docCount / 2
	for i, r := range results {
		require.GreaterOrEqual(t, r.bm25Hits, expectedHits,
			"cycle %d bm25('%s') must return all %d 'fox' docs (got %d). If 0, the migration silently no-opped against an empty bucket. The most recent task in this run is %s.",
			i+1, query, expectedHits, r.bm25Hits, r.taskID)
	}
}

// TestSuppress ensures this file compiles in isolation.
func TestSuppress_DeleteThenReEnableShortCircuit(t *testing.T) {
	assert.NotNil(t, testDeleteThenReEnableShortCircuit)
}
