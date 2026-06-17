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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const blockmaxClassName = "ReindexTest"

var blockmaxDocuments = []string{
	"alpha bravo charlie delta echo foxtrot",
	"golf hotel india juliet kilo lima",
	"mike november oscar papa quebec romeo",
	"sierra tango uniform victor whiskey xray",
	"yankee zulu alpha bravo charlie delta",
	"echo foxtrot golf hotel india juliet",
	"kilo lima mike november oscar papa",
	"quebec romeo sierra tango uniform victor",
	"whiskey xray yankee zulu alpha bravo",
	"charlie delta echo foxtrot golf hotel",
	"india juliet kilo lima mike november",
	"oscar papa quebec romeo sierra tango",
	"uniform victor whiskey xray yankee zulu",
	"alpha charlie echo golf india kilo",
	"mike oscar quebec sierra uniform whiskey",
	"yankee bravo delta foxtrot hotel juliet",
	"lima november papa romeo tango victor",
	"xray zulu alpha echo india mike",
	"oscar sierra uniform yankee charlie foxtrot",
	"hotel kilo november quebec romeo victor",
	"alpha alpha alpha bravo bravo charlie",
	"delta delta delta echo echo foxtrot",
	"golf golf golf hotel hotel india",
	"juliet juliet juliet kilo kilo lima",
	"mike mike mike november november oscar",
}

var blockmaxBM25Queries = []string{
	"alpha",
	"bravo charlie",
	"echo foxtrot golf",
	"mike november oscar",
}

type blockmaxBaseline struct {
	query string
	ids   []string
}

var blockmaxBaselines []blockmaxBaseline

func testBlockmaxMigration(t *testing.T, restURI string) {
	class := &models.Class{
		Class: blockmaxClassName,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{K1: 1.2, B: 0.75},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	createdClass := helper.GetClass(t, blockmaxClassName)
	require.False(t, createdClass.InvertedIndexConfig.UsingBlockMaxWAND)

	// Pre-rebuild: GET /indexes must surface algorithm=wand because the
	// class was created with USE_INVERTED_SEARCHABLE=false. Honest UI
	// rendering depends on this; frontend-claude flagged the gap.
	assertSearchableAlgorithm(t, restURI, blockmaxClassName, "text", "wand", "")

	for i, text := range blockmaxDocuments {
		obj := &models.Object{
			Class:      blockmaxClassName,
			Properties: map[string]interface{}{"text": text},
		}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	blockmaxBaselines = make([]blockmaxBaseline, len(blockmaxBM25Queries))
	for i, q := range blockmaxBM25Queries {
		ids := blockmaxBM25Query(t, q)
		require.NotEmpty(t, ids, "baseline query %q returned no results", q)
		blockmaxBaselines[i] = blockmaxBaseline{query: q, ids: ids}
	}

	var queryFailures atomic.Int64
	var queryRuns atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
			}
			for _, bl := range blockmaxBaselines {
				ids, err := blockmaxBM25QuerySafe(t, bl.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
				} else if !reindexhelpers.IdsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
				}
			}
		}
	}()

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, blockmaxClassName, "text", `{"searchable":{"algorithm":"blockmax"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// While the rebuild is in flight, GET /indexes must surface
	// targetAlgorithm="blockmax" (and may still show algorithm="wand"
	// because the class flag flips only after every shard completes).
	// This is the algorithm-side equivalent of targetTokenization.
	// We make this best-effort: very fast rebuilds may not be observed,
	// but the field must never be unset on an in-flight repair-searchable.
	sawTargetAlgorithm := pollForTargetAlgorithm(t, restURI, blockmaxClassName, "text", "blockmax", 5*time.Second)
	if !sawTargetAlgorithm {
		t.Log("rebuild completed before targetAlgorithm could be observed (fast path)")
	}

	reindexhelpers.AwaitReindexViaIndexes(t, restURI, blockmaxClassName, "text", "searchable")
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Post-rebuild: GET /indexes must surface algorithm=blockmax and must
	// no longer carry targetAlgorithm (the rebuild has completed and the
	// class flag has flipped).
	assertSearchableAlgorithm(t, restURI, blockmaxClassName, "text", "blockmax", "")

	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "BM25 queries failed during migration")

	updatedClass := helper.GetClass(t, blockmaxClassName)
	require.True(t, updatedClass.InvertedIndexConfig.UsingBlockMaxWAND)

	for _, bl := range blockmaxBaselines {
		ids := blockmaxBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-migration query %q results differ", bl.query)
	}
}

func testBlockmaxPostRestart(t *testing.T) {
	for _, bl := range blockmaxBaselines {
		ids := blockmaxBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-restart query %q results differ", bl.query)
	}
}

// assertSearchableAlgorithm fetches GET /v1/schema/{collection}/indexes and
// requires that the searchable entry for the named property carries the
// expected algorithm and targetAlgorithm values. Empty string means "must
// not be set". This is the public contract surface for the BM25 algorithm
// (WAND vs Block Max WAND) that frontend-claude flagged as missing.
func assertSearchableAlgorithm(t *testing.T, restURI, collection, property, wantAlgorithm, wantTargetAlgorithm string) {
	t.Helper()
	resp := reindexhelpers.GetIndexes(t, restURI, collection)
	for _, prop := range resp.Properties {
		if prop.Name != property {
			continue
		}
		for _, idx := range prop.Indexes {
			if idx.Type != "searchable" {
				continue
			}
			require.Equal(t, wantAlgorithm, idx.Algorithm,
				"GET /indexes: prop=%s algorithm mismatch", property)
			require.Equal(t, wantTargetAlgorithm, idx.TargetAlgorithm,
				"GET /indexes: prop=%s targetAlgorithm mismatch", property)
			return
		}
	}
	t.Fatalf("GET /indexes: searchable entry for property %q not found", property)
}

// pollForTargetAlgorithm polls GET /indexes for up to timeout, returning
// true as soon as the named property's searchable entry surfaces
// targetAlgorithm=want. Used to verify the in-flight signal during a
// repair-searchable rebuild without making the test flaky on fast paths
// (small data sets) where the rebuild may complete before we can observe.
func pollForTargetAlgorithm(t *testing.T, restURI, collection, property, want string, timeout time.Duration) bool {
	t.Helper()
	// Best-effort observation: a fast migration may legitimately complete
	// before the transient targetAlgorithm signal can be sampled. This must
	// NOT fail the test, so we drive a plain ticker + deadline loop and
	// return whether the signal was observed (never require.Eventually).
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		resp := reindexhelpers.GetIndexes(t, restURI, collection)
		for _, prop := range resp.Properties {
			if prop.Name != property {
				continue
			}
			for _, idx := range prop.Indexes {
				if idx.Type == "searchable" && idx.TargetAlgorithm == want {
					return true
				}
			}
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-ticker.C:
		case <-time.After(time.Until(deadline)):
			return false
		}
	}
}

func blockmaxBM25Query(t *testing.T, query string) []string {
	t.Helper()
	ids, err := blockmaxBM25QuerySafe(t, query)
	require.NoError(t, err, "BM25 query %q failed", query)
	return ids
}

func blockmaxBM25QuerySafe(t *testing.T, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}) {
				text
				_additional { id }
			}
		}
	}`, blockmaxClassName, query)
	return runGraphQLQuery(t, blockmaxClassName, gqlQuery)
}
