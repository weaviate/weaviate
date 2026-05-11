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
	"github.com/weaviate/weaviate/test/helper"
)

const enableSearchableClassName = "EnableSearchableTest"

var enableSearchableObjects = []map[string]interface{}{
	{"name": "alpha", "description": "the quick brown fox jumps over the lazy dog"},
	{"name": "bravo", "description": "lorem ipsum dolor sit amet consectetur adipiscing elit"},
	{"name": "charlie", "description": "the rain in spain falls mainly on the plain"},
	{"name": "delta", "description": "to be or not to be that is the question"},
	{"name": "echo", "description": "all that glitters is not gold"},
	{"name": "foxtrot", "description": "the fox and the hound are old friends"},
	{"name": "golf", "description": "lorem ipsum but with extra words for tokenization"},
	{"name": "hotel", "description": "spain and portugal share the iberian peninsula"},
	{"name": "india", "description": "the question of consciousness remains unanswered"},
	{"name": "juliet", "description": "all roads lead to rome"},
}

type enableSearchableBaseline struct {
	name string
	ids  []string
}

var enableSearchableBaselines []enableSearchableBaseline

type enableSearchableBM25 struct {
	name, query string
}

var enableSearchableBM25Queries = []enableSearchableBM25{
	{"single_token_fox", "fox"},
	{"single_token_spain", "spain"},
	{"multi_token_lorem_ipsum", "lorem ipsum"},
	{"single_token_question", "question"},
}

// Currently RED. Same backfill gap as enable-filterable: the migration
// completes and the schema flag flips, but the new searchable bucket is
// empty because the analyzer skips properties whose IndexSearchable is
// still false at migration time. Post-migration BM25 queries return
// no hits and require.NotEmpty fires. Kept red on purpose per repo
// policy — see issue #10675 follow-up.
func testEnableSearchable(t *testing.T, restURI string) {
	falseVal := false
	trueVal := true
	class := &models.Class{
		Class: enableSearchableClassName,
		Properties: []*models.Property{
			// "name" keeps the default text indexes so the background safety
			// query has something to filter against during the migration.
			{Name: "name", DataType: []string{"text"}, Tokenization: "field"},
			// "description" starts WITHOUT a searchable index. The migration
			// builds it from scratch with the requested tokenization.
			// IndexFilterable is left true so filter queries on the property
			// continue to work pre-migration (proving the migration doesn't
			// disturb the existing inverted index).
			{
				Name:            "description",
				DataType:        []string{"text"},
				Tokenization:    "field",
				IndexSearchable: &falseVal,
				IndexFilterable: &trueVal,
			},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	for i, props := range enableSearchableObjects {
		obj := &models.Object{Class: enableSearchableClassName, Properties: props}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	// Pre-migration sanity: description must NOT be searchable yet.
	preClass := helper.GetClass(t, enableSearchableClassName)
	for _, prop := range preClass.Properties {
		if prop.Name == "description" {
			require.NotNil(t, prop.IndexSearchable, "description.IndexSearchable should be explicitly false")
			require.False(t, *prop.IndexSearchable, "description.IndexSearchable should be false pre-migration")
		}
	}

	// Background safety-net query against the always-filterable "name"
	// property. This query path is unrelated to the migration; it must
	// continue to work for the entire migration window.
	nameBaseline := enableSearchableNameQuery(t, "alpha")
	require.NotEmpty(t, nameBaseline)

	var queryFailures atomic.Int64
	var queryRuns atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			ids, err := enableSearchableNameQuerySafe(t, "alpha")
			queryRuns.Add(1)
			if err != nil {
				queryFailures.Add(1)
			} else if !idsMatchUnordered(nameBaseline, ids) {
				queryFailures.Add(1)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Submit enable-searchable with tokenization=word so the BM25 queries
	// below split on whitespace.
	taskID := submitIndexUpdate(t, restURI, enableSearchableClassName, "description",
		`{"searchable":{"enabled":true,"tokenization":"word"}}`)
	t.Logf("submitted enable-searchable task for description: %s", taskID)
	awaitReindexFinished(t, restURI, taskID)

	close(stopCh)
	wg.Wait()
	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "background queries must not fail during migration")

	// Schema-flag assertion: IndexSearchable=true and Tokenization="word".
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, enableSearchableClassName)
		for _, prop := range cls.Properties {
			if prop.Name == "description" {
				return prop.IndexSearchable != nil && *prop.IndexSearchable && prop.Tokenization == "word"
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "IndexSearchable must be true and Tokenization word after migration")

	// Functional assertion: BM25 queries against the freshly-built index
	// must return non-empty, deterministic results.
	enableSearchableBaselines = make([]enableSearchableBaseline, len(enableSearchableBM25Queries))
	for i, q := range enableSearchableBM25Queries {
		ids := enableSearchableBM25Query(t, q.query)
		require.NotEmpty(t, ids, "post-migration BM25 query %q should return results", q.name)
		enableSearchableBaselines[i] = enableSearchableBaseline{name: q.name, ids: ids}
	}

	// Specific correctness checks that pin tokenizer behavior:
	//   - "fox" hits both "the quick brown fox jumps over the lazy dog"
	//     (alpha) and "the fox and the hound are old friends" (foxtrot).
	//   - "lorem" hits bravo + golf (both contain "lorem ipsum...").
	foxHits := enableSearchableBM25Query(t, "fox")
	assert.GreaterOrEqual(t, len(foxHits), 2, "BM25 fox should hit at least alpha + foxtrot")

	loremHits := enableSearchableBM25Query(t, "lorem")
	assert.GreaterOrEqual(t, len(loremHits), 2, "BM25 lorem should hit at least bravo + golf")
}

func testEnableSearchablePostRestart(t *testing.T) {
	// After restart, the (formerly-ingest) blockmax bucket must be picked
	// up and serve the same BM25 results we captured pre-restart.
	for i, bl := range enableSearchableBaselines {
		ids := enableSearchableBM25Query(t, enableSearchableBM25Queries[i].query)
		assert.ElementsMatch(t, bl.ids, ids, "post-restart BM25 query %q differs", bl.name)
	}

	cls := helper.GetClass(t, enableSearchableClassName)
	for _, prop := range cls.Properties {
		if prop.Name == "description" {
			require.NotNil(t, prop.IndexSearchable)
			assert.True(t, *prop.IndexSearchable, "post-restart: description.IndexSearchable should still be true")
			assert.Equal(t, "word", prop.Tokenization, "post-restart: tokenization should be word")
		}
	}
}

func enableSearchableBM25Query(t *testing.T, query string) []string {
	t.Helper()
	ids, err := enableSearchableBM25QuerySafe(t, query)
	require.NoError(t, err)
	return ids
}

func enableSearchableBM25QuerySafe(t *testing.T, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["description"]}) {
				name
				description
				_additional { id }
			}
		}
	}`, enableSearchableClassName, query)
	return runGraphQLQuery(t, enableSearchableClassName, gqlQuery)
}

func enableSearchableNameQuery(t *testing.T, value string) []string {
	t.Helper()
	ids, err := enableSearchableNameQuerySafe(t, value)
	require.NoError(t, err)
	return ids
}

func enableSearchableNameQuerySafe(t *testing.T, value string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path:["name"], operator:Equal, valueText:%q}) {
				name
				_additional { id }
			}
		}
	}`, enableSearchableClassName, value)
	return runGraphQLQuery(t, enableSearchableClassName, gqlQuery)
}
