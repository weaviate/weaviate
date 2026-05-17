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

const enableFilterableClassName = "EnableFilterableTest"

var enableFilterableObjects = func() []map[string]interface{} {
	categories := []string{"electronics", "books", "clothing", "food", "sports"}
	objects := make([]map[string]interface{}, 0, 25)
	for i := 0; i < 25; i++ {
		objects = append(objects, map[string]interface{}{
			"name":      fmt.Sprintf("item_%d", i),
			"category":  categories[i%5],
			"score":     int64(i + 1),
			"available": i%2 == 0,
		})
	}
	return objects
}()

// enableFilterableBaseline captures per-query expected results, computed
// AFTER migration so we can compare the new (just-built) filterable index
// against itself across the post-restart restart cycle.
type enableFilterableBaseline struct {
	name string
	ids  []string
}

var enableFilterableBaselines []enableFilterableBaseline

type enableFilterableQuery struct {
	name  string
	where string
}

var enableFilterableQueries = []enableFilterableQuery{
	{"score_eq_5", `{path:["score"], operator:Equal, valueInt:5}`},
	{"score_neq_3", `{path:["score"], operator:NotEqual, valueInt:3}`},
	{"available_eq_true", `{path:["available"], operator:Equal, valueBoolean:true}`},
	{"available_eq_false", `{path:["available"], operator:Equal, valueBoolean:false}`},
	{"and_score_available", `{operator:And, operands:[
		{path:["score"], operator:GreaterThanEqual, valueInt:10},
		{path:["available"], operator:Equal, valueBoolean:true}
	]}`},
}

// Currently RED. The migration triggers, the schema flag flips, and the
// shard ends up with a (new, empty) filterable bucket — but the backfill
// from the objects bucket produces no entries because the analyzer's
// HasAnyInvertedIndex check (inverted/objects.go) skips properties whose
// schema flag is still false at migration time. As a result the
// post-migration baseline queries against the freshly-built bucket return
// no rows and require.NotEmpty fires. Kept red on purpose per repo policy
// — see issue https://github.com/weaviate/weaviate/issues/10675 follow-up "backfill skipped for from-scratch enable".
func testEnableFilterable(t *testing.T, restURI string) {
	falseVal := false
	class := &models.Class{
		Class: enableFilterableClassName,
		Properties: []*models.Property{
			// "name" and "category" keep default indexes so we can run a
			// safety-net query against them while the migration runs.
			{Name: "name", DataType: []string{"text"}, Tokenization: "field"},
			{Name: "category", DataType: []string{"text"}, Tokenization: "field"},
			// These two are the migration targets: filterable disabled at
			// class creation, enabled at runtime via the reindex API.
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &falseVal, IndexRangeFilters: &falseVal},
			{Name: "available", DataType: []string{"boolean"}, IndexFilterable: &falseVal},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	for i, props := range enableFilterableObjects {
		obj := &models.Object{Class: enableFilterableClassName, Properties: props}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	// Sanity check: before any migration runs, the targeted properties must
	// not have a filterable index. This guards against the test silently
	// passing if a default value flipped under us.
	preClass := helper.GetClass(t, enableFilterableClassName)
	for _, prop := range preClass.Properties {
		switch prop.Name {
		case "score", "available":
			require.NotNil(t, prop.IndexFilterable, "%s.IndexFilterable should be explicitly false", prop.Name)
			require.False(t, *prop.IndexFilterable, "%s.IndexFilterable should be false pre-migration", prop.Name)
		}
	}

	// Baseline query against an always-indexed property — we will run it in a
	// background loop and fail the test if it errors or returns wrong results
	// during the migration. This pins the invariant that enable-filterable
	// does not regress unrelated query paths.
	categoryBaseline := enableFilterableCategoryQuery(t, "electronics")
	require.NotEmpty(t, categoryBaseline, "category=electronics baseline should match at least one object")

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
			ids, err := enableFilterableCategoryQuerySafe(t, "electronics")
			queryRuns.Add(1)
			if err != nil {
				queryFailures.Add(1)
			} else if !reindexhelpers.IdsMatchUnordered(categoryBaseline, ids) {
				queryFailures.Add(1)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 1) Enable filterable on the int property.
	taskID1 := reindexhelpers.SubmitIndexUpdate(t, restURI, enableFilterableClassName, "score", `{"filterable":{"enabled":true}}`)
	t.Logf("submitted enable-filterable task for score: %s", taskID1)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID1)

	// 2) Enable filterable on the boolean property.
	taskID2 := reindexhelpers.SubmitIndexUpdate(t, restURI, enableFilterableClassName, "available", `{"filterable":{"enabled":true}}`)
	t.Logf("submitted enable-filterable task for available: %s", taskID2)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID2)

	close(stopCh)
	wg.Wait()
	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "background queries on always-indexed property must not fail during migration")

	// Schema-flag assertion: the migration must flip IndexFilterable=true on
	// exactly the targeted properties — and on no others.
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, enableFilterableClassName)
		var scoreOK, availableOK bool
		for _, prop := range cls.Properties {
			switch prop.Name {
			case "score":
				scoreOK = prop.IndexFilterable != nil && *prop.IndexFilterable
			case "available":
				availableOK = prop.IndexFilterable != nil && *prop.IndexFilterable
			}
		}
		return scoreOK && availableOK
	}, 30*time.Second, 500*time.Millisecond, "IndexFilterable must flip to true on targeted properties")

	// Capture post-migration baselines so the post-restart phase can compare
	// against the exact result set we just produced from the new index.
	enableFilterableBaselines = make([]enableFilterableBaseline, len(enableFilterableQueries))
	for i, q := range enableFilterableQueries {
		ids := enableFilterableQuery_(t, q.where)
		require.NotEmpty(t, ids, "post-migration query %q should match at least one object", q.name)
		enableFilterableBaselines[i] = enableFilterableBaseline{name: q.name, ids: ids}
	}
}

func testEnableFilterablePostRestart(t *testing.T) {
	// After the shared post-restart, the filterable bucket must be queryable
	// on disk: the same queries that produced enableFilterableBaselines must
	// produce identical results.
	for i, bl := range enableFilterableBaselines {
		ids := enableFilterableQuery_(t, enableFilterableQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids, "post-restart query %q differs", bl.name)
	}

	cls := helper.GetClass(t, enableFilterableClassName)
	for _, prop := range cls.Properties {
		switch prop.Name {
		case "score", "available":
			require.NotNil(t, prop.IndexFilterable)
			assert.True(t, *prop.IndexFilterable, "post-restart: %s.IndexFilterable should still be true", prop.Name)
		}
	}
}

// enableFilterableQuery_ executes a where filter query. The trailing underscore
// avoids the name collision with the package-level enableFilterableQuery type.
func enableFilterableQuery_(t *testing.T, where string) []string {
	t.Helper()
	ids, err := enableFilterableQuerySafe(t, where)
	require.NoError(t, err)
	return ids
}

func enableFilterableQuerySafe(t *testing.T, where string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s) {
				name
				_additional { id }
			}
		}
	}`, enableFilterableClassName, where)
	return runGraphQLQuery(t, enableFilterableClassName, gqlQuery)
}

func enableFilterableCategoryQuery(t *testing.T, value string) []string {
	t.Helper()
	ids, err := enableFilterableCategoryQuerySafe(t, value)
	require.NoError(t, err)
	return ids
}

func enableFilterableCategoryQuerySafe(t *testing.T, value string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path:["category"], operator:Equal, valueText:%q}) {
				name
				_additional { id }
			}
		}
	}`, enableFilterableClassName, value)
	return runGraphQLQuery(t, enableFilterableClassName, gqlQuery)
}
