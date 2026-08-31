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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeleteDuringDeferredPromotion crosses the two journeys this package
// covers separately and never together: DELETE an index on a property that
// carries migration state, and restart the node.
//
// Promotion is deferred to the next shard load, so between a finished reindex
// and the next restart every migration sits at "flip decided, not promoted",
// naming directories the DELETE has just removed. Everything the promotion
// machinery then does to those directories happens at a load no DELETE test
// performs, and against a schema no restart test has touched.
//
// Two restarts per journey, not one: promotion acts on the first load, and
// what that load leaves behind is only visible on the second.
//
// Both sub-journeys assert that an index the operator deleted stays deleted
// and an index nobody touched keeps answering. Only the re-enable journey goes
// on to assert that an index rebuilt after the DELETE keeps its contents.
func testDeleteDuringDeferredPromotion(t *testing.T, compose *docker.DockerCompose) {
	t.Run("delete then re-enable across three loads", func(t *testing.T) {
		testDeleteThenReEnableAcrossLoads(t, compose)
	})
	t.Run("delete one index type beside a live one", func(t *testing.T) {
		testDeleteBesideALiveIndexAcrossLoads(t, compose)
	})
}

// restartWeaviate stops and starts the shared container and re-points the
// client, returning the new REST URI. Callers that only query through the
// helper client can ignore it.
func restartWeaviate(t *testing.T, compose *docker.DockerCompose, why string) string {
	t.Helper()
	ctx := context.Background()
	require.NoErrorf(t, compose.StopAt(ctx, 0, nil), "%s: stop", why)
	require.NoErrorf(t, compose.StartAt(ctx, 0), "%s: start", why)
	uri := compose.GetWeaviate().URI()
	helper.SetupClient(uri)
	return uri
}

// testDeleteBesideALiveIndexAcrossLoads is the same crossing with the
// migration's canonical bucket left LIVE. A change-tokenization retokenizes a
// property's filterable and searchable buckets together, so deleting only the
// filterable index removes one half's directories while the other half keeps
// serving queries with its own migration still waiting to be promoted.
func testDeleteBesideALiveIndexAcrossLoads(t *testing.T, compose *docker.DockerCompose) {
	const class = "DeleteBesideALiveIndex"

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name: "name", DataType: []string{"text"},
				IndexFilterable: &trueVal, IndexSearchable: &trueVal, Tokenization: "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	const numObjects = 3
	for i := 0; i < numObjects; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": "alpha"},
		}), "object %d", i)
	}

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "name", "searchable",
		`{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	require.Equal(t, numObjects, bm25HitsForProp(t, class, "name", "alpha"),
		"fixture: the searchable index must answer before the DELETE")

	// The trigger: only the filterable half is deleted. The searchable half
	// keeps serving, and its migration is still waiting to be promoted.
	deleteIndex(t, restURI, class, "name", "filterable")

	for i := 1; i <= 2; i++ {
		restartWeaviate(t, compose, fmt.Sprintf("load %d after the DELETE", i))
		require.Equalf(t, numObjects, bm25HitsForProp(t, class, "name", "alpha"),
			"load %d after deleting the filterable index emptied the live searchable index", i)
	}
}

func testDeleteThenReEnableAcrossLoads(t *testing.T, compose *docker.DockerCompose) {
	const class = "DeleteDuringDeferredPromotion"

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "kept", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
			{Name: "dropped", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	const numObjects = 3
	for i := 0; i < numObjects; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class,
			Properties: map[string]interface{}{
				"kept":    fmt.Sprintf("quick brown fox %d", i),
				"dropped": fmt.Sprintf("lazy hound cart %d", i),
			},
		}), "object %d", i)
	}

	for _, prop := range []string{"kept", "dropped"} {
		taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, prop, "searchable",
			`{"tokenization":"word"}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireSearchableEnabled(t, class, prop)
	}
	require.Equal(t, numObjects, bm25HitsForProp(t, class, "kept", "fox"),
		"fixture: the surviving property's index must answer before the DELETE")
	require.Equal(t, numObjects, bm25HitsForProp(t, class, "dropped", "hound"),
		"fixture: the deleted property's index must answer before the DELETE")

	// The trigger: an ordinary index DELETE, while both migrations are still
	// waiting for the load that promotes them.
	deleteIndex(t, restURI, class, "dropped", "searchable")
	requireSearchableDisabled(t, class, "dropped")

	// The load the DELETE journey never performs. Nothing may re-enable an
	// index the operator removed, and the property that was not touched must
	// keep answering.
	restURI = restartWeaviate(t, compose, "the load after the DELETE")
	requireSearchableDisabledf(t, class, "dropped",
		"the load after the DELETE re-enabled an index the operator removed; the schema now "+
			"advertises a searchable index nothing rebuilt")
	require.Equal(t, numObjects, bm25HitsForProp(t, class, "kept", "fox"),
		"the load after the DELETE emptied the untouched property's index")

	// Re-enable, then two more loads. This is where the deleted migration's
	// leftovers get their chance: the first load is where a promotion subject's
	// staged directory can be re-created, and the second is where promotion
	// renames it onto the canonical name — over the index this re-enable just
	// rebuilt.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "dropped", "searchable",
		`{"tokenization":"word"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireSearchableEnabled(t, class, "dropped")
	require.Equal(t, numObjects, bm25HitsForProp(t, class, "dropped", "hound"),
		"re-enabling after the DELETE must rebuild the index")

	for i := 1; i <= 2; i++ {
		restartWeaviate(t, compose, fmt.Sprintf("load %d after the re-enable", i))
		requireSearchableEnabled(t, class, "dropped")
		require.Equalf(t, numObjects, bm25HitsForProp(t, class, "dropped", "hound"),
			"load %d after the re-enable emptied the rebuilt index", i)
		require.Equalf(t, numObjects, bm25HitsForProp(t, class, "kept", "fox"),
			"load %d after the re-enable emptied the untouched property's index", i)
	}
}

// requireSearchableDisabled is [requireSearchableEnabled]'s opposite: an index
// the operator deleted must stay deleted.
func requireSearchableDisabled(t *testing.T, class, prop string) {
	t.Helper()
	requireSearchableDisabledf(t, class, prop, "IndexSearchable on %s.%s must stay false", class, prop)
}

func requireSearchableDisabledf(t *testing.T, class, prop, msg string, args ...interface{}) {
	t.Helper()
	// Never Eventually: this asserts a flag does NOT change, so a poll would
	// only widen the window in which it may.
	c := helper.GetClass(t, class)
	require.NotNil(t, c)
	for _, p := range c.Properties {
		if p.Name == prop {
			require.Falsef(t, p.IndexSearchable != nil && *p.IndexSearchable, msg, args...)
			return
		}
	}
	require.FailNowf(t, "property not found", "%s.%s", class, prop)
}

// TestSuppress_DeleteDuringDeferredPromotion keeps this file compiling in
// isolation; the entry point is the suite subtest.
func TestSuppress_DeleteDuringDeferredPromotion(t *testing.T) {
	assert.NotNil(t, testDeleteDuringDeferredPromotion)
}
