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

const roaringSetClassName = "RoaringSetRefreshTest"

var roaringSetObjects = func() []map[string]interface{} {
	categories := []string{"electronics", "books", "clothing", "food", "sports"}
	objects := make([]map[string]interface{}, 0, 30)
	for i := 0; i < 30; i++ {
		objects = append(objects, map[string]interface{}{
			"category": categories[i%5],
			"score":    float64(i + 1),
			"active":   i%2 == 0,
		})
	}
	return objects
}()

type roaringSetFilterQuery struct {
	name  string
	where string
}

var roaringSetFilterQueries = []roaringSetFilterQuery{
	{"equal_text", `{path:["category"], operator:Equal, valueText:"electronics"}`},
	{"not_equal_text", `{path:["category"], operator:NotEqual, valueText:"books"}`},
	{"greater_than_int", `{path:["score"], operator:GreaterThan, valueInt:20}`},
	{"less_than_int", `{path:["score"], operator:LessThan, valueInt:10}`},
	{"equal_boolean", `{path:["active"], operator:Equal, valueBoolean:true}`},
	{"and_composite", `{operator:And, operands:[
		{path:["category"], operator:Equal, valueText:"electronics"},
		{path:["active"], operator:Equal, valueBoolean:true}
	]}`},
	{"or_composite", `{operator:Or, operands:[
		{path:["category"], operator:Equal, valueText:"books"},
		{path:["score"], operator:GreaterThan, valueInt:25}
	]}`},
}

type roaringSetBaseline struct {
	name string
	ids  []string
}

var roaringSetBaselines []roaringSetBaseline

func testRoaringSetRefresh(t *testing.T, restURI string) {
	class := &models.Class{
		Class: roaringSetClassName,
		Properties: []*models.Property{
			{Name: "category", DataType: []string{"text"}, Tokenization: "field"},
			{Name: "score", DataType: []string{"int"}},
			{Name: "active", DataType: []string{"boolean"}},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	for i, props := range roaringSetObjects {
		obj := &models.Object{Class: roaringSetClassName, Properties: props}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	roaringSetBaselines = make([]roaringSetBaseline, len(roaringSetFilterQueries))
	for i, fq := range roaringSetFilterQueries {
		ids := roaringSetQuery(t, fq.where)
		require.NotEmpty(t, ids, "baseline query %q returned no results", fq.name)
		roaringSetBaselines[i] = roaringSetBaseline{name: fq.name, ids: ids}
	}

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
			for i, bl := range roaringSetBaselines {
				ids, err := roaringSetQuerySafe(t, roaringSetFilterQueries[i].where)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
				} else if !idsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	taskID := submitIndexUpdate(t, restURI, roaringSetClassName, "category", `{"filterable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	awaitReindexViaIndexes(t, restURI, roaringSetClassName, "category", "filterable")
	awaitReindexFinished(t, restURI, taskID)

	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load())

	for i, bl := range roaringSetBaselines {
		ids := roaringSetQuery(t, roaringSetFilterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids, "post-migration query %q differs", bl.name)
	}
}

func testRoaringSetRefreshPostRestart(t *testing.T) {
	for i, bl := range roaringSetBaselines {
		ids := roaringSetQuery(t, roaringSetFilterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids, "post-restart query %q differs", bl.name)
	}
}

func roaringSetQuery(t *testing.T, where string) []string {
	t.Helper()
	ids, err := roaringSetQuerySafe(t, where)
	require.NoError(t, err)
	return ids
}

func roaringSetQuerySafe(t *testing.T, where string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s) {
				category
				score
				active
				_additional { id }
			}
		}
	}`, roaringSetClassName, where)
	return runGraphQLQuery(t, roaringSetClassName, gqlQuery)
}
