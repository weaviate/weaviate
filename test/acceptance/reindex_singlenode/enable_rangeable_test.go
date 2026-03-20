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

const rangeableClassName = "EnableRangeableTest"

var rangeableObjects = func() []map[string]interface{} {
	objects := make([]map[string]interface{}, 0, 25)
	for i := 0; i < 25; i++ {
		objects = append(objects, map[string]interface{}{
			"name":  fmt.Sprintf("item_%d", i),
			"score": float64(i + 1),
			"price": float64(i)*3.5 + 10.0,
		})
	}
	return objects
}()

type rangeableFilterQuery struct {
	name  string
	where string
}

var rangeableFilterQueries = []rangeableFilterQuery{
	{"score_gt_15", `{path:["score"], operator:GreaterThan, valueInt:15}`},
	{"price_lte_50", `{path:["price"], operator:LessThanEqual, valueNumber:50.0}`},
	{"score_gte_10_and_price_lt_80", `{operator:And, operands:[
		{path:["score"], operator:GreaterThanEqual, valueInt:10},
		{path:["price"], operator:LessThan, valueNumber:80.0}
	]}`},
	{"score_eq_5", `{path:["score"], operator:Equal, valueInt:5}`},
	{"price_eq_exact", `{path:["price"], operator:Equal, valueNumber:10.0}`},
}

type rangeableBaseline struct {
	name string
	ids  []string
}

var rangeableBaselines []rangeableBaseline

func testEnableRangeable(t *testing.T, restURI string) {
	class := &models.Class{
		Class: rangeableClassName,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}},
			{Name: "score", DataType: []string{"int"}},
			{Name: "price", DataType: []string{"number"}},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	for i, props := range rangeableObjects {
		obj := &models.Object{Class: rangeableClassName, Properties: props}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	rangeableBaselines = make([]rangeableBaseline, len(rangeableFilterQueries))
	for i, fq := range rangeableFilterQueries {
		ids := rangeableQuery(t, fq.where)
		require.NotEmpty(t, ids, "baseline query %q returned no results", fq.name)
		rangeableBaselines[i] = rangeableBaseline{name: fq.name, ids: ids}
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
			for i, bl := range rangeableBaselines {
				ids, err := rangeableQuerySafe(t, rangeableFilterQueries[i].where)
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

	taskID1 := submitIndexUpdate(t, restURI, rangeableClassName, "score", `{"rangeable":{"enabled":true}}`)
	t.Logf("submitted reindex task for score: %s", taskID1)
	awaitReindexViaIndexes(t, restURI, rangeableClassName, "score", "rangeable")
	awaitReindexFinished(t, restURI, taskID1)

	taskID2 := submitIndexUpdate(t, restURI, rangeableClassName, "price", `{"rangeable":{"enabled":true}}`)
	t.Logf("submitted reindex task for price: %s", taskID2)
	awaitReindexViaIndexes(t, restURI, rangeableClassName, "price", "rangeable")
	awaitReindexFinished(t, restURI, taskID2)

	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load())

	for i, bl := range rangeableBaselines {
		ids := rangeableQuery(t, rangeableFilterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids, "post-migration query %q differs", bl.name)
	}

	updatedClass := helper.GetClass(t, rangeableClassName)
	for _, prop := range updatedClass.Properties {
		switch prop.Name {
		case "score", "price":
			require.NotNil(t, prop.IndexRangeFilters)
			assert.True(t, *prop.IndexRangeFilters)
		case "name":
			if prop.IndexRangeFilters != nil {
				assert.False(t, *prop.IndexRangeFilters)
			}
		}
	}
}

func testEnableRangeablePostRestart(t *testing.T) {
	for i, bl := range rangeableBaselines {
		ids := rangeableQuery(t, rangeableFilterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids, "post-restart query %q differs", bl.name)
	}
}

func rangeableQuery(t *testing.T, where string) []string {
	t.Helper()
	ids, err := rangeableQuerySafe(t, where)
	require.NoError(t, err)
	return ids
}

func rangeableQuerySafe(t *testing.T, where string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s) {
				name
				score
				price
				_additional { id }
			}
		}
	}`, rangeableClassName, where)
	return runGraphQLQuery(t, rangeableClassName, gqlQuery)
}
