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

package db

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

const (
	// logged only when the resolver machinery actually ran
	actionDedupStart = refcache.ActionDedupJoblistStart
	// logged when Build ran but found no jobs; never fires if skipped entirely
	actionFetchSkip = refcache.ActionFetchJobsSkip
)

// refLessResults returns a fresh copy each call, so tests can compare two
// independent runs without shared mutation.
func refLessResults() search.Results {
	return search.Results{
		{
			ID:        "91b1a2ce-0000-0000-0000-000000000001",
			ClassName: "Book",
			Schema: map[string]interface{}{
				"title": "some title",
				"pages": float64(342),
				"meta": map[string]interface{}{
					"isbn": "978-3-16-148410-0",
				},
				// ref data present but never selected in these tests
				"ofAuthor": models.MultipleRef{
					&models.SingleRef{Beacon: "weaviate://localhost/Author/91b1a2ce-0000-0000-0000-000000000002"},
				},
			},
		},
		{
			ID:        "91b1a2ce-0000-0000-0000-000000000003",
			ClassName: "Book",
			Schema: map[string]interface{}{
				"title": "another title",
			},
		},
	}
}

func groupedResults(hitProps map[string]interface{}) search.Results {
	res := refLessResults()
	res[0].AdditionalProperties = models.AdditionalProperties{
		"group": &additional.Group{
			Hits: []map[string]interface{}{hitProps},
		},
	}
	return res
}

func refSelectProps() search.SelectProperties {
	return search.SelectProperties{
		{Name: "title", IsPrimitive: true},
		{
			Name: "ofAuthor",
			Refs: []search.SelectClass{
				{
					ClassName: "Author",
					RefProperties: search.SelectProperties{
						{Name: "name", IsPrimitive: true},
					},
				},
			},
		},
	}
}

func newTestDBWithHook() (*DB, *test.Hook) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.TraceLevel)
	return &DB{logger: logger}, hook
}

func countLogAction(hook *test.Hook, action string) int {
	count := 0
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == action {
			count++
		}
	}
	return count
}

func TestResolveReferencesSkipsWithoutRefSelectProps(t *testing.T) {
	tests := []struct {
		name    string
		props   search.SelectProperties
		groupBy *searchparams.GroupBy
	}{
		{
			name:  "nil select props",
			props: nil,
		},
		{
			name: "primitive select props only",
			props: search.SelectProperties{
				{Name: "title", IsPrimitive: true},
				{Name: "pages", IsPrimitive: true},
			},
		},
		{
			name: "nested object select props without refs",
			props: search.SelectProperties{
				{Name: "meta", IsObject: true, Props: search.SelectProperties{
					{Name: "isbn", IsPrimitive: true},
				}},
			},
		},
		{
			name: "ref-shaped prop selected without target classes",
			props: search.SelectProperties{
				{Name: "ofAuthor"},
			},
		},
		{
			name: "groupBy without refs anywhere",
			props: search.SelectProperties{
				{Name: "title", IsPrimitive: true},
			},
			groupBy: &searchparams.GroupBy{
				Property: "title",
				Properties: search.SelectProperties{
					{Name: "title", IsPrimitive: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, hook := newTestDBWithHook()

			gated, err := db.ResolveReferences(context.Background(),
				refLessResults(), tt.props, tt.groupBy, additional.Properties{}, "")
			require.NoError(t, err)

			assert.Equal(t, 0, countLogAction(hook, actionFetchSkip),
				"cacher ran despite ref-less select props")
			assert.Equal(t, 0, countLogAction(hook, actionDedupStart))

			var ungated search.Results
			if tt.groupBy != nil {
				ungated, err = refcache.NewResolverWithGroup(
					refcache.NewCacher(db, db.logger, ""), tt.groupBy.Properties).
					Do(context.Background(), refLessResults(), tt.props, additional.Properties{})
			} else {
				ungated, err = refcache.NewResolver(refcache.NewCacher(db, db.logger, "")).
					Do(context.Background(), refLessResults(), tt.props, additional.Properties{})
			}
			require.NoError(t, err)

			assert.Equal(t, ungated, gated,
				"early-exited result differs from full resolver result")
			assert.Equal(t, refLessResults(), gated,
				"ref-less resolution must not alter the input")

			// guard: fail loudly if the full path itself never ran
			assert.Equal(t, 1, countLogAction(hook, actionFetchSkip))
		})
	}
}

func TestResolveReferencesRunsWithRefSelectProps(t *testing.T) {
	db, hook := newTestDBWithHook()

	res, err := db.ResolveReferences(context.Background(),
		refLessResults(), refSelectProps(), nil, additional.Properties{}, "")
	require.NoError(t, err)
	require.Len(t, res, 2)

	assert.Equal(t, 1, countLogAction(hook, actionDedupStart),
		"resolver machinery did not run despite ref select props")
}

func TestResolveReferencesRunsWithGroupByRefProps(t *testing.T) {
	db, hook := newTestDBWithHook()

	// refs live only in groupBy properties, not top-level select props
	props := search.SelectProperties{{Name: "title", IsPrimitive: true}}
	groupBy := &searchparams.GroupBy{
		Property:   "title",
		Properties: refSelectProps(),
	}
	objs := groupedResults(map[string]interface{}{
		"ofAuthor": models.MultipleRef{
			&models.SingleRef{Beacon: "weaviate://localhost/Author/91b1a2ce-0000-0000-0000-000000000004"},
		},
	})

	res, err := db.ResolveReferences(context.Background(),
		objs, props, groupBy, additional.Properties{}, "")
	require.NoError(t, err)
	require.Len(t, res, 2)

	assert.Equal(t, 1, countLogAction(hook, actionDedupStart),
		"resolver machinery did not run despite refs in groupBy props")
}

func TestEnrichRefsForSingleSkipsWithoutRefSelectProps(t *testing.T) {
	db, hook := newTestDBWithHook()

	obj := refLessResults()[0]
	props := search.SelectProperties{{Name: "title", IsPrimitive: true}}

	gated, err := db.enrichRefsForSingle(context.Background(), &obj, props,
		additional.Properties{}, "")
	require.NoError(t, err)
	assert.Equal(t, 0, countLogAction(hook, actionFetchSkip),
		"cacher ran despite ref-less select props")

	ungatedIn := refLessResults()[0]
	ungated, err := refcache.NewResolver(refcache.NewCacher(db, db.logger, "")).
		Do(context.Background(), []search.Result{ungatedIn}, props, additional.Properties{})
	require.NoError(t, err)

	assert.Equal(t, ungated[0], *gated,
		"early-exited result differs from full resolver result")
	assert.Equal(t, refLessResults()[0], *gated,
		"ref-less resolution must not alter the input")
	assert.Equal(t, 1, countLogAction(hook, actionFetchSkip))
}

func TestEnrichRefsForSingleRunsWithRefSelectProps(t *testing.T) {
	db, hook := newTestDBWithHook()

	obj := refLessResults()[0]
	res, err := db.enrichRefsForSingle(context.Background(), &obj,
		refSelectProps(), additional.Properties{}, "")
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, 1, countLogAction(hook, actionDedupStart),
		"resolver machinery did not run despite ref select props")
}
