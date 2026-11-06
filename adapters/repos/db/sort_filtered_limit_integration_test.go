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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestObjectSearch_SortedWithFilterReturnsTheTopOfAllMatches pins that a sorted search
// with a filter and a limit returns the first objects in sort order among every match.
// A filter resolved with a limit stops after the first matches in index key order, so
// sorting that subset returned the top of whichever matches the reader met first, and
// paging with an offset could return the same objects on two pages.
//
// Every filter below matches every object, so the result must equal the unfiltered
// sorted listing page for page.
func TestObjectSearch_SortedWithFilterReturnsTheTopOfAllMatches(t *testing.T) {
	const objectCount = 200

	ctx := context.Background()
	repo := newBatchDeleteRepoAt(t, t.TempDir(), batchDeleteTestClass(false), singleShardState(), 0)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	insertBatchDeleteObjects(t, repo, 0, objectCount)

	matchAll := []struct {
		name   string
		filter *filters.Clause
	}{
		{
			// The id bucket is read in uuid order, which the sort order does not follow.
			name:   "Like on id",
			filter: batchDeleteMatchAllClause(),
		},
		{
			name: "Like on stringProp",
			filter: &filters.Clause{
				Operator: filters.OperatorLike,
				On: &filters.Path{
					Class:    batchDeleteClassName,
					Property: schema.PropertyName("stringProp"),
				},
				Value: &filters.Value{Value: "elem*", Type: schema.DataTypeText},
			},
		},
		{
			// Every value has a numeric token, and every numeric token is >= "0".
			name: "GreaterThanEqual on stringProp",
			filter: &filters.Clause{
				Operator: filters.OperatorGreaterThanEqual,
				On: &filters.Path{
					Class:    batchDeleteClassName,
					Property: schema.PropertyName("stringProp"),
				},
				Value: &filters.Value{Value: "0", Type: schema.DataTypeText},
			},
		},
	}

	pages := []struct{ offset, limit int }{
		{0, 1}, {0, 3}, {3, 3}, {10, 10}, {190, 10},
	}

	search := func(t *testing.T, filter *filters.LocalFilter, sort []filters.Sort, offset, limit int) []any {
		t.Helper()
		res, err := repo.ObjectSearch(ctx, offset, limit, filter, sort, additional.Properties{}, "")
		require.NoError(t, err)
		values := make([]any, len(res))
		for i, r := range res {
			values[i] = r.Schema.(map[string]any)["stringProp"]
		}
		return values
	}

	for _, order := range []string{"asc", "desc"} {
		sort := []filters.Sort{{Path: []string{"stringProp"}, Order: order}}
		want := search(t, nil, sort, 0, objectCount)
		require.Len(t, want, objectCount)

		for _, tt := range matchAll {
			t.Run(tt.name+" "+order, func(t *testing.T) {
				filter := &filters.LocalFilter{Root: tt.filter}
				for _, p := range pages {
					got := search(t, filter, sort, p.offset, p.limit)
					require.Equal(t, want[p.offset:p.offset+p.limit], got,
						"offset %d limit %d", p.offset, p.limit)
				}
			})
		}
	}
}
