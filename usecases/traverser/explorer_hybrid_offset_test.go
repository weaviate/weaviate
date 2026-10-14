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

package traverser

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// makeHybridKeywordResults returns the objects of makeHybridVectorResults as
// BM25 results, best first.
func makeHybridKeywordResults(n int) ([]*storobj.Object, []float32) {
	objs := make([]*storobj.Object, n)
	scores := make([]float32, n)
	for i, res := range makeHybridVectorResults(n) {
		objs[i] = &storobj.Object{
			Object: models.Object{ID: res.ID, Class: "TestClass", Properties: res.Schema},
			DocID:  *res.DocID,
		}
		scores[i] = float32(n - i)
	}
	return objs, scores
}

// newOffsetTestSearcher returns a searcher with the same 200 objects in both
// legs. fetched is called with the pagination of each leg.
func newOffsetTestSearcher(fetched func(filters.Pagination)) *fakeVectorSearcher {
	searcher := &fakeVectorSearcher{}
	searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
		fetched(*params.Pagination)
		return page(makeHybridVectorResults(200), params.Pagination), nil
	}
	searcher.sparseObjectSearchFn = func(params dto.GetParams) ([]*storobj.Object, []float32, error) {
		fetched(*params.Pagination)
		objs, scores := makeHybridKeywordResults(200)
		n := min(params.Pagination.Limit, len(objs))
		return objs[:n], scores[:n], nil
	}
	return searcher
}

// Each leg of a hybrid search has to fetch up to the end of the requested
// page: the page is cut from the fused list afterwards.
func TestExplorerHybridLegsReachThePage(t *testing.T) {
	legs := []struct {
		name  string
		alpha float64
	}{
		{name: "vector", alpha: 1},
		{name: "keyword", alpha: 0},
	}
	pages := []struct {
		name        string
		offset      int
		limit       int
		wantFetched int
		wantFirst   string
	}{
		{name: "first page fetches the hybrid minimum", offset: 0, limit: 10, wantFetched: 100, wantFirst: "Item 00"},
		{name: "page inside the hybrid minimum", offset: 50, limit: 10, wantFetched: 100, wantFirst: "Item 50"},
		{name: "page that crosses the hybrid minimum", offset: 95, limit: 10, wantFetched: 105, wantFirst: "Item 95"},
		{name: "page beyond the hybrid minimum", offset: 100, limit: 10, wantFetched: 110, wantFirst: "Item 100"},
	}
	for _, leg := range legs {
		for _, tt := range pages {
			t.Run(leg.name+"/"+tt.name, func(t *testing.T) {
				var seen filters.Pagination
				explorer := newHybridDepthTestExplorer(newOffsetTestSearcher(func(p filters.Pagination) { seen = p }))
				params := dto.GetParams{
					ClassName:    "TestClass",
					Pagination:   &filters.Pagination{Offset: tt.offset, Limit: tt.limit},
					HybridSearch: &searchparams.HybridSearch{Query: "test", Alpha: leg.alpha, Vector: []float32{0.1, 0.2, 0.3}},
				}

				res, err := explorer.GetClass(context.Background(), params)

				require.NoError(t, err)
				assert.Equal(t, tt.wantFetched, seen.Limit)
				assert.Equal(t, 0, seen.Offset)
				require.Len(t, res, tt.limit)
				assert.Equal(t, tt.wantFirst, idsFromResponse(res)[0])
			})
		}
	}
}

// A page that ends beyond QUERY_MAXIMUM_RESULTS is rejected before any leg
// runs, for every alpha. The error has the offset and limit of the request.
func TestExplorerHybridRejectsPageBeyondQueryMaximumResults(t *testing.T) {
	const wantErr = "offset '9995' and limit '10' exceeds the configured value for QUERY_MAXIMUM_RESULTS '10000'"
	tests := []struct {
		name    string
		offset  int
		boost   *filters.Boost
		wantErr string
	}{
		{name: "page that ends at the maximum", offset: 9990},
		{name: "page that ends beyond the maximum", offset: 9995, wantErr: wantErr},
		{name: "boosted page that ends beyond the maximum", offset: 9995, boost: likesBoost(1, 0), wantErr: wantErr},
	}
	for _, alpha := range []float64{0, 0.5, 1} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("alpha %v/%s", alpha, tt.name), func(t *testing.T) {
				var searches atomic.Int32
				explorer := newHybridDepthTestExplorer(newOffsetTestSearcher(func(filters.Pagination) { searches.Add(1) }))
				params := dto.GetParams{
					ClassName:    "TestClass",
					Pagination:   &filters.Pagination{Offset: tt.offset, Limit: 10},
					HybridSearch: &searchparams.HybridSearch{Query: "test", Alpha: alpha, Vector: []float32{0.1, 0.2, 0.3}},
					Boost:        tt.boost,
				}

				_, err := explorer.GetClass(context.Background(), params)

				if tt.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, tt.wantErr)
				assert.Zero(t, searches.Load())
			})
		}
	}
}

// The legs of a hybrid search load the references without resolving them.
// The references are resolved for the returned page only.
func TestExplorerHybridResolvesReferencesOfThePage(t *testing.T) {
	props := search.SelectProperties{
		{Name: "name", IsPrimitive: true},
		{Name: "inCity", Refs: []search.SelectClass{{
			ClassName:     "City",
			RefProperties: search.SelectProperties{{Name: "name", IsPrimitive: true}},
		}}},
	}
	var (
		mu            sync.Mutex
		legProps      []search.SelectProperties
		resolvedNames [][]string
		resolvedProps []search.SelectProperties
	)
	searcher := newOffsetTestSearcher(func(filters.Pagination) {})
	vectorSearch, sparseSearch := searcher.vectorSearchFn, searcher.sparseObjectSearchFn
	searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
		mu.Lock()
		legProps = append(legProps, params.Properties)
		mu.Unlock()
		return vectorSearch(params)
	}
	searcher.sparseObjectSearchFn = func(params dto.GetParams) ([]*storobj.Object, []float32, error) {
		mu.Lock()
		legProps = append(legProps, params.Properties)
		mu.Unlock()
		return sparseSearch(params)
	}
	searcher.resolveReferencesFn = func(objs search.Results, props search.SelectProperties) (search.Results, error) {
		names := make([]string, len(objs))
		for i, obj := range objs {
			names[i] = obj.Schema.(map[string]any)["name"].(string)
		}
		resolvedNames = append(resolvedNames, names)
		resolvedProps = append(resolvedProps, props)
		return objs, nil
	}
	explorer := newHybridDepthTestExplorer(searcher)
	params := dto.GetParams{
		ClassName:    "TestClass",
		Pagination:   &filters.Pagination{Offset: 100, Limit: 3},
		HybridSearch: &searchparams.HybridSearch{Query: "test", Alpha: 0.5, Vector: []float32{0.1, 0.2, 0.3}},
		Properties:   props,
	}

	_, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	require.Len(t, legProps, 2)
	for _, p := range legProps {
		assert.False(t, p.HasRefs(), "a leg resolves references")
		assert.Equal(t, props.GetPropertyNames(), p.GetPropertyNames(), "a leg does not load every selected property")
	}
	assert.Equal(t, [][]string{{"Item 100", "Item 101", "Item 102"}}, resolvedNames)
	assert.Equal(t, []search.SelectProperties{props}, resolvedProps)
}
