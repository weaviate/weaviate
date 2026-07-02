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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func mmrSel(limit uint32, balance float32) *searchparams.Selection {
	return &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: limit, Balance: balance}}
}

func makeHybridVectorResults(n int) []search.Result {
	results := make([]search.Result, n)
	for i := range results {
		docID := uint64(i)
		results[i] = search.Result{
			ID:     strfmt.UUID(fmt.Sprintf("id-%02d", i)),
			DocID:  &docID,
			Dist:   float32(i) * 0.05,
			Vector: []float32{float32(i), 0, 0},
			Schema: map[string]any{"name": fmt.Sprintf("Item %02d", i)},
			Dims:   3,
		}
	}
	return results
}

// Test_Explorer_HybridSelection: MMR on a hybrid query must run as a post-fusion
// pass on the fused pool, with the legs loading the vectors MMR needs.
func Test_Explorer_HybridSelection(t *testing.T) {
	t.Run("MMR selection runs post-fusion on hybrid results", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		var capturedAddl additional.Properties
		searcher.On("VectorSearch", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedAddl = args.Get(0).(dto.GetParams).AdditionalProperties
			}).
			Return(makeHybridVectorResults(10), nil)

		// Return the full diversified ordering (reversed), like the real DiversifyResults.
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, results []search.Result) ([]search.Result, error) {
			reversed := make([]search.Result, len(results))
			for i := range results {
				reversed[i] = results[len(results)-1-i]
			}
			return reversed, nil
		}

		// Query Limit is the pool (diversify all 10); MMR.Limit is the page size (3).
		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 10},
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
			Selection:    mmrSel(3, 0),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 3, "MMR.Limit is the page size")

		require.NotNil(t, searcher.diversifyCalledSel, "selection must reach the post-fusion pass")
		assert.Equal(t, uint32(3), searcher.diversifyCalledSel.MMR.Limit)

		assert.True(t, capturedAddl.Vector, "expected vector force-load on the hybrid leg")

		names := idsFromResponse(res)
		assert.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, names,
			"post-fusion selection did not reorder+paginate results: %v", names)
	})

	t.Run("MMR pagination diversifies disjoint windows", func(t *testing.T) {
		// offset advances by the query Limit (window size), so consecutive pages
		// diversify disjoint relevance windows: no overlap, and deep pages return
		// real results rather than paging off the end of a fixed pool.
		newExplorer := func() (*Explorer, *fakeVectorSearcher) {
			searcher := &fakeVectorSearcher{}
			searcher.On("VectorSearch", mock.Anything, mock.Anything).
				Return(makeHybridVectorResults(20), nil)
			searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, results []search.Result) ([]search.Result, error) {
				reversed := make([]search.Result, len(results))
				for i := range results {
					reversed[i] = results[len(results)-1-i]
				}
				return reversed, nil
			}
			return newTestExplorer(searcher, getFakeModulesProvider()), searcher
		}

		page := func(offset int) []string {
			explorer, _ := newExplorer()
			res, err := explorer.GetClass(context.Background(), dto.GetParams{
				ClassName:    "TestClass",
				Pagination:   &filters.Pagination{Offset: offset, Limit: 10},
				HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
				Selection:    mmrSel(3, 0),
			})
			require.NoError(t, err)
			return idsFromResponse(res)
		}

		// Window [0:10] reversed → top 3; window [10:20] reversed → top 3.
		page1 := page(0)
		page2 := page(10)
		require.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, page1)
		require.Equal(t, []string{"Item 19", "Item 18", "Item 17"}, page2,
			"deep page must diversify its own window, not return empty")
		// No overlap between disjoint windows.
		for _, n := range page1 {
			assert.NotContains(t, page2, n)
		}
	})

	t.Run("no Selection leaves the hybrid path untouched", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		var capturedAddl additional.Properties
		searcher.On("VectorSearch", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedAddl = args.Get(0).(dto.GetParams).AdditionalProperties
			}).
			Return(makeHybridVectorResults(10), nil)

		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 3},
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 3)

		assert.Nil(t, searcher.diversifyCalledSel)
		assert.False(t, capturedAddl.Vector)
		names := idsFromResponse(res)
		assert.Equal(t, "Item 00", names[0], "plain hybrid order should be preserved")
	})

	t.Run("boost re-ranks the fused pool before MMR (terminal)", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		// likes increases with index, so boost(weight=1) reverses the fused order.
		results := makeHybridVectorResults(10)
		for i := range results {
			results[i].Schema.(map[string]any)["likes"] = float64(i * 100)
		}
		searcher.On("VectorSearch", mock.Anything, mock.Anything).Return(results, nil)

		var diversifyInput []string
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, in []search.Result) ([]search.Result, error) {
			diversifyInput = make([]string, len(in))
			for i, r := range in {
				diversifyInput[i] = r.Schema.(map[string]any)["name"].(string)
			}
			return in, nil
		}

		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 10},
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
			Boost:        likesBoost(1.0, 10),
			Selection:    mmrSel(3, 1),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 3, "MMR.Limit is the page size")

		require.Len(t, diversifyInput, 10)
		assert.Equal(t, "Item 09", diversifyInput[0], "boost must re-rank the fused pool before MMR")
		assert.False(t, searcher.diversifyCalledRelevanceFromDist, "boost active ⇒ score relevance")
		assert.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, idsFromResponse(res))
	})

	t.Run("Boost.Depth deepens the leg fetch, MMR over top Limit", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		results := makeHybridVectorResults(50)
		for i := range results {
			results[i].Schema.(map[string]any)["likes"] = float64(i * 100)
		}
		var legLimit int
		searcher.On("VectorSearch", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				legLimit = args.Get(0).(dto.GetParams).Pagination.Limit
			}).
			Return(results, nil)

		var diversifyInputLen int
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, in []search.Result) ([]search.Result, error) {
			diversifyInputLen = len(in)
			return in, nil
		}

		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 10}, // MMR candidate pool
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
			Boost:        likesBoost(1.0, 40), // Depth = 40
			Selection:    mmrSel(5, 0.5),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 5, "page size is MMR.Limit")

		assert.GreaterOrEqual(t, legLimit, 40, "leg fetch must reach Boost.Depth")
		assert.Equal(t, 10, diversifyInputLen, "MMR pool must be the query Limit")
	})
}
