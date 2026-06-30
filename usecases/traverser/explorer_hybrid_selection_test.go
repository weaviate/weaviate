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

// Test_Explorer_HybridSelection pins the fix for the post-fusion MMR no-op bug.
// Before the fix, diversity_selection on a hybrid query was forwarded to the
// vector leg only and then discarded by fusion, so no post-fusion pass ever
// ran. These tests assert that the pass is invoked on the fused pool and that
// the legs are told to load the vectors MMR needs.
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

		// Return the full diversified ordering (reversed), matching the real
		// DiversifyResults contract; pagination applies the page size afterwards.
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, results []search.Result) ([]search.Result, error) {
			reversed := make([]search.Result, len(results))
			for i := range results {
				reversed[i] = results[len(results)-1-i]
			}
			return reversed, nil
		}

		// Y model: query Limit is the candidate pool (diversify all 10); MMR.Limit
		// is the page size (return 3).
		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 10},
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
			Selection:    mmrSel(3, 0),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 3, "MMR.Limit is the page size")

		// 1. Selection reached the post-fusion pass (previously it never did).
		require.NotNil(t, searcher.diversifyCalledSel)
		assert.Equal(t, uint32(3), searcher.diversifyCalledSel.MMR.Limit)

		// 2. The dense leg was instructed to load vectors for the MMR pass.
		assert.True(t, capturedAddl.Vector, "expected vector force-load on the hybrid leg")

		// 3. The output reflects the post-fusion reorder: the fused pool was
		// ordered by distance ascending (Item 00 first); after the reversing
		// diversifyFn the most-distant items come first, then paginated to 3.
		names := idsFromResponse(res)
		assert.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, names,
			"post-fusion selection did not reorder+paginate results: %v", names)
	})

	t.Run("MMR pagination tiles the diversified pool", func(t *testing.T) {
		// Y model: query Limit = pool (10), MMR.Limit = page size (3), Offset pages
		// the diversified pool. Pages must tile one stable ordering with no overlap.
		newExplorer := func() (*Explorer, *fakeVectorSearcher) {
			searcher := &fakeVectorSearcher{}
			searcher.On("VectorSearch", mock.Anything, mock.Anything).
				Return(makeHybridVectorResults(10), nil)
			// Full reversed ordering (deterministic, offset-independent).
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

		page1 := page(0)
		page2 := page(3)
		require.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, page1)
		require.Equal(t, []string{"Item 06", "Item 05", "Item 04"}, page2)
		// No overlap between consecutive pages.
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

		// No diversity pass, no forced vector load — zero overhead when unused.
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

		// Capture the pool entering MMR to prove boost ran first.
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

		// Boost ran before MMR: the fused pool arrives boost-ordered (likes desc),
		// so Item 09 (highest likes) leads instead of the fusion-relevance leader.
		require.Len(t, diversifyInput, 10)
		assert.Equal(t, "Item 09", diversifyInput[0], "boost must re-rank the fused pool before MMR")

		// MMR relevance uses the post-boost score, not raw distance.
		assert.False(t, searcher.diversifyCalledRelevanceFromDist)

		// Boost is applied exactly once (inside the selection fn); the output is the
		// boost-ordered, MMR-passthrough pool paginated to MMR.Limit.
		assert.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, idsFromResponse(res))
	})
}
