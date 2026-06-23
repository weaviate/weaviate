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

		// Reverse the fused order then truncate to the MMR limit, so we can
		// detect that the pass actually ran on the fused candidate pool.
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, results []search.Result) ([]search.Result, error) {
			reversed := make([]search.Result, len(results))
			for i := range results {
				reversed[i] = results[len(results)-1-i]
			}
			if k := int(sel.MMR.Limit); k > 0 && k < len(reversed) {
				reversed = reversed[:k]
			}
			return reversed, nil
		}

		params := dto.GetParams{
			ClassName:    "TestClass",
			Pagination:   &filters.Pagination{Offset: 0, Limit: 3},
			HybridSearch: &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
			Selection:    mmrSel(3, 0),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 3)

		// 1. Selection reached the post-fusion pass (previously it never did).
		require.NotNil(t, searcher.diversifyCalledSel)
		assert.Equal(t, uint32(3), searcher.diversifyCalledSel.MMR.Limit)

		// 2. The dense leg was instructed to load vectors for the MMR pass.
		assert.True(t, capturedAddl.Vector, "expected vector force-load on the hybrid leg")

		// 3. The output reflects the post-fusion reorder: the fused pool was
		// ordered by distance ascending (Item 00 first); after the reversing
		// diversifyFn the most-distant items come first.
		names := idsFromResponse(res)
		assert.Equal(t, "Item 09", names[0], "post-fusion selection did not reorder results: %v", names)
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
}
