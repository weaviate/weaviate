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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// The vector leg of a hybrid search has to fetch up to the end of the
// requested page: the page is cut from the fused list afterwards.
func TestExplorerHybridVectorLegReachesThePage(t *testing.T) {
	tests := []struct {
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher := &fakeVectorSearcher{}
			explorer := newHybridDepthTestExplorer(searcher)
			var seen filters.Pagination
			searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
				seen = *params.Pagination
				return page(makeHybridVectorResults(200), params.Pagination), nil
			}
			params := dto.GetParams{
				ClassName:    "TestClass",
				Pagination:   &filters.Pagination{Offset: tt.offset, Limit: tt.limit},
				HybridSearch: &searchparams.HybridSearch{Query: "test", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
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
