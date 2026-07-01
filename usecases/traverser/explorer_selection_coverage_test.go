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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestPaginateResults(t *testing.T) {
	mk := func(n int) []search.Result {
		r := make([]search.Result, n)
		for i := range r {
			r[i] = search.Result{ID: strfmt.UUID(fmt.Sprintf("id-%02d", i))}
		}
		return r
	}
	ids := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i] = res[i].ID.String()
		}
		return out
	}

	tests := []struct {
		name           string
		n, offset, lim int
		want           []string
	}{
		{"first window", 10, 0, 3, []string{"id-00", "id-01", "id-02"}},
		{"second disjoint window", 10, 3, 3, []string{"id-03", "id-04", "id-05"}},
		{"limit past end clamps", 5, 3, 10, []string{"id-03", "id-04"}},
		{"offset at len is empty", 5, 5, 3, []string{}},
		{"offset past len is empty", 5, 8, 3, []string{}},
		{"negative offset treated as zero", 5, -2, 2, []string{"id-00", "id-01"}},
		{"non-positive limit returns tail from offset", 5, 2, 0, []string{"id-02", "id-03", "id-04"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := paginateResults(mk(tt.n), tt.offset, tt.lim)
			assert.Equal(t, tt.want, ids(got))
		})
	}
}

func Test_Explorer_VectorSelectionPagination(t *testing.T) {
	newExplorer := func() *Explorer {
		searcher := &fakeVectorSearcher{}
		searcher.On("VectorSearch", mock.Anything, mock.Anything).Return(makeHybridVectorResults(20), nil)
		searcher.diversifyFn = func(sel *searchparams.Selection, class, target string, results []search.Result) ([]search.Result, error) {
			reversed := make([]search.Result, len(results))
			for i := range results {
				reversed[i] = results[len(results)-1-i]
			}
			return reversed, nil
		}
		return newTestExplorer(searcher, getFakeModulesProvider())
	}

	page := func(offset int) []string {
		res, err := newExplorer().GetClass(context.Background(), dto.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Offset: offset, Limit: 10},
			NearVector: &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}},
			Selection:  mmrSel(3, 0),
		})
		require.NoError(t, err)
		return idsFromResponse(res)
	}

	page1 := page(0)
	page2 := page(10)
	require.Equal(t, []string{"Item 09", "Item 08", "Item 07"}, page1)
	require.Equal(t, []string{"Item 19", "Item 18", "Item 17"}, page2)
	for _, n := range page1 {
		assert.NotContains(t, page2, n)
	}
}

func Test_Explorer_VectorSelectionVectorStripping(t *testing.T) {
	run := func(requestVector bool) []search.Result {
		searcher := &fakeVectorSearcher{}
		searcher.On("VectorSearch", mock.Anything, mock.Anything).Return(makeHybridVectorResults(10), nil)
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		res, _, err := explorer.getClassVectorSearch(context.Background(), dto.GetParams{
			ClassName:            "TestClass",
			Pagination:           &filters.Pagination{Offset: 0, Limit: 10},
			NearVector:           &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}},
			Selection:            mmrSel(3, 0),
			AdditionalProperties: additional.Properties{Vector: requestVector},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res)
		return res
	}

	t.Run("stripped when unrequested", func(t *testing.T) {
		for _, r := range run(false) {
			assert.Nil(t, r.Vector)
		}
	})

	t.Run("survives when requested", func(t *testing.T) {
		for _, r := range run(true) {
			assert.NotNil(t, r.Vector)
		}
	})
}

func Test_Explorer_HybridSelectionNamedTarget(t *testing.T) {
	run := func(requestVector bool) ([]search.Result, *fakeVectorSearcher, additional.Properties) {
		searcher := &fakeVectorSearcher{}
		results := makeHybridVectorResults(10)
		for i := range results {
			results[i].Vectors = map[string]models.Vector{"custom": []float32{float32(i), 0, 0}}
		}
		var capturedAddl additional.Properties
		searcher.On("VectorSearch", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedAddl = args.Get(0).(dto.GetParams).AdditionalProperties
			}).
			Return(results, nil)
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		addl := additional.Properties{}
		if requestVector {
			addl.Vectors = []string{"custom"}
		}
		res, err := explorer.Hybrid(context.Background(), dto.GetParams{
			ClassName:            "TestClass",
			Pagination:           &filters.Pagination{Offset: 0, Limit: 10},
			HybridSearch:         &searchparams.HybridSearch{Query: "foo", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}, TargetVectors: []string{"custom"}},
			Selection:            mmrSel(3, 0),
			AdditionalProperties: addl,
		})
		require.NoError(t, err)
		require.NotEmpty(t, res)
		return res, searcher, capturedAddl
	}

	t.Run("named target reaches MMR and both legs force-load it", func(t *testing.T) {
		_, searcher, capturedAddl := run(false)
		assert.Equal(t, "custom", searcher.diversifyCalledTarget)
		assert.Contains(t, capturedAddl.Vectors, "custom")
	})

	t.Run("stripped when unrequested", func(t *testing.T) {
		res, _, _ := run(false)
		for _, r := range res {
			assert.NotContains(t, r.Vectors, "custom")
		}
	})

	t.Run("survives when requested", func(t *testing.T) {
		res, _, _ := run(true)
		for _, r := range res {
			assert.Contains(t, r.Vectors, "custom")
		}
	})
}
