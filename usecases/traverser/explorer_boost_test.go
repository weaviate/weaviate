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
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
)

// spyModulesProvider wraps the default fakeModulesProvider and records
// what results the module extension (reranker) receives.
type spyModulesProvider struct {
	*fakeModulesProvider
	extendCalls [][]strfmt.UUID
}

func (s *spyModulesProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector models.Vector,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	s.recordCall(in)
	return in, nil
}

func (s *spyModulesProvider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{},
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	s.recordCall(in)
	return in, nil
}

func (s *spyModulesProvider) recordCall(in []search.Result) {
	ids := make([]strfmt.UUID, len(in))
	for i, r := range in {
		ids[i] = r.ID
	}
	s.extendCalls = append(s.extendCalls, ids)
}

// makeBM25Results generates n results with deterministic BM25 scores and properties.
// Score decreases, likes increases — so boost on likes reverses the order.
func makeBM25Results(n int) []search.Result {
	results := make([]search.Result, n)
	for i := range results {
		results[i] = search.Result{
			ID:    strfmt.UUID(fmt.Sprintf("id-%02d", i)),
			Score: float32(n-i) * 0.1,
			Schema: map[string]interface{}{
				"name":  fmt.Sprintf("Item %02d", i),
				"likes": float64(i * 100),
			},
			Dims: 3,
		}
	}
	return results
}

// makeVectorResults generates n results with deterministic distances and properties.
func makeVectorResults(n int) []search.Result {
	results := make([]search.Result, n)
	for i := range results {
		results[i] = search.Result{
			ID:   strfmt.UUID(fmt.Sprintf("id-%02d", i)),
			Dist: float32(i) * 0.05,
			Schema: map[string]interface{}{
				"name":  fmt.Sprintf("Item %02d", i),
				"likes": float64(i * 100),
			},
			Dims: 3,
		}
	}
	return results
}

func newTestExplorer(searcher *fakeVectorSearcher, modules ModulesProvider) *Explorer {
	log, _ := test.NewNullLogger()
	metrics := &fakeMetrics{}
	metrics.On("AddUsageDimensions", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	conf := config.Config{
		QueryDefaults:       config.QueryDefaults{Limit: 100},
		QueryMaximumResults: 200,
	}
	explorer := NewExplorer(searcher, log, modules, metrics, conf)
	explorer.SetSchemaGetter(newFakeSchemaGetter("TestClass"))
	return explorer
}

func likesBoost(weight float32, depth int) *filters.Boost {
	return &filters.Boost{
		Weight: weight,
		Depth:  depth,
		Conditions: []filters.BoostCondition{{
			PropertyValue: &filters.PropertyValue{
				Path:     &filters.Path{Property: "likes"},
				Modifier: filters.PropertyValueModifierNone,
			},
			Weight: 1.0,
		}},
	}
}

func idsFromResponse(res []interface{}) []string {
	// GetClass returns []interface{} where each is the Schema map.
	// We can't read IDs from Schema unless we put _additional in.
	// Instead, read the "name" field which encodes the index.
	ids := make([]string, len(res))
	for i, r := range res {
		m := r.(map[string]interface{})
		ids[i] = m["name"].(string)
	}
	return ids
}

func Test_Explorer_BoostPipeline(t *testing.T) {
	t.Run("BM25 + boost reorders by likes", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		searcher.On("Search", mock.Anything).Return(makeBM25Results(20), nil)

		params := dto.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Offset: 0, Limit: 5},
			KeywordRanking: &searchparams.KeywordRanking{
				Query: "test", Type: "bm25", Properties: []string{"name"},
			},
			Boost: likesBoost(1.0, 20),
		}

		res, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)
		require.Len(t, res, 5)

		// With weight=1.0 on likes, highest-likes items should be first.
		// makeBM25Results gives likes = i*100, so id-19 has likes=1900, id-18=1800, etc.
		names := idsFromResponse(res)
		assert.Equal(t, "Item 19", names[0])
		assert.Equal(t, "Item 18", names[1])
	})

	t.Run("BM25 + boost page-through consistency", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		searcher.On("Search", mock.Anything).Return(makeBM25Results(20), nil)

		makeParams := func(offset, limit int) dto.GetParams {
			return dto.GetParams{
				ClassName:  "TestClass",
				Pagination: &filters.Pagination{Offset: offset, Limit: limit},
				KeywordRanking: &searchparams.KeywordRanking{
					Query: "test", Type: "bm25", Properties: []string{"name"},
				},
				Boost: likesBoost(0.8, 20),
			}
		}

		allRes, err := explorer.GetClass(context.Background(), makeParams(0, 10))
		require.NoError(t, err)
		require.Len(t, allRes, 10)

		p1Res, err := explorer.GetClass(context.Background(), makeParams(0, 5))
		require.NoError(t, err)
		require.Len(t, p1Res, 5)

		p2Res, err := explorer.GetClass(context.Background(), makeParams(5, 5))
		require.NoError(t, err)
		require.Len(t, p2Res, 5)

		allNames := idsFromResponse(allRes)
		p1Names := idsFromResponse(p1Res)
		p2Names := idsFromResponse(p2Res)

		combined := append(p1Names, p2Names...)
		assert.Equal(t, allNames, combined, "page 1 + page 2 should equal full result")
	})

	t.Run("nearVector + boost page-through consistency", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		explorer := newTestExplorer(searcher, getFakeModulesProvider())

		searcher.On("VectorSearch", mock.Anything, mock.Anything).Return(makeVectorResults(20), nil)

		makeParams := func(offset, limit int) dto.GetParams {
			return dto.GetParams{
				ClassName:  "TestClass",
				Pagination: &filters.Pagination{Offset: offset, Limit: limit},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}},
				},
				Boost: likesBoost(0.8, 20),
			}
		}

		allRes, err := explorer.GetClass(context.Background(), makeParams(0, 10))
		require.NoError(t, err)
		require.Len(t, allRes, 10)

		p1Res, err := explorer.GetClass(context.Background(), makeParams(0, 5))
		require.NoError(t, err)
		require.Len(t, p1Res, 5)

		p2Res, err := explorer.GetClass(context.Background(), makeParams(5, 5))
		require.NoError(t, err)
		require.Len(t, p2Res, 5)

		allNames := idsFromResponse(allRes)
		p1Names := idsFromResponse(p1Res)
		p2Names := idsFromResponse(p2Res)

		combined := append(p1Names, p2Names...)
		assert.Equal(t, allNames, combined, "page 1 + page 2 should equal full result")
	})

	t.Run("module extension receives boost-paginated results for BM25", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		spy := &spyModulesProvider{fakeModulesProvider: &fakeModulesProvider{}}
		explorer := newTestExplorer(searcher, spy)

		searcher.On("Search", mock.Anything).Return(makeBM25Results(20), nil)

		params := dto.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Offset: 0, Limit: 5},
			KeywordRanking: &searchparams.KeywordRanking{
				Query: "test", Type: "bm25", Properties: []string{"name"},
			},
			Boost: likesBoost(1.0, 20),
		}

		_, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)

		// Module extension should receive exactly 5 results (after boost pagination),
		// not 20 (the full overfetched pool).
		require.Len(t, spy.extendCalls, 1)
		assert.Len(t, spy.extendCalls[0], 5,
			"reranker should receive boost-paginated results, not the full pool")

		// The IDs should be in boost order (highest likes first).
		assert.Equal(t, strfmt.UUID("id-19"), spy.extendCalls[0][0])
	})

	t.Run("module extension receives boost-paginated results for nearVector", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		spy := &spyModulesProvider{fakeModulesProvider: &fakeModulesProvider{}}
		explorer := newTestExplorer(searcher, spy)

		searcher.On("VectorSearch", mock.Anything, mock.Anything).Return(makeVectorResults(20), nil)

		params := dto.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Offset: 0, Limit: 5},
			NearVector: &searchparams.NearVector{
				Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}},
			},
			Boost: likesBoost(1.0, 20),
		}

		_, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)

		require.Len(t, spy.extendCalls, 1)
		assert.Len(t, spy.extendCalls[0], 5,
			"reranker should receive boost-paginated results, not the full pool")
		assert.Equal(t, strfmt.UUID("id-19"), spy.extendCalls[0][0])
	})

	t.Run("module extension receives offset results for BM25 + boost", func(t *testing.T) {
		searcher := &fakeVectorSearcher{}
		spy := &spyModulesProvider{fakeModulesProvider: &fakeModulesProvider{}}
		explorer := newTestExplorer(searcher, spy)

		searcher.On("Search", mock.Anything).Return(makeBM25Results(20), nil)

		params := dto.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Offset: 5, Limit: 3},
			KeywordRanking: &searchparams.KeywordRanking{
				Query: "test", Type: "bm25", Properties: []string{"name"},
			},
			Boost: likesBoost(1.0, 20),
		}

		_, err := explorer.GetClass(context.Background(), params)
		require.NoError(t, err)

		require.Len(t, spy.extendCalls, 1)
		assert.Len(t, spy.extendCalls[0], 3,
			"reranker should receive 3 results (offset=5, limit=3)")

		// With weight=1.0, boost orders by likes desc: id-19, id-18, ..., id-00.
		// Offset=5 skips the first 5, so reranker should see id-14, id-13, id-12.
		assert.Equal(t, strfmt.UUID("id-14"), spy.extendCalls[0][0])
		assert.Equal(t, strfmt.UUID("id-13"), spy.extendCalls[0][1])
		assert.Equal(t, strfmt.UUID("id-12"), spy.extendCalls[0][2])
	})
}
