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
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
)

// droppingRerankProvider plays a reranker that keeps only every second result
// and asks the search for depth candidates.
type droppingRerankProvider struct {
	*fakeModulesProvider
	depth    int
	depthErr error
	// pageEnds records the page end of every depth request.
	pageEnds []int
	// reverse returns the kept results in reverse order, as a reranker that
	// sorts by its own score can.
	reverse  bool
	received []int
	// calls lists every extend call as "<params>:<results>", for example
	// "generate+rerank:20".
	calls []string
}

func (p *droppingRerankProvider) RerankFetchDepth(ctx context.Context, className string, pageEnd int) (int, error) {
	p.pageEnds = append(p.pageEnds, pageEnd)
	if p.depthErr != nil || p.depth <= 0 {
		return 0, p.depthErr
	}
	return max(p.depth, pageEnd), nil
}

func (p *droppingRerankProvider) ValidateSearchParam(name string, value any, className string) error {
	return nil
}

func (p *droppingRerankProvider) VectorFromSearchParam(ctx context.Context, className, targetVector, tenant,
	param string, params any, findVectorFn modulecapabilities.FindVectorFn[[]float32],
) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

func (p *droppingRerankProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]any, searchVector models.Vector, argumentModuleParams map[string]any,
) ([]search.Result, error) {
	return p.keepEven(in, moduleParams), nil
}

func (p *droppingRerankProvider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]any, argumentModuleParams map[string]any,
) ([]search.Result, error) {
	return p.keepEven(in, moduleParams), nil
}

func (p *droppingRerankProvider) keepEven(in []search.Result, moduleParams map[string]any) []search.Result {
	names := make([]string, 0, len(moduleParams))
	for name := range moduleParams {
		names = append(names, name)
	}
	slices.Sort(names)
	p.calls = append(p.calls, fmt.Sprintf("%s:%d", strings.Join(names, "+"), len(in)))
	if _, ok := moduleParams["rerank"]; !ok {
		return in
	}
	p.received = append(p.received, len(in))
	kept := make([]search.Result, 0, len(in))
	for i, r := range in {
		if i%2 == 0 {
			kept = append(kept, r)
		}
	}
	if p.reverse {
		slices.Reverse(kept)
	}
	return kept
}

func TestExplorerRerankOverfetch(t *testing.T) {
	type searchCase struct {
		name string
		// params sets the search type on the request.
		params func(p *dto.GetParams)
		// expect prepares the searcher fake and returns a pointer to the
		// pagination it was called with.
		expect func(s *fakeVectorSearcher) *filters.Pagination
	}
	searches := []searchCase{
		{
			name: "bm25",
			params: func(p *dto.GetParams) {
				p.KeywordRanking = &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}}
			},
			expect: func(s *fakeVectorSearcher) *filters.Pagination {
				seen := &filters.Pagination{}
				s.searchFn = func(params dto.GetParams) ([]search.Result, error) {
					*seen = *params.Pagination
					return page(makeBM25Results(200), params.Pagination), nil
				}
				return seen
			},
		},
		{
			name: "nearVector",
			params: func(p *dto.GetParams) {
				p.NearVector = &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}}
			},
			expect: func(s *fakeVectorSearcher) *filters.Pagination {
				seen := &filters.Pagination{}
				s.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
					*seen = *params.Pagination
					return page(makeVectorResults(200), params.Pagination), nil
				}
				return seen
			},
		},
		{
			name: "hybrid",
			params: func(p *dto.GetParams) {
				p.HybridSearch = &searchparams.HybridSearch{Query: "test", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}}
			},
			expect: func(s *fakeVectorSearcher) *filters.Pagination {
				seen := &filters.Pagination{}
				s.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
					*seen = *params.Pagination
					return page(makeHybridVectorResults(200), params.Pagination), nil
				}
				return seen
			},
		},
		{
			// The nearText leg must not run the reranker itself: the fused
			// list is reranked once.
			name: "hybrid nearText",
			params: func(p *dto.GetParams) {
				p.HybridSearch = &searchparams.HybridSearch{
					Query: "test", Alpha: 1,
					NearTextParams: &searchparams.NearTextParams{Values: []string{"test"}},
				}
			},
			expect: func(s *fakeVectorSearcher) *filters.Pagination {
				seen := &filters.Pagination{}
				s.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
					*seen = *params.Pagination
					return page(makeHybridVectorResults(200), params.Pagination), nil
				}
				return seen
			},
		},
	}

	tests := []struct {
		name            string
		depth           int
		offset          int
		limit           int
		noRerank        bool
		zeroWeightBoost bool
		// wantFetched is the limit the store must see; 0 means the page
		// itself (no over-fetch).
		wantFetched int
		wantNames   []string
		// wantLen is checked instead of wantNames for hybrid, where the
		// vector leg always fetches QueryHybridMaximumResults or more.
		wantLen int
	}{
		{
			name: "no over-fetch keeps the page semantics", depth: 0, limit: 6,
			wantNames: []string{"Item 00", "Item 02", "Item 04"}, wantLen: 3,
		},
		{
			name: "over-fetch fills the page", depth: 20, limit: 5, wantFetched: 20,
			wantNames: []string{"Item 00", "Item 02", "Item 04", "Item 06", "Item 08"}, wantLen: 5,
		},
		{
			name: "second page continues where the first ended", depth: 20, offset: 5, limit: 5, wantFetched: 20,
			wantNames: []string{"Item 10", "Item 12", "Item 14", "Item 16", "Item 18"}, wantLen: 5,
		},
		{
			// The results are the survivors of the top candidates. A page
			// past them is empty, whatever the store holds further down.
			name: "a page beyond the survivors is empty", depth: 20, offset: 30, limit: 5, wantFetched: 35,
			wantNames: []string{}, wantLen: 0,
		},
		{
			// The candidates have to reach the end of the page, or a page
			// past the depth could never return anything.
			name: "a page that ends beyond the depth fetches up to its end", depth: 4, offset: 3, limit: 5, wantFetched: 8,
			wantNames: []string{"Item 06"}, wantLen: 1,
		},
		{
			name: "a limit above the depth fetches the limit", depth: 4, limit: 10, wantFetched: 10,
			wantNames: []string{"Item 00", "Item 02", "Item 04", "Item 06", "Item 08"}, wantLen: 5,
		},
		{
			name: "a page that ends exactly at the depth is over-fetched", depth: 20, offset: 16, limit: 4, wantFetched: 20,
			wantNames: []string{}, wantLen: 0,
		},
		{
			name: "zero-weight boost does not switch over-fetch off", depth: 20, limit: 5, zeroWeightBoost: true, wantFetched: 20,
			wantNames: []string{"Item 00", "Item 02", "Item 04", "Item 06", "Item 08"}, wantLen: 5,
		},
		{
			name: "depth above the query maximum is capped", depth: 5000, limit: 3, wantFetched: 200,
			wantNames: []string{"Item 00", "Item 02", "Item 04"}, wantLen: 3,
		},
		{
			name: "fewer survivors than the page returns what survived", depth: 6, limit: 5, wantFetched: 6,
			wantNames: []string{"Item 00", "Item 02", "Item 04"}, wantLen: 3,
		},
		{
			name: "no rerank in the request means no over-fetch", depth: 20, limit: 5, noRerank: true,
			wantNames: []string{"Item 00", "Item 01", "Item 02", "Item 03", "Item 04"}, wantLen: 5,
		},
	}

	for _, s := range searches {
		for _, tt := range tests {
			t.Run(s.name+"/"+tt.name, func(t *testing.T) {
				searcher := &fakeVectorSearcher{}
				provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: tt.depth}
				explorer := newTestExplorer(searcher, provider)
				seen := s.expect(searcher)

				params := dto.GetParams{
					ClassName:  "TestClass",
					Pagination: &filters.Pagination{Offset: tt.offset, Limit: tt.limit},
				}
				s.params(&params)
				if !tt.noRerank {
					params.AdditionalProperties.ModuleParams = rerankParams()
				}
				if tt.zeroWeightBoost {
					params.Boost = likesBoost(0, 50)
				}
				rerankBefore := rank.Params{}
				if rerank, ok := params.AdditionalProperties.ModuleParams["rerank"].(*rank.Params); ok {
					rerankBefore = *rerank
				}

				res, err := explorer.GetClass(context.Background(), params)

				require.NoError(t, err)
				if tt.noRerank {
					require.Empty(t, provider.received)
				} else {
					require.Len(t, provider.received, 1, "the reranker must run exactly once")
					// The request's rerank params are shared with the caller.
					assert.Equal(t, rerankBefore, *params.AdditionalProperties.ModuleParams["rerank"].(*rank.Params))
				}
				if strings.HasPrefix(s.name, "hybrid") {
					// The vector leg fetches at least QueryHybridMaximumResults,
					// so only the page and the fetch floor are checked.
					assert.Len(t, res, tt.wantLen)
					if tt.wantFetched > 0 {
						assert.GreaterOrEqual(t, seen.Limit, tt.wantFetched)
						assert.Equal(t, 0, seen.Offset)
					}
					return
				}
				assert.Equal(t, tt.wantNames, idsFromResponse(res))
				if tt.wantFetched > 0 {
					assert.Equal(t, filters.Pagination{Offset: 0, Limit: tt.wantFetched}, *seen)
				} else {
					assert.Equal(t, filters.Pagination{Offset: tt.offset, Limit: tt.limit}, *seen)
				}
			})
		}
	}
}

func TestExplorerRerankOverfetchDepthError(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{
		fakeModulesProvider: &fakeModulesProvider{},
		depthErr:            errors.New("X-Typesafeai-Fetch-Depth must be a number"),
	}
	explorer := newTestExplorer(searcher, provider)
	searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
		t.Fatal("the store must not be searched")
		return nil, nil
	}

	params := dto.GetParams{
		ClassName:      "TestClass",
		Pagination:     &filters.Pagination{Limit: 5},
		KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	_, err := explorer.GetClass(context.Background(), params)

	require.ErrorContains(t, err, "X-Typesafeai-Fetch-Depth must be a number")
}

// The store checks offset+limit against QueryMaximumResults, but the
// over-fetch starts at offset 0. The page has to fail the same way.
func TestExplorerRerankOverfetchPageBeyondQueryMaximumResults(t *testing.T) {
	tests := []struct {
		name   string
		offset int
		limit  int
	}{
		{name: "page ends beyond the maximum", offset: 195, limit: 10},
		{name: "offset beyond the maximum", offset: 300, limit: 5},
		{name: "offset+limit overflows", offset: math.MaxInt - 2, limit: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher := &fakeVectorSearcher{}
			provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 20}
			explorer := newTestExplorer(searcher, provider)
			searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
				t.Fatal("the store must not be searched")
				return nil, nil
			}
			params := dto.GetParams{
				ClassName:      "TestClass",
				Pagination:     &filters.Pagination{Offset: tt.offset, Limit: tt.limit},
				KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
			}
			params.AdditionalProperties.ModuleParams = rerankParams()

			_, err := explorer.GetClass(context.Background(), params)

			require.ErrorContains(t, err, "query maximum results exceeded")
			assert.Empty(t, provider.pageEnds, "the module is not asked for a page it cannot get")
		})
	}
}

// The module gets the end of the page, so that it can refuse a page it
// cannot judge whole (maxDocuments) before the search runs.
func TestExplorerRerankOverfetchPassesThePageEnd(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 20}
	explorer := newTestExplorer(searcher, provider)
	searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
		return page(makeBM25Results(200), params.Pagination), nil
	}
	params := dto.GetParams{
		ClassName:      "TestClass",
		Pagination:     &filters.Pagination{Offset: 30, Limit: 10},
		KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	_, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	assert.Equal(t, []int{40}, provider.pageEnds)
}

func TestExplorerRerankOverfetchSkippedWithBoost(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 50}
	explorer := newTestExplorer(searcher, provider)
	var seen filters.Pagination
	searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
		seen = *params.Pagination
		return page(makeBM25Results(200), params.Pagination), nil
	}

	params := dto.GetParams{
		ClassName:      "TestClass",
		Pagination:     &filters.Pagination{Limit: 5},
		KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
		Boost:          likesBoost(1.0, 20),
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	res, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	// Boost fetches its own depth and cuts back to the page before the
	// reranker runs, so the reranker sees the boosted page of 5.
	assert.Equal(t, filters.Pagination{Offset: 0, Limit: 20}, seen)
	assert.Equal(t, []int{5}, provider.received)
	assert.Len(t, res, 3)
}

func TestExplorerRerankOverfetchSkipped(t *testing.T) {
	bm25 := &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}}
	tests := []struct {
		name   string
		params func(p *dto.GetParams)
	}{
		{
			// A negative limit is a flag (search by distance, default limit),
			// not a page size.
			name: "search by distance without a limit",
			params: func(p *dto.GetParams) {
				p.NearVector = &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}, Distance: 0.4, WithDistance: true,
				}
				p.Pagination = &filters.Pagination{Limit: filters.LimitFlagSearchByDist}
			},
		},
		{
			// Autocut already picks the candidates by the jumps in their
			// scores. More candidates would move the jump it cuts at.
			name: "autocut",
			params: func(p *dto.GetParams) {
				p.KeywordRanking = bm25
				p.Pagination = &filters.Pagination{Limit: 5, Autocut: 1}
			},
		},
		{
			name: "groupBy",
			params: func(p *dto.GetParams) {
				p.KeywordRanking = bm25
				p.Pagination = &filters.Pagination{Limit: 5}
				p.GroupBy = &searchparams.GroupBy{Property: "name", Groups: 2, ObjectsPerGroup: 2}
			},
		},
		{
			name: "legacy group",
			params: func(p *dto.GetParams) {
				p.KeywordRanking = bm25
				p.Pagination = &filters.Pagination{Limit: 5}
				p.Group = &dto.GroupParams{Strategy: "closest", Force: 0.5}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher := &fakeVectorSearcher{}
			provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 40}
			explorer := newTestExplorer(searcher, provider)
			params := dto.GetParams{ClassName: "TestClass"}
			tt.params(&params)
			params.AdditionalProperties.ModuleParams = rerankParams()
			want := *params.Pagination

			var seen filters.Pagination
			record := func(params dto.GetParams) ([]search.Result, error) {
				seen = *params.Pagination
				return page(makeVectorResults(60), params.Pagination), nil
			}
			searcher.searchFn = record
			searcher.vectorSearchFn = record

			// Only the pagination the store sees matters here: grouping the
			// fake results can fail without affecting it.
			_, _ = explorer.GetClass(context.Background(), params)

			assert.Equal(t, want, seen)
		})
	}
}

// Paging through over-fetched results must not repeat or reorder a result.
// The candidates of a page are the top max(depth, offset+limit), so a page
// past the depth fetches more.
func TestExplorerRerankOverfetchPagesAreConsistent(t *testing.T) {
	const depth, limit = 50, 20
	wantFetched := map[int]int{0: 50, 20: 50, 40: 60, 60: 80}
	var all []string
	for offset := 0; offset <= 60; offset += limit {
		searcher := &fakeVectorSearcher{}
		provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: depth}
		explorer := newTestExplorer(searcher, provider)
		var seen filters.Pagination
		searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
			seen = *params.Pagination
			return page(makeBM25Results(200), params.Pagination), nil
		}
		params := dto.GetParams{
			ClassName:      "TestClass",
			Pagination:     &filters.Pagination{Offset: offset, Limit: limit},
			KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
		}
		params.AdditionalProperties.ModuleParams = rerankParams()

		res, err := explorer.GetClass(context.Background(), params)

		require.NoError(t, err)
		assert.Equal(t, filters.Pagination{Offset: 0, Limit: wantFetched[offset]}, seen, "offset %d", offset)
		all = append(all, idsFromResponse(res)...)
	}

	// The 25 survivors of the top 50 in search order, each once. The pages at
	// offset 40 and 60 fetch 60 and 80 candidates, which leaves 30 and 40
	// survivors: fewer than their offset, so they are empty.
	want := make([]string, 0, 25)
	for i := 0; i <= 48; i += 2 {
		want = append(want, fmt.Sprintf("Item %02d", i))
	}
	assert.Equal(t, want, all)
}

// The page is cut from what the response filters keep. A result that the
// certainty filter removes must not take a place on the page.
func TestExplorerRerankOverfetchCutsAfterResponseFilters(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	// The reranker puts the most distant survivors first.
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 20, reverse: true}
	explorer := newTestExplorer(searcher, provider)
	searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
		return page(makeVectorResults(200), params.Pagination), nil
	}
	params := dto.GetParams{
		ClassName:  "TestClass",
		Pagination: &filters.Pagination{Limit: 3},
		// makeVectorResults gives item i the distance i*0.05, so a certainty
		// of 0.8 keeps items 0 to 8.
		NearVector: &searchparams.NearVector{
			Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}, Certainty: 0.8,
		},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	res, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	assert.Equal(t, []string{"Item 08", "Item 06", "Item 04"}, idsFromResponse(res))
}

func TestExplorerRerankOverfetchWithoutALimit(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 150}
	explorer := newTestExplorer(searcher, provider)
	var seen filters.Pagination
	searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
		seen = *params.Pagination
		return page(makeBM25Results(200), params.Pagination), nil
	}
	params := dto.GetParams{
		ClassName: "TestClass",
		// GraphQL sets this flag when the client gives an offset and no limit.
		Pagination:     &filters.Pagination{Offset: 10, Limit: filters.LimitFlagNotSet},
		KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	res, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	assert.Equal(t, filters.Pagination{Offset: 0, Limit: 150}, seen)
	// 75 survivors, minus the offset of 10. The default limit is 100.
	require.Len(t, res, 65)
	assert.Equal(t, "Item 20", idsFromResponse(res)[0])
}

// A hybrid query without a limit gets the page a query without a reranker
// gets: QueryHybridMaximumResults results, not the default limit.
func TestExplorerRerankOverfetchHybridWithoutALimit(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 150}
	log, _ := test.NewNullLogger()
	metrics := &fakeMetrics{}
	metrics.On("AddUsageDimensions", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	conf := config.Config{
		QueryDefaults:             config.QueryDefaults{Limit: 10},
		QueryMaximumResults:       200,
		QueryHybridMaximumResults: 100,
	}
	explorer := NewExplorer(searcher, log, provider, metrics, conf)
	explorer.SetSchemaGetter(newFakeSchemaGetter("TestClass"))
	searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
		return page(makeHybridVectorResults(200), params.Pagination), nil
	}
	params := dto.GetParams{
		ClassName:    "TestClass",
		Pagination:   &filters.Pagination{Offset: 10, Limit: filters.LimitFlagNotSet},
		HybridSearch: &searchparams.HybridSearch{Query: "test", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	res, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	assert.Equal(t, []int{110}, provider.pageEnds)
	// 75 survivors of 150 candidates, minus the offset of 10.
	require.Len(t, res, 65)
}

// The other additional properties must see the page the client gets, not
// every candidate the reranker kept: a generative module would otherwise be
// called for results that are thrown away.
func TestExplorerRerankOverfetchRunsOtherPropertiesOnThePage(t *testing.T) {
	type searchCase struct {
		name   string
		params func(p *dto.GetParams)
	}
	searches := []searchCase{
		{name: "bm25", params: func(p *dto.GetParams) {
			p.KeywordRanking = &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}}
		}},
		{name: "nearVector", params: func(p *dto.GetParams) {
			p.NearVector = &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}}
		}},
		{name: "hybrid", params: func(p *dto.GetParams) {
			p.HybridSearch = &searchparams.HybridSearch{Query: "test", Alpha: 1, Vector: []float32{0.1, 0.2, 0.3}}
		}},
	}
	tests := []struct {
		name      string
		depth     int
		offset    int
		wantCalls []string
	}{
		{name: "over-fetch: rerank on the candidates, the rest on the page", depth: 20, wantCalls: []string{"rerank:20", "generate+summary:5"}},
		{name: "over-fetch with an offset", depth: 20, offset: 2, wantCalls: []string{"rerank:20", "generate+summary:5"}},
		{name: "page past the survivors: the rest does not run", depth: 20, offset: 12, wantCalls: []string{"rerank:20"}},
		{name: "no over-fetch: one call with everything", depth: 0, wantCalls: []string{"generate+rerank+summary:5"}},
	}
	for _, s := range searches {
		for _, tt := range tests {
			t.Run(s.name+"/"+tt.name, func(t *testing.T) {
				searcher := &fakeVectorSearcher{}
				provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: tt.depth}
				explorer := newHybridDepthTestExplorer(searcher)
				explorer.modulesProvider = provider
				results := func(params dto.GetParams) ([]search.Result, error) {
					return page(makeHybridVectorResults(200), params.Pagination), nil
				}
				searcher.searchFn = results
				searcher.vectorSearchFn = results

				params := dto.GetParams{ClassName: "TestClass", Pagination: &filters.Pagination{Offset: tt.offset, Limit: 5}}
				s.params(&params)
				params.AdditionalProperties.ModuleParams = rerankParams()
				params.AdditionalProperties.ModuleParams["generate"] = "prompt"
				params.AdditionalProperties.ModuleParams["summary"] = "params"

				_, err := explorer.GetClass(context.Background(), params)

				require.NoError(t, err)
				assert.Equal(t, tt.wantCalls, provider.calls)
			})
		}
	}
}

// The query profile travels on the first result. When the reranker moves or
// drops that result, the profile must end up on the first result of the page.
func TestExplorerRerankOverfetchKeepsTheQueryProfile(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &droppingRerankProvider{fakeModulesProvider: &fakeModulesProvider{}, depth: 20, reverse: true}
	explorer := newTestExplorer(searcher, provider)
	searcher.searchFn = func(params dto.GetParams) ([]search.Result, error) {
		res := page(makeBM25Results(200), params.Pagination)
		res[0].AdditionalProperties = models.AdditionalProperties{"queryProfile": "the profile"}
		return res, nil
	}
	params := dto.GetParams{
		ClassName:      "TestClass",
		Pagination:     &filters.Pagination{Offset: 2, Limit: 3},
		KeywordRanking: &searchparams.KeywordRanking{Query: "test", Type: "bm25", Properties: []string{"name"}},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	res, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	// Reversed survivors of the top 20 are 18, 16, 14, ...: the page at offset 2.
	require.Equal(t, []string{"Item 14", "Item 12", "Item 10"}, idsFromResponse(res))
	for i, result := range res {
		additional, _ := result.(map[string]any)["_additional"].(map[string]any)
		if i == 0 {
			assert.Equal(t, "the profile", additional["queryProfile"])
		} else {
			assert.NotContains(t, additional, "queryProfile")
		}
	}
}
