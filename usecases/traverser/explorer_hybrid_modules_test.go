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
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
)

// recordingModulesProvider records every call that runs additional
// properties of modules as "<get|list>:<params>:<results>", for example
// "list:generate+rerank:5".
type recordingModulesProvider struct {
	*fakeModulesProvider
	calls []string
}

func (p *recordingModulesProvider) ValidateSearchParam(name string, value any, className string) error {
	return nil
}

func (p *recordingModulesProvider) VectorFromSearchParam(ctx context.Context, className, targetVector, tenant,
	param string, params any, findVectorFn modulecapabilities.FindVectorFn[[]float32],
) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

func (p *recordingModulesProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]any, searchVector models.Vector, argumentModuleParams map[string]any,
) ([]search.Result, error) {
	p.record("get", in, moduleParams)
	return in, nil
}

func (p *recordingModulesProvider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]any, argumentModuleParams map[string]any,
) ([]search.Result, error) {
	p.record("list", in, moduleParams)
	return in, nil
}

func (p *recordingModulesProvider) record(path string, in []search.Result, moduleParams map[string]any) {
	// A call without properties runs no module.
	if len(moduleParams) == 0 {
		return
	}
	names := make([]string, 0, len(moduleParams))
	for name := range moduleParams {
		names = append(names, name)
	}
	slices.Sort(names)
	p.calls = append(p.calls, fmt.Sprintf("%s:%s:%d", path, strings.Join(names, "+"), len(in)))
}

func rerankParams() map[string]any {
	property, query := "name", "the item is good"
	return map[string]any{"rerank": &rank.Params{Property: &property, Query: &query}}
}

// page returns what a store would return for the pagination: the fakes
// otherwise ignore limit and offset.
func page(all []search.Result, pagination *filters.Pagination) []search.Result {
	start := min(pagination.Offset, len(all))
	if pagination.Limit < 0 {
		return all[start:]
	}
	end := min(start+pagination.Limit, len(all))
	return all[start:end]
}

// The additional properties of the modules run once, on the fused results
// of a hybrid search, whatever the vector leg is.
func TestExplorerHybridRunsModulesOnce(t *testing.T) {
	vector := []float32{0.1, 0.2, 0.3}
	searches := []struct {
		name   string
		hybrid searchparams.HybridSearch
	}{
		{name: "vector", hybrid: searchparams.HybridSearch{Query: "q", Alpha: 0.5, Vector: vector}},
		{name: "nearVector sub-search", hybrid: searchparams.HybridSearch{
			Query: "q", Alpha: 0.5,
			NearVectorParams: &searchparams.NearVector{Vectors: []models.Vector{vector}},
		}},
		{name: "nearText sub-search", hybrid: searchparams.HybridSearch{
			Query: "q", Alpha: 0.5,
			NearTextParams: &searchparams.NearTextParams{Values: []string{"q"}},
		}},
		{name: "nearText sub-search without a keyword leg", hybrid: searchparams.HybridSearch{
			Query: "q", Alpha: 1,
			NearTextParams: &searchparams.NearTextParams{Values: []string{"q"}},
		}},
	}
	moduleParams := []struct {
		name   string
		params func() map[string]any
	}{
		{name: "rerank", params: rerankParams},
		{name: "generate", params: func() map[string]any { return map[string]any{"generate": struct{}{}} }},
		{name: "generate+rerank", params: func() map[string]any {
			params := rerankParams()
			params["generate"] = struct{}{}
			return params
		}},
	}
	for _, s := range searches {
		for _, mp := range moduleParams {
			t.Run(s.name+"/"+mp.name, func(t *testing.T) {
				searcher := &fakeVectorSearcher{}
				searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
					return page(makeHybridVectorResults(20), params.Pagination), nil
				}
				searcher.sparseObjectSearchFn = func(params dto.GetParams) ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
				provider := &recordingModulesProvider{fakeModulesProvider: &fakeModulesProvider{}}
				explorer := newTestExplorer(searcher, provider)
				hybrid := s.hybrid
				params := dto.GetParams{
					ClassName:    "TestClass",
					Pagination:   &filters.Pagination{Limit: 5},
					HybridSearch: &hybrid,
				}
				params.AdditionalProperties.ModuleParams = mp.params()

				_, err := explorer.GetClass(context.Background(), params)

				require.NoError(t, err)
				assert.Equal(t, []string{"list:" + mp.name + ":5"}, provider.calls)
			})
		}
	}
}

// Modules that run on the fused list may need the object vectors, so the
// nearText leg has to load them although it runs no module itself.
func TestExplorerHybridNearTextLegLoadsVectorsForModules(t *testing.T) {
	searcher := &fakeVectorSearcher{}
	provider := &recordingModulesProvider{fakeModulesProvider: &fakeModulesProvider{}}
	explorer := newTestExplorer(searcher, provider)
	var legWantsVector bool
	searcher.vectorSearchFn = func(params dto.GetParams) ([]search.Result, error) {
		legWantsVector = params.AdditionalProperties.Vector
		return page(makeHybridVectorResults(20), params.Pagination), nil
	}
	params := dto.GetParams{
		ClassName:  "TestClass",
		Pagination: &filters.Pagination{Limit: 5},
		HybridSearch: &searchparams.HybridSearch{
			Query: "test", Alpha: 1,
			NearTextParams: &searchparams.NearTextParams{Values: []string{"test"}},
		},
	}
	params.AdditionalProperties.ModuleParams = rerankParams()

	_, err := explorer.GetClass(context.Background(), params)

	require.NoError(t, err)
	assert.True(t, legWantsVector)
}
