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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// tokenCapturingSearcher records the dedupe token each hybrid leg receives.
type tokenCapturingSearcher struct {
	fakeVectorSearcher

	mu         sync.Mutex
	sparseToks []string
	vectorToks []string
}

func (s *tokenCapturingSearcher) SparseObjectSearch(ctx context.Context,
	params dto.GetParams,
) ([]*storobj.Object, []float32, error) {
	s.mu.Lock()
	s.sparseToks = append(s.sparseToks, helpers.QueryDedupeToken(ctx))
	s.mu.Unlock()
	return nil, nil, nil
}

func (s *tokenCapturingSearcher) VectorSearch(ctx context.Context, params dto.GetParams,
	targetVectors []string, searchVectors []models.Vector,
) ([]search.Result, error) {
	s.mu.Lock()
	s.vectorToks = append(s.vectorToks, helpers.QueryDedupeToken(ctx))
	s.mu.Unlock()
	return nil, nil
}

func (s *tokenCapturingSearcher) ResolveReferences(ctx context.Context, objs search.Results,
	props search.SelectProperties, groupBy *searchparams.GroupBy,
	addl additional.Properties, tenant string,
) (search.Results, error) {
	return objs, nil
}

func (s *tokenCapturingSearcher) Object(ctx context.Context, className string, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	repl *additional.ReplicationProperties, tenant string,
) (*search.Result, error) {
	return nil, nil
}

func hybridDedupeFilter() *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorGreaterThan,
		On:       &filters.Path{Class: "MyClass", Property: "score"},
		Value:    &filters.Value{Value: 100, Type: schema.DataTypeInt},
	}}
}

func TestHybridMintsOneDedupeTokenPerQuery(t *testing.T) {
	tests := []struct {
		name      string
		alpha     float32
		filter    *filters.LocalFilter
		disabled  bool
		wantToken bool
	}{
		{
			name: "both legs run with a filter", alpha: 0.5,
			filter: hybridDedupeFilter(), wantToken: true,
		},
		{
			name: "no filter means nothing to share", alpha: 0.5,
			filter: nil, wantToken: false,
		},
		{
			name: "alpha 1 runs only the dense leg", alpha: 1,
			filter: hybridDedupeFilter(), wantToken: false,
		},
		{
			name: "alpha 0 runs only the sparse leg", alpha: 0,
			filter: hybridDedupeFilter(), wantToken: false,
		},
		{
			name: "kill switch opts out", alpha: 0.5,
			filter: hybridDedupeFilter(), disabled: true, wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher := &tokenCapturingSearcher{}
			log, _ := test.NewNullLogger()

			conf := defaultConfig
			conf.HybridFilterDedupeDisabled = runtime.NewDynamicValue(tt.disabled)

			explorer := NewExplorer(searcher, log, getFakeModulesProvider(), nil, conf)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "MyClass", Vectorizer: config.VectorizerModuleNone},
				}}},
			})

			_, err := explorer.Hybrid(context.Background(), dto.GetParams{
				ClassName: "MyClass",
				HybridSearch: &searchparams.HybridSearch{
					Query:  "some query",
					Vector: []float32{1, 2, 3},
					Alpha:  float64(tt.alpha),
				},
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    tt.filter,
			})
			require.NoError(t, err)

			toks := append(append([]string{}, searcher.sparseToks...), searcher.vectorToks...)
			require.NotEmpty(t, toks, "at least one leg must have run")

			if !tt.wantToken {
				for _, tok := range toks {
					assert.Empty(t, tok, "no token expected for this query shape")
				}
				return
			}

			require.Len(t, searcher.sparseToks, 1)
			require.Len(t, searcher.vectorToks, 1)
			assert.NotEmpty(t, searcher.sparseToks[0])
			assert.Equal(t, searcher.sparseToks[0], searcher.vectorToks[0],
				"both legs must carry the same token or the shard cannot dedupe")
			assert.LessOrEqual(t, len(searcher.sparseToks[0]), helpers.MaxQueryDedupeTokenLen)
		})
	}
}

// TestHybridDedupeKillSwitchIsObservable pins that an operator can confirm the
// kill switch engaged. Both states carry their own series, and both exist before
// either is ever incremented, so "off" never reads the same as "no traffic".
func TestHybridDedupeKillSwitchIsObservable(t *testing.T) {
	const metric = "weaviate_hybrid_filter_dedupe_tokens_total"

	before := gatherCounter(t, metric, "state")
	require.Contains(t, before, helpers.QueryDedupeTokenMinted)
	require.Contains(t, before, helpers.QueryDedupeTokenDisabled)

	run := func(t *testing.T, disabled bool, filter *filters.LocalFilter, alpha float64) {
		t.Helper()
		log, _ := test.NewNullLogger()
		conf := defaultConfig
		conf.HybridFilterDedupeDisabled = runtime.NewDynamicValue(disabled)

		explorer := NewExplorer(&tokenCapturingSearcher{}, log, getFakeModulesProvider(), nil, conf)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "MyClass", Vectorizer: config.VectorizerModuleNone},
			}}},
		})

		_, err := explorer.Hybrid(context.Background(), dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query: "some query", Vector: []float32{1, 2, 3}, Alpha: alpha,
			},
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    filter,
		})
		require.NoError(t, err)
	}

	run(t, false, hybridDedupeFilter(), 0.5)
	run(t, true, hybridDedupeFilter(), 0.5)
	run(t, true, hybridDedupeFilter(), 0.5)
	// Neither state may move for a query that was never a dedupe candidate.
	run(t, false, nil, 0.5)
	run(t, false, hybridDedupeFilter(), 1)

	after := gatherCounter(t, metric, "state")
	assert.EqualValues(t, 1, after[helpers.QueryDedupeTokenMinted]-before[helpers.QueryDedupeTokenMinted])
	assert.EqualValues(t, 2, after[helpers.QueryDedupeTokenDisabled]-before[helpers.QueryDedupeTokenDisabled])
}

// gatherCounter reads one counter vector out of the default registry as a
// label-value to value map, so tests assert on deltas without a production-only
// accessor.
func gatherCounter(t *testing.T, name, label string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label {
					out[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	require.NotEmpty(t, out, "metric %q is not registered", name)
	return out
}

func TestHybridTokensAreUniquePerQuery(t *testing.T) {
	searcher := &tokenCapturingSearcher{}
	log, _ := test.NewNullLogger()

	conf := defaultConfig
	conf.HybridFilterDedupeDisabled = runtime.NewDynamicValue(false)

	explorer := NewExplorer(searcher, log, getFakeModulesProvider(), nil, conf)
	explorer.SetSchemaGetter(&fakeSchemaGetter{
		schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
			{Class: "MyClass", Vectorizer: config.VectorizerModuleNone},
		}}},
	})

	const queries = 25
	seen := map[string]struct{}{}
	for i := 0; i < queries; i++ {
		_, err := explorer.Hybrid(context.Background(), dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "some query",
				Vector: []float32{1, 2, 3},
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    hybridDedupeFilter(),
		})
		require.NoError(t, err)
	}

	require.Len(t, searcher.sparseToks, queries)
	for _, tok := range searcher.sparseToks {
		_, dup := seen[tok]
		require.False(t, dup, "token %q reused across queries", tok)
		seen[tok] = struct{}{}
	}
}
