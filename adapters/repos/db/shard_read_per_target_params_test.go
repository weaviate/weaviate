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

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestObjectVectorSearch_PerTargetParams(t *testing.T) {
	tests := []struct {
		name   string
		params *searchparams.VectorSearchParams
		wantA  *searchparams.VectorSearchParams
		wantB  *searchparams.VectorSearchParams
	}{
		{
			name:   "nil params",
			params: nil,
			wantA:  nil,
			wantB:  nil,
		},
		{
			name:   "no per-target entries pass through",
			params: &searchparams.VectorSearchParams{EF: 64},
			wantA:  &searchparams.VectorSearchParams{EF: 64},
			wantB:  &searchparams.VectorSearchParams{EF: 64},
		},
		{
			name: "entry overrides only its target",
			params: &searchparams.VectorSearchParams{
				EF:        64,
				PerTarget: map[string]searchparams.VectorSearchParams{"a": {EF: 128}},
			},
			wantA: &searchparams.VectorSearchParams{EF: 128},
			wantB: &searchparams.VectorSearchParams{EF: 64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			shardLike, _ := testShardWithSettings(t, ctx, &models.Class{Class: "test"}, enthnsw.UserConfig{}, false, false, func(idx *Index) {
				idx.vectorIndexUserConfig = nil
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"a": enthnsw.NewDefaultUserConfig(),
					"b": enthnsw.NewDefaultUserConfig(),
				}
			})
			shard := underlyingShard(t, shardLike)

			mocks := map[string]*MockVectorIndex{}
			for _, name := range []string{"a", "b"} {
				real, release, ok := shard.AcquireVectorIndex(name)
				require.True(t, ok)
				release()
				t.Cleanup(func() { real.Shutdown(ctx) })

				m := NewMockVectorIndex(t)
				m.On("Type").Return(common.IndexType(common.IndexTypeHNSW)).Maybe()
				m.On("Multivector").Return(false).Maybe()
				m.On("SearchByVector", mock.Anything, mock.Anything, 10, mock.Anything, mock.Anything).
					Return([]uint64{}, []float32{}, nil).Once()
				m.On("Shutdown", mock.Anything).Return(nil).Maybe()
				require.True(t, shard.vectors.Replace(name, m))
				mocks[name] = m
			}

			_, _, err := shard.ObjectVectorSearch(ctx,
				[]models.Vector{[]float32{1, 2}, []float32{3, 4}}, []string{"a", "b"}, 0, 10,
				nil, nil, nil, additional.Properties{}, nil, nil, tt.params)
			require.NoError(t, err)

			for name, want := range map[string]*searchparams.VectorSearchParams{"a": tt.wantA, "b": tt.wantB} {
				calls := mocks[name].Calls
				var got *searchparams.VectorSearchParams
				for _, c := range calls {
					if c.Method == "SearchByVector" {
						got, _ = c.Arguments.Get(4).(*searchparams.VectorSearchParams)
					}
				}
				assert.Equal(t, want, got, "params seen by index %q", name)
			}
		})
	}
}

// multiVectorMockIndex adds the multi-vector search methods to MockVectorIndex
// (which does not implement VectorIndexMulti) and records the params it gets.
type multiVectorMockIndex struct {
	*MockVectorIndex
	multiParams []*searchparams.VectorSearchParams
}

func (m *multiVectorMockIndex) SearchByMultiVector(_ context.Context, _ [][]float32, _ int,
	_ helpers.AllowList, params *searchparams.VectorSearchParams,
) ([]uint64, []float32, error) {
	m.multiParams = append(m.multiParams, params)
	return []uint64{}, []float32{}, nil
}

func (m *multiVectorMockIndex) SearchByMultiVectorDistance(_ context.Context, _ [][]float32, _ float32,
	_ int64, _ helpers.AllowList, params *searchparams.VectorSearchParams,
) ([]uint64, []float32, error) {
	m.multiParams = append(m.multiParams, params)
	return []uint64{}, []float32{}, nil
}

func (m *multiVectorMockIndex) AddMulti(context.Context, uint64, [][]float32) error { return nil }

func (m *multiVectorMockIndex) AddMultiBatch(context.Context, []uint64, [][][]float32) error {
	return nil
}

func (m *multiVectorMockIndex) DeleteMulti(...uint64) error { return nil }

func (m *multiVectorMockIndex) QueryMultiVectorDistancer([][]float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (m *multiVectorMockIndex) ValidateMultiBeforeInsert([][]float32) error { return nil }

// callParams returns the params argument of every call to method on m.
func callParams(m *MockVectorIndex, method string, idx int) []*searchparams.VectorSearchParams {
	var out []*searchparams.VectorSearchParams
	for _, c := range m.Calls {
		if c.Method == method {
			p, _ := c.Arguments.Get(idx).(*searchparams.VectorSearchParams)
			out = append(out, p)
		}
	}
	return out
}

func TestObjectVectorSearch_PerTargetParams_Methods(t *testing.T) {
	merged := &searchparams.VectorSearchParams{
		EF:        64,
		PerTarget: map[string]searchparams.VectorSearchParams{"a": {EF: 128}},
	}
	wantA := &searchparams.VectorSearchParams{EF: 128}
	wantB := &searchparams.VectorSearchParams{EF: 64}

	tests := []struct {
		name    string
		targets []string
		vectors []models.Vector
		limit   int
		dist    float32
		multi   bool
		method  string
		want    map[string][]*searchparams.VectorSearchParams // per target name
		// wantSame asserts every recorded call received the same pointer
		wantSame bool
		params   *searchparams.VectorSearchParams
	}{
		{
			name:    "distance search (limit < 0) passes merged params to SearchByVectorDistance",
			targets: []string{"a", "b"},
			vectors: []models.Vector{[]float32{1, 2}, []float32{3, 4}},
			limit:   -1, dist: 0.5,
			method: "SearchByVectorDistance",
			params: merged,
			want: map[string][]*searchparams.VectorSearchParams{
				"a": {wantA}, "b": {wantB},
			},
		},
		{
			name:    "multi-vector index gets merged params in SearchByMultiVector",
			targets: []string{"a", "b"},
			vectors: []models.Vector{[][]float32{{1, 2}, {3, 4}}, [][]float32{{5, 6}}},
			limit:   10, multi: true,
			method: "SearchByMultiVector",
			params: merged,
			want: map[string][]*searchparams.VectorSearchParams{
				"a": {wantA}, "b": {wantB},
			},
		},
		{
			name:    "multi-vector distance search gets merged params in SearchByMultiVectorDistance",
			targets: []string{"a", "b"},
			vectors: []models.Vector{[][]float32{{1, 2}, {3, 4}}, [][]float32{{5, 6}}},
			limit:   -1, dist: 0.5, multi: true,
			method: "SearchByMultiVectorDistance",
			params: merged,
			want: map[string][]*searchparams.VectorSearchParams{
				"a": {wantA}, "b": {wantB},
			},
		},
		{
			name:    "repeated target names receive identical merged params",
			targets: []string{"a", "a"},
			vectors: []models.Vector{[]float32{1, 2}, []float32{3, 4}},
			limit:   10,
			method:  "SearchByVector",
			params:  merged,
			want: map[string][]*searchparams.VectorSearchParams{
				"a": {wantA, wantA},
			},
		},
		{
			name:    "no per-target entries: every target shares the very same pointer",
			targets: []string{"a", "b", "c"},
			vectors: []models.Vector{[]float32{1, 2}, []float32{3, 4}, []float32{5, 6}},
			limit:   10,
			method:  "SearchByVector",
			params:  &searchparams.VectorSearchParams{EF: 64},
			want: map[string][]*searchparams.VectorSearchParams{
				"a": {{EF: 64}}, "b": {{EF: 64}}, "c": {{EF: 64}},
			},
			wantSame: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			shardLike, _ := testShardWithSettings(t, ctx, &models.Class{Class: "test"}, enthnsw.UserConfig{}, false, false, func(idx *Index) {
				idx.vectorIndexUserConfig = nil
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{}
				for _, n := range []string{"a", "b", "c"} {
					idx.vectorIndexUserConfigs[n] = enthnsw.NewDefaultUserConfig()
				}
			})
			shard := underlyingShard(t, shardLike)

			mocks := map[string]*MockVectorIndex{}
			multis := map[string]*multiVectorMockIndex{}
			for _, name := range []string{"a", "b", "c"} {
				real, release, ok := shard.AcquireVectorIndex(name)
				require.True(t, ok)
				release()
				t.Cleanup(func() { real.Shutdown(ctx) })

				m := NewMockVectorIndex(t)
				m.On("Type").Return(common.IndexType(common.IndexTypeHNSW)).Maybe()
				m.On("Multivector").Return(tt.multi).Maybe()
				m.On("Shutdown", mock.Anything).Return(nil).Maybe()
				m.On("SearchByVector", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return([]uint64{}, []float32{}, nil).Maybe()
				m.On("SearchByVectorDistance", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return([]uint64{}, []float32{}, nil).Maybe()
				mocks[name] = m
				if tt.multi {
					mm := &multiVectorMockIndex{MockVectorIndex: m}
					multis[name] = mm
					require.True(t, shard.vectors.Replace(name, mm))
				} else {
					require.True(t, shard.vectors.Replace(name, m))
				}
			}

			_, _, err := shard.ObjectVectorSearch(ctx, tt.vectors, tt.targets, tt.dist, tt.limit,
				nil, nil, nil, additional.Properties{}, nil, nil, tt.params)
			require.NoError(t, err)

			var all []*searchparams.VectorSearchParams
			for name, want := range tt.want {
				var got []*searchparams.VectorSearchParams
				if tt.multi {
					got = multis[name].multiParams
				} else {
					got = callParams(mocks[name], tt.method, map[string]int{
						"SearchByVector": 4, "SearchByVectorDistance": 5,
					}[tt.method])
				}
				assert.Equal(t, want, got, "params seen by index %q via %s", name, tt.method)
				all = append(all, got...)
			}
			if tt.wantSame {
				require.Len(t, all, len(tt.want))
				for _, p := range all {
					assert.Same(t, tt.params, p, "request-wide params are shared, not copied")
				}
			}
			if tt.multi {
				// the single-vector entry points must not have been used
				for name, m := range mocks {
					assert.Empty(t, callParams(m, "SearchByVector", 4), "index %q", name)
				}
			}
		})
	}
}
