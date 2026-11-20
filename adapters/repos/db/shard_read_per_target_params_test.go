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
