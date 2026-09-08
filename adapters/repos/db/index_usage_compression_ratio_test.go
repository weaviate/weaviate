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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestDimensionInfoCompressionRatio(t *testing.T) {
	tests := []struct {
		name string
		// quantizedVectorsExist defaults to false, so every case that expects a
		// compressed ratio has to say the quantizer already ran
		quantizedVectorsExist bool
		dimInfo               DimensionInfo
		dimensions            int
		wantRatio             float64
	}{
		{
			name:       "standard",
			dimInfo:    DimensionInfo{category: DimensionCategoryStandard},
			dimensions: 64,
			wantRatio:  1,
		},
		{
			name:       "auto quantizes like rq on 1 bit, without its own bucket",
			dimInfo:    DimensionInfo{category: DimensionCategoryAuto},
			dimensions: 64,
			wantRatio:  16,
		},
		{
			name:                  "bq stores one bit per dimension",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryBQ},
			dimensions:            64,
			wantRatio:             32,
		},
		{
			name:                  "sq",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategorySQ},
			dimensions:            64,
			wantRatio:             4,
		},
		{
			name:                  "pq with configured segments",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryPQ, segments: 16},
			dimensions:            64,
			wantRatio:             16, // 64 * 4 bytes / 16 codes
		},
		{
			name:                  "pq with unset segments uses the optimal count",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryPQ},
			dimensions:            64,
			wantRatio:             8, // optimal is 64/2 segments
		},
		{
			name:                  "pq with unset segments on a six-divisible dimension count",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryPQ},
			dimensions:            768,
			wantRatio:             24, // optimal is 768/6 segments
		},
		{
			name:                  "pq with unset segments on an odd dimension count",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryPQ},
			dimensions:            1,
			wantRatio:             4, // optimal is one segment per dimension
		},
		{
			name:                  "rq with 8 bits",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryRQ, bits: 8},
			dimensions:            64,
			wantRatio:             3.2, // 64 * 4 bytes / (16 metadata + 64 codes)
		},
		{
			name:                  "rq with unset bits reports the 8 bit ratio",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryRQ},
			dimensions:            64,
			wantRatio:             3.2,
		},
		{
			name:                  "rq with 1 bit",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryRQ, bits: 1},
			dimensions:            64,
			wantRatio:             16, // 64 * 4 bytes / (8 metadata + 8 packed bytes)
		},
		{
			name:       "pq that has not reached its training limit",
			dimInfo:    DimensionInfo{category: DimensionCategoryPQ, segments: 16},
			dimensions: 64,
			wantRatio:  1,
		},
		{
			name:       "bq on a shard that never built its index",
			dimInfo:    DimensionInfo{category: DimensionCategoryBQ},
			dimensions: 64,
			wantRatio:  1,
		},
		{
			name:                  "pq without tracked dimensions",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryPQ},
			dimensions:            0,
			wantRatio:             1,
		},
		{
			name:                  "rq without tracked dimensions",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryRQ, bits: 8},
			dimensions:            0,
			wantRatio:             1,
		},
		{
			name:                  "bq without tracked dimensions",
			quantizedVectorsExist: true,
			dimInfo:               DimensionInfo{category: DimensionCategoryBQ},
			dimensions:            0,
			wantRatio:             1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDelta(t, tt.wantRatio,
				tt.dimInfo.compressionRatio(tt.dimensions, tt.quantizedVectorsExist), 1e-9)
		})
	}
}

func TestUnloadedVectorUsage(t *testing.T) {
	dimensionality := types.Dimensionality{Dimensions: 64, Count: 10}

	tests := []struct {
		name            string
		vectorConfig    models.VectorConfig
		state           unloadedVectorState
		wantRatio       float64
		wantCompression string
		wantIndexType   string
		wantBits        int16
		wantDynamic     bool
		wantErr         bool
	}{
		{
			name: "hnsw without compression",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{},
			},
			wantRatio:       1,
			wantCompression: "standard",
			wantIndexType:   common.IndexTypeHNSW,
		},
		{
			name: "hnsw with bq",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{BQ: enthnsw.BQConfig{Enabled: true}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       32,
			wantCompression: "bq",
			wantIndexType:   common.IndexTypeHNSW,
		},
		{
			name: "hnsw with sq",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{SQ: enthnsw.SQConfig{Enabled: true}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       4,
			wantCompression: "sq",
			wantIndexType:   common.IndexTypeHNSW,
		},
		{
			name: "hnsw with pq on unset segments",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{PQ: enthnsw.PQConfig{Enabled: true}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       8,
			wantCompression: "pq",
			wantIndexType:   common.IndexTypeHNSW,
		},
		{
			name: "hnsw with rq on 1 bit",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{RQ: enthnsw.RQConfig{Enabled: true, Bits: 1}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       16,
			wantCompression: "rq",
			wantIndexType:   common.IndexTypeHNSW,
			wantBits:        1,
		},
		{
			name: "flat with bq",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeFlat,
				VectorIndexConfig: flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       32,
			wantCompression: "bq",
			wantIndexType:   common.IndexTypeFlat,
		},
		{
			name: "flat with rq on 8 bits",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeFlat,
				VectorIndexConfig: flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true, Bits: 8}},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       3.2,
			wantCompression: "rq",
			wantIndexType:   common.IndexTypeFlat,
			wantBits:        8,
		},
		{
			name: "hfresh",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHFresh,
				VectorIndexConfig: hfreshent.NewDefaultUserConfig(),
			},
			wantRatio:       16, // hfresh always quantizes with rq on 1 bit
			wantCompression: "auto",
			wantIndexType:   common.IndexTypeHFresh,
		},
		{
			name: "hnsw with pq that has not reached its training limit",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: enthnsw.UserConfig{PQ: enthnsw.PQConfig{Enabled: true}},
			},
			wantRatio:       1,
			wantCompression: "pq",
			wantIndexType:   common.IndexTypeHNSW,
		},
		{
			name: "dynamic still on flat",
			vectorConfig: models.VectorConfig{
				VectorIndexType: common.IndexTypeDynamic,
				VectorIndexConfig: dynamicent.UserConfig{
					HnswUC: enthnsw.UserConfig{PQ: enthnsw.PQConfig{Enabled: true}},
					FlatUC: flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}},
				},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true},
			wantRatio:       32,
			wantCompression: "bq",
			wantIndexType:   common.IndexTypeFlat,
			wantDynamic:     true,
		},
		{
			name: "dynamic upgraded to hnsw",
			vectorConfig: models.VectorConfig{
				VectorIndexType: common.IndexTypeDynamic,
				VectorIndexConfig: dynamicent.UserConfig{
					HnswUC: enthnsw.UserConfig{PQ: enthnsw.PQConfig{Enabled: true}},
					FlatUC: flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}},
				},
			},
			state:           unloadedVectorState{quantizedVectorsExist: true, dynamicUpgraded: true},
			wantRatio:       8, // pq on the optimal 64/2 segments
			wantCompression: "pq",
			wantIndexType:   common.IndexTypeHNSW,
			wantDynamic:     true,
		},
		{
			name: "unparsed config",
			vectorConfig: models.VectorConfig{
				VectorIndexType:   common.IndexTypeHNSW,
				VectorIndexConfig: map[string]any{"bq": map[string]any{"enabled": true}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usage, err := unloadedVectorUsage("vec", tt.vectorConfig, dimensionality, tt.state)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, "vec", usage.Name)
			assert.InDelta(t, tt.wantRatio, usage.VectorCompressionRatio, 1e-9)
			assert.Equal(t, tt.wantCompression, usage.Compression)
			assert.Equal(t, tt.wantIndexType, usage.VectorIndexType)
			assert.Equal(t, tt.wantBits, usage.Bits)
			assert.Equal(t, tt.wantDynamic, usage.IsDynamic)
			require.Len(t, usage.Dimensionalities, 1)
			assert.Equal(t, dimensionality, *usage.Dimensionalities[0])
		})
	}
}
