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

	"github.com/weaviate/weaviate/adapters/repos/db/shardmemory"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/cluster/usage/types"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShardMemoryVector(t *testing.T) {
	standard := DimensionCategoryStandard.String()
	hnswConfig := enthnsw.UserConfig{MaxConnections: 48, VectorCacheMaxObjects: 111}
	withHNSW := func(mutate func(*enthnsw.UserConfig)) enthnsw.UserConfig {
		c := hnswConfig
		mutate(&c)
		return c
	}
	skipHNSWConfig := withHNSW(func(c *enthnsw.UserConfig) { c.Skip = true })
	flatConfig := func(mutate func(*flatent.UserConfig)) flatent.UserConfig {
		c := flatent.UserConfig{VectorCacheMaxObjects: 222}
		mutate(&c)
		return c
	}
	dynamicConfig := dynamicent.UserConfig{
		HnswUC: withHNSW(func(c *enthnsw.UserConfig) { c.Skip = true; c.BQ.Enabled = true }),
		FlatUC: flatConfig(func(c *flatent.UserConfig) { c.BQ.Enabled = true; c.BQ.Cache = true }),
	}
	// vectorUsages and unloadedVectorUsage always set MultiVectorConfig, so row
	// does too.
	row := func(indexType, compression string, ratio float64, dims ...*types.Dimensionality) *types.VectorUsage {
		return &types.VectorUsage{
			Name:                   "vec",
			VectorIndexType:        indexType,
			Compression:            compression,
			VectorCompressionRatio: ratio,
			MultiVectorConfig:      &types.MultiVectorConfig{},
			Dimensionalities:       dims,
		}
	}
	with := func(r *types.VectorUsage, mutate func(*types.VectorUsage)) *types.VectorUsage {
		mutate(r)
		return r
	}
	dynamicRow := func(r *types.VectorUsage) *types.VectorUsage {
		return with(r, func(r *types.VectorUsage) { r.IsDynamic = true })
	}
	rq8Row := func(indexType string, ratio float64, dims *types.Dimensionality) *types.VectorUsage {
		return with(row(indexType, DimensionCategoryRQ.String(), ratio, dims), func(r *types.VectorUsage) {
			r.Bits = 8
		})
	}
	dims := &types.Dimensionality{Dimensions: 768, Count: 1000}
	rq8Ratio := compressionhelpers.RQStats{Bits: 8}.CompressionRatio(768)
	rq8Ratio4Dims := compressionhelpers.RQStats{Bits: 8}.CompressionRatio(4)
	pq128Ratio := compressionhelpers.PQStats{M: 128}.CompressionRatio(768)

	tests := []struct {
		name       string
		row        *types.VectorUsage
		userConfig schemaConfig.VectorIndexConfig
		wantVector shardmemory.VectorIndex
		wantState  vectorLoadState
	}{
		{
			name:       "plain hnsw takes its settings from the hnsw config",
			row:        row("hnsw", standard, 1, dims),
			userConfig: hnswConfig,
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: standard},
		},
		{
			name:       "a ratio above 1 marks hnsw compressed",
			row:        row("hnsw", DimensionCategoryBQ.String(), 32, dims),
			userConfig: withHNSW(func(c *enthnsw.UserConfig) { c.BQ.Enabled = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 32, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "bq", compressed: true},
		},
		{
			name:       "a ratio below 1 still marks hnsw compressed",
			row:        rq8Row("hnsw", rq8Ratio4Dims, &types.Dimensionality{Dimensions: 4, Count: 1000}),
			userConfig: withHNSW(func(c *enthnsw.UserConfig) { c.RQ.Enabled = true; c.RQ.Bits = 8 }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: rq8Ratio4Dims, Compressed: true, DimensionsTracked: true,
				Dimensions: 4, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "rq", compressed: true},
		},
		// An untrained PQ index reports a compression ratio of 1.
		{
			name:       "a pq compression category sets PQ",
			row:        row("hnsw", DimensionCategoryPQ.String(), 1, dims),
			userConfig: withHNSW(func(c *enthnsw.UserConfig) { c.PQ.Enabled = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, PQ: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "pq"},
		},
		{
			name:       "a trained pq index is PQ and compressed",
			row:        row("hnsw", DimensionCategoryPQ.String(), pq128Ratio, dims),
			userConfig: withHNSW(func(c *enthnsw.UserConfig) { c.PQ.Enabled = true; c.PQ.Segments = 128 }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: pq128Ratio, Compressed: true, PQ: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "pq", compressed: true},
		},
		{
			name: "an enabled multivector config sets Multivector",
			row: with(row("hnsw", standard, 1, dims), func(r *types.VectorUsage) {
				r.MultiVectorConfig = &types.MultiVectorConfig{Enabled: true, MuveraConfig: &types.MuveraConfig{}}
			}),
			userConfig: withHNSW(func(c *enthnsw.UserConfig) { c.Multivector.Enabled = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, Multivector: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: standard},
		},
		{
			name:       "hnsw Skip sets skip in the load state",
			row:        row("hnsw", standard, 1, dims),
			userConfig: skipHNSWConfig,
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: standard, skip: true},
		},
		{
			name:       "a row with no dimensionalities tracks no dimensions",
			row:        row("hnsw", standard, 1),
			userConfig: hnswConfig,
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: standard},
		},
		{
			name:       "flat with BQ and its cache on",
			row:        row("flat", DimensionCategoryBQ.String(), 32, dims),
			userConfig: flatConfig(func(c *flatent.UserConfig) { c.BQ.Enabled = true; c.BQ.Cache = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: 32, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222, FlatQuantizerCache: true,
			},
			wantState: vectorLoadState{indexType: "flat", compression: "bq", compressed: true},
		},
		{
			name:       "flat with BQ and its cache off",
			row:        row("flat", DimensionCategoryBQ.String(), 32, dims),
			userConfig: flatConfig(func(c *flatent.UserConfig) { c.BQ.Enabled = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: 32, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222,
			},
			wantState: vectorLoadState{indexType: "flat", compression: "bq", compressed: true},
		},
		{
			name:       "flat with RQ and its cache on",
			row:        rq8Row("flat", rq8Ratio, dims),
			userConfig: flatConfig(func(c *flatent.UserConfig) { c.RQ.Enabled = true; c.RQ.Bits = 8; c.RQ.Cache = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: rq8Ratio, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222, FlatQuantizerCache: true,
			},
			wantState: vectorLoadState{indexType: "flat", compression: "rq", compressed: true},
		},
		{
			name:       "flat with RQ and its cache off",
			row:        rq8Row("flat", rq8Ratio, dims),
			userConfig: flatConfig(func(c *flatent.UserConfig) { c.RQ.Enabled = true; c.RQ.Bits = 8 }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: rq8Ratio, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222,
			},
			wantState: vectorLoadState{indexType: "flat", compression: "rq", compressed: true},
		},
		{
			name:       "flat with no quantizer has no quantizer cache",
			row:        row("flat", standard, 1, dims),
			userConfig: flatConfig(func(c *flatent.UserConfig) { c.BQ.Cache = true; c.RQ.Cache = true }),
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: 1, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222,
			},
			wantState: vectorLoadState{indexType: "flat", compression: standard},
		},
		{
			name:       "dynamic running as flat uses the flat sub-config and ignores hnsw Skip",
			row:        dynamicRow(row("flat", DimensionCategoryBQ.String(), 32, dims)),
			userConfig: dynamicConfig,
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: 32, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222, FlatQuantizerCache: true,
			},
			wantState: vectorLoadState{indexType: "flat", compression: "bq", compressed: true},
		},
		{
			name:       "dynamic running as hnsw uses the hnsw sub-config and ignores hnsw Skip",
			row:        dynamicRow(row("hnsw", DimensionCategoryBQ.String(), 32, dims)),
			userConfig: dynamicConfig,
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 32, Compressed: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "bq", compressed: true},
		},
		{
			name: "dynamic running as hnsw takes pq from the row over flat's bq",
			row:  dynamicRow(row("hnsw", DimensionCategoryPQ.String(), 1, dims)),
			userConfig: dynamicent.UserConfig{
				HnswUC: withHNSW(func(c *enthnsw.UserConfig) { c.PQ.Enabled = true }),
				FlatUC: flatConfig(func(c *flatent.UserConfig) { c.BQ.Enabled = true }),
			},
			wantVector: shardmemory.VectorIndex{
				IndexType: "hnsw", CompressionRatio: 1, PQ: true, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, MaxConnections: 48, CacheLimit: 111,
			},
			wantState: vectorLoadState{indexType: "hnsw", compression: "pq"},
		},
		{
			name: "dynamic running as flat takes standard from the row over hnsw's bq",
			row:  dynamicRow(row("flat", standard, 1, dims)),
			userConfig: dynamicent.UserConfig{
				HnswUC: withHNSW(func(c *enthnsw.UserConfig) { c.BQ.Enabled = true }),
				FlatUC: flatConfig(func(c *flatent.UserConfig) {}),
			},
			wantVector: shardmemory.VectorIndex{
				IndexType: "flat", CompressionRatio: 1, DimensionsTracked: true,
				Dimensions: 768, VectorCount: 1000, CacheLimit: 222,
			},
			wantState: vectorLoadState{indexType: "flat", compression: standard},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vector, state := shardMemoryVector(tt.row, tt.userConfig)
			assert.Equal(t, tt.wantVector, vector)
			assert.Equal(t, tt.wantState, state)
		})
	}
}
