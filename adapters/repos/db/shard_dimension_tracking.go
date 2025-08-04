//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/cluster/usage/types"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type DimensionCategory int

const (
	DimensionCategoryStandard DimensionCategory = iota
	DimensionCategoryPQ
	DimensionCategoryBQ
	DimensionCategorySQ
	DimensionCategoryRQ
)

func (c DimensionCategory) String() string {
	switch c {
	case DimensionCategoryPQ:
		return "pq"
	case DimensionCategoryBQ:
		return "bq"
	case DimensionCategorySQ:
		return "sq"
	case DimensionCategoryRQ:
		return "rq"
	default:
		return "standard"
	}
}

// DimensionsUsage returns the total number of dimensions and the number of objects for a given vector
func (s *Shard) DimensionsUsage(ctx context.Context, targetVector string) (types.Dimensionality, error) {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return len(v), dimLength
	})
	if err != nil {
		return types.Dimensionality{}, err
	}
	return dimensionality, nil
}

// Dimensions returns the total number of dimensions for a given vector
func (s *Shard) Dimensions(ctx context.Context, targetVector string) (int, error) {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return dimLength * len(v), dimLength
	})
	if err != nil {
		return 0, err
	}
	return dimensionality.Count, nil
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) int {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return len(v), dimLength
	})
	if err != nil {
		return 0
	}

	return dimensionality.Count * correctEmptySegments(segments, dimensionality.Dimensions)
}

func (s *Shard) calcTargetVectorDimensions(ctx context.Context, targetVector string, calcEntry func(dimLen int, v []lsmkv.MapPair) (int, int)) (types.Dimensionality, error) {
	return calcTargetVectorDimensionsFromStore(ctx, s.store, targetVector, calcEntry), nil
}

// DimensionMetrics represents the dimension tracking metrics for a vector.
// The metrics are used to track memory usage and performance characteristics
// of different vector compression methods.
//
// Usage patterns:
// - Standard vectors: Only Uncompressed is set (4 bytes per dimension)
// - PQ (Product Quantization): Only Compressed is set (1 byte per segment)
// - BQ (Binary Quantization): Only Compressed is set (1 bit per dimension, packed in uint64 blocks)
//
// The metrics are aggregated across all vectors in a shard and published
// to Prometheus for monitoring and capacity planning.
type DimensionMetrics struct {
	Uncompressed int // Uncompressed dimensions count (for standard vectors)
	Compressed   int // Compressed dimensions count (for PQ/BQ vectors)
}

// Add creates a new DimensionMetrics instance with total values summed pairwise.
func (dm DimensionMetrics) Add(add DimensionMetrics) DimensionMetrics {
	return DimensionMetrics{
		Uncompressed: dm.Uncompressed + add.Uncompressed,
		Compressed:   dm.Compressed + add.Compressed,
	}
}

// Vector dimension metrics are collected on the node level and
// are normally _polled_ from each shard. Shard SHOULD only update
// its dimension metrics on its own iff it is being shut down or dropped
// and metrics grouping is disatbled.
func (s *Shard) clearDimensionMetrics() {
	if s.index.metrics.baseMetrics == nil || s.index.metrics.baseMetrics.Group {
		return
	}

	className, shardName := s.index.Config.ClassName.String(), s.name
	if g, err := s.index.metrics.baseMetrics.VectorDimensionsSum.
		GetMetricWithLabelValues(className, shardName); err == nil {
		g.Set(0)
	}
	if g, err := s.index.metrics.baseMetrics.VectorSegmentsSum.
		GetMetricWithLabelValues(className, shardName); err == nil {
		g.Set(0)
	}
}

func GetDimensionCategory(cfg schemaConfig.VectorIndexConfig) (DimensionCategory, int) {
	// We have special dimension tracking for BQ and PQ to represent reduced costs
	// these are published under the separate vector_segments_dimensions metric
	if hnswUserConfig, ok := cfg.(hnswent.UserConfig); ok {
		if hnswUserConfig.PQ.Enabled {
			return DimensionCategoryPQ, hnswUserConfig.PQ.Segments
		}
		if hnswUserConfig.BQ.Enabled {
			return DimensionCategoryBQ, 0
		}
		if hnswUserConfig.SQ.Enabled {
			return DimensionCategorySQ, 0
		}
		if hnswUserConfig.RQ.Enabled {
			return DimensionCategoryRQ, 0
		}
	}
	return DimensionCategoryStandard, 0
}

// GetRQBits returns the bits value for RQ compression, or 0 if not RQ
func GetRQBits(cfg schemaConfig.VectorIndexConfig) int16 {
	if hnswUserConfig, ok := cfg.(hnswent.UserConfig); ok && hnswUserConfig.RQ.Enabled {
		return hnswUserConfig.RQ.Bits
	}
	return 0
}

func correctEmptySegments(segments int, dimensions int) int {
	// If segments is 0 (unset), in this case PQ will calculate the number of segments
	// based on the number of dimensions
	if segments > 0 {
		return segments
	}
	return common.CalculateOptimalSegments(dimensions)
}
