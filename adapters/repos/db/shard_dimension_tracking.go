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
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	var dimensionality types.Dimensionality
	var err error
	if dimensionTrackingVersion == "v2" {
	dimensionality, err = s.calcTargetVectorDimensions_v2(ctx, targetVector, func(dimLength int, v int) (int, int) {
		return v, dimLength
	})
} else {
	dimensionality, err = s.calcTargetVectorDimensions_v1(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return v[0].Value, dimLength
	})
}
	if err != nil {
		return types.Dimensionality{}, err
	}
	return dimensionality, nil
}

// Dimensions returns the total number of dimensions for a given vector
func (s *Shard) Dimensions(ctx context.Context, targetVector string) (int, error) {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v int) (int, int) {
		return dimLength * v, dimLength
	})
	if err != nil {
		return 0, err
	}
	return dimensionality.Count, nil
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) int {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v int) (int, int) {
		return v, dimLength
	})
	if err != nil {
		return 0
	}

	return dimensionality.Count * correctEmptySegments(segments, dimensionality.Dimensions)
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

// Set shard's vector_dimensions_sum and vector_segments_sum metrics to 0.
func (s *Shard) clearDimensionMetrics() {
	if s.promMetrics == nil {
		return
	}
	clearDimensionMetrics(s.index.Config, s.promMetrics, s.index.Config.ClassName.String(), s.name)
}

// Set shard's vector_dimensions_sum and vector_segments_sum metrics to 0.
//
// Vector dimension metrics are collected on the node level and
// are normally _polled_ from each shard. Shard dimension metrics
// should only be updated on the shard level iff it is being shut
// down or dropped and metrics grouping is disabled.
// If metrics grouping is enabled, the difference is eventually
// accounted for the next time nodeWideMetricsObserver recalculates
// total vector dimensions, because only _active_ shards are considered.
func clearDimensionMetrics(cfg IndexConfig, promMetrics *monitoring.PrometheusMetrics, className, shardName string) {
	if !cfg.TrackVectorDimensions || promMetrics.Group {
		return
	}
	if g, err := promMetrics.VectorDimensionsSum.
		GetMetricWithLabelValues(className, shardName); err == nil {
		g.Set(0)
	}
	if g, err := promMetrics.VectorSegmentsSum.
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

func correctEmptySegments(segments int, dimensions int) int {
	// If segments is 0 (unset), in this case PQ will calculate the number of segments
	// based on the number of dimensions
	if segments > 0 {
		return segments
	}
	return common.CalculateOptimalSegments(dimensions)
}
