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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/cluster/usage/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
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

// initDimensionTracking initializes the dimension tracking for a shard.
// it's no op if the trackVectorDimensions is disabled or the usage is enabled
func (s *Shard) initDimensionTracking() {
	// do not use the context passed from NewShard, as that one is only meant for
	// initialization. However, this goroutine keeps running forever, so if the
	// startup context expires, this would error.
	// https://github.com/weaviate/weaviate/issues/5091
	rootCtx := context.Background()
	if s.index.Config.TrackVectorDimensions && !s.index.Config.UsageEnabled {
		s.dimensionTrackingInitialized.Store(true)

		// The timeout is rather arbitrary, it's just meant to prevent a context
		// leak. The actual work should be much faster.
		ctx, cancel := context.WithTimeout(rootCtx, 30*time.Minute)
		defer cancel()

		// always send vector dimensions at startup if tracking is enabled
		s.publishDimensionMetrics(ctx)
		// start tracking vector dimensions goroutine only when tracking is enabled
		f := func() {
			interval := config.DefaultTrackVectorDimensionsInterval
			if s.index.Config.TrackVectorDimensionsInterval != 0 {
				interval = s.index.Config.TrackVectorDimensionsInterval
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-s.stopDimensionTracking:
					s.dimensionTrackingInitialized.Store(false)
					return
				case <-t.C:
					func() {
						// The timeout is rather arbitrary, it's just meant to prevent a context
						// leak. The actual work should be much faster.
						ctx, cancel := context.WithTimeout(rootCtx, 30*time.Minute)
						defer cancel()
						s.publishDimensionMetrics(ctx)
					}()
				}
			}
		}
		enterrors.GoWrapper(f, s.index.logger)
	}
}

func (s *Shard) publishDimensionMetrics(ctx context.Context) {
	if s.promMetrics != nil {
		var (
			className = s.index.Config.ClassName.String()
			shardName = s.name
			configs   = s.index.GetVectorIndexConfigs()

			sumSegments   = 0
			sumDimensions = 0
		)

		// Apply grouping logic when PROMETHEUS_MONITORING_GROUP is enabled
		if s.promMetrics.Group {
			className = "n/a"
			shardName = "n/a"
		}

		for targetVector, config := range configs {
			metrics := s.calcDimensionMetrics(ctx, config, targetVector)
			sumDimensions += metrics.Uncompressed
			sumSegments += metrics.Compressed
		}

		sendVectorSegmentsMetric(s.promMetrics, className, shardName, sumSegments)
		sendVectorDimensionsMetric(s.promMetrics, className, shardName, sumDimensions)
	}
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

func (s *Shard) calcDimensionMetrics(ctx context.Context, vecCfg schemaConfig.VectorIndexConfig, vecName string) DimensionMetrics {
	switch category, segments := GetDimensionCategory(vecCfg); category {
	case DimensionCategoryPQ:
		return DimensionMetrics{Uncompressed: 0, Compressed: s.QuantizedDimensions(ctx, vecName, segments)}
	case DimensionCategoryBQ:
		// BQ: 1 bit per dimension, packed into uint64 blocks (8 bytes per 64 dimensions)
		// [1..64] dimensions -> 8 bytes, [65..128] dimensions -> 16 bytes, etc.
		// Roundup is required because BQ packs bits into uint64 blocks - you can't have
		// a partial uint64 block. Even 1 dimension needs a full 8-byte uint64 block.
		count, _ := s.Dimensions(ctx, vecName)
		bytes := (count + 63) / 64 * 8 // Round up to next uint64 block, then multiply by 8 bytes
		return DimensionMetrics{Uncompressed: 0, Compressed: bytes}
	default:
		count, _ := s.Dimensions(ctx, vecName)
		return DimensionMetrics{Uncompressed: count, Compressed: 0}
	}
}

func (s *Shard) getTotalDimensionMetrics(ctx context.Context) DimensionMetrics {
	var total DimensionMetrics
	for vectorName, config := range s.index.GetVectorIndexConfigs() {
		metrics := s.calcDimensionMetrics(ctx, config, vectorName)
		total = total.Add(metrics)
	}
	return total
}

func (s *Shard) clearDimensionMetrics(ctx context.Context) {
	// s.dimensionMetricsCh <- taggedDimensionMetrics{
	// 	ClassName: s.index.Config.ClassName.String(),
	// 	ShardName: s.Name(),
	// 	Dimensions: s.getTotalDimensionMetrics(ctx),
	// }
}

func clearDimensionMetrics(promMetrics *monitoring.PrometheusMetrics, className, shardName string) {
	if promMetrics != nil {
		// Apply grouping logic when PROMETHEUS_MONITORING_GROUP is enabled
		if promMetrics.Group {
			className = "n/a"
			shardName = "n/a"
		}
		sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
		sendVectorSegmentsMetric(promMetrics, className, shardName, 0)
	}
}

func sendVectorSegmentsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	// Apply grouping logic when PROMETHEUS_MONITORING_GROUP is enabled
	if promMetrics != nil && promMetrics.Group {
		className = "n/a"
		shardName = "n/a"
	}

	metric, err := promMetrics.VectorSegmentsSum.
		GetMetricWithLabelValues(className, shardName)
	if err == nil {
		metric.Set(float64(count))
	}
}

func sendVectorDimensionsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	// Apply grouping logic when PROMETHEUS_MONITORING_GROUP is enabled
	if promMetrics != nil && promMetrics.Group {
		className = "n/a"
		shardName = "n/a"
	}

	metric, err := promMetrics.VectorDimensionsSum.
		GetMetricWithLabelValues(className, shardName)
	if err == nil {
		metric.Set(float64(count))
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
