//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/binary"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type DimensionCategory int

const (
	DimensionCategoryStandard DimensionCategory = iota
	DimensionCategoryPQ
	DimensionCategoryBQ
)

func (s *Shard) Dimensions(ctx context.Context, targetVector string) int {
	sum, _ := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return dimLength * len(v), dimLength
	})
	return sum
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) int {
	sum, dimensions := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v []lsmkv.MapPair) (int, int) {
		return len(v), dimLength
	})

	return sum * correctEmptySegments(segments, dimensions)
}

func (s *Shard) calcTargetVectorDimensions(ctx context.Context, targetVector string, calcEntry func(dimLen int, v []lsmkv.MapPair) (int, int)) (sum int, dimensions int) {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0, 0
	}

	c := b.MapCursor()
	defer c.Close()

	var (
		nameLen        = len(targetVector)
		expectedKeyLen = 4 + nameLen
	)

	for k, v := c.First(ctx); k != nil; k, v = c.Next(ctx) {
		// for named vectors we have to additionally check if the key is prefixed with the vector name
		keyMatches := len(k) == expectedKeyLen && (nameLen == 4 || strings.HasPrefix(string(k), targetVector))
		if !keyMatches {
			continue
		}

		dimLength := int(binary.LittleEndian.Uint32(k[nameLen:]))
		size, dim := calcEntry(dimLength, v)
		if dimensions == 0 && dim > 0 {
			dimensions = dim
		}
		sum += size
	}

	return sum, dimensions
}

func (s *Shard) initDimensionTracking() {
	// do not use the context passed from NewShard, as that one is only meant for
	// initialization. However, this goroutine keeps running forever, so if the
	// startup context expires, this would error.
	// https://github.com/weaviate/weaviate/issues/5091
	rootCtx := context.Background()
	if s.index.Config.TrackVectorDimensions {
		s.dimensionTrackingInitialized.Store(true)

		// The timeout is rather arbitrary, it's just meant to prevent a context
		// leak. The actual work should be much faster.
		ctx, cancel := context.WithTimeout(rootCtx, 30*time.Minute)
		defer cancel()
		s.index.logger.WithFields(logrus.Fields{
			"action":   "init_dimension_tracking",
			"duration": 30 * time.Minute,
		}).Debug("context.WithTimeout")

		// always send vector dimensions at startup if tracking is enabled
		s.publishDimensionMetrics(ctx)
		// start tracking vector dimensions goroutine only when tracking is enabled
		f := func() {
			t := time.NewTicker(5 * time.Minute)
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
						s.index.logger.WithFields(logrus.Fields{
							"action":   "init_dimension_tracking",
							"duration": 30 * time.Minute,
						}).Debug("context.WithTimeout")
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
			configs   = s.index.GetVectorIndexConfigs()

			sumSegments   = 0
			sumDimensions = 0
		)

		for targetVector, config := range configs {
			dimensions, segments := s.calcDimensionsAndSegments(ctx, config, targetVector)
			sumDimensions += dimensions
			sumSegments += segments
		}

		sendVectorSegmentsMetric(s.promMetrics, className, s.name, sumSegments)
		sendVectorDimensionsMetric(s.promMetrics, className, s.name, sumDimensions)
	}
}

func (s *Shard) calcDimensionsAndSegments(ctx context.Context, vecCfg schemaConfig.VectorIndexConfig, vecName string) (dims int, segs int) {
	switch category, segments := getDimensionCategory(vecCfg); category {
	case DimensionCategoryPQ:
		count := s.QuantizedDimensions(ctx, vecName, segments)
		return 0, count
	case DimensionCategoryBQ:
		count := s.Dimensions(ctx, vecName) / 8 // BQ has a flat 8x reduction in the dimensions metric
		return 0, count
	default:
		count := s.Dimensions(ctx, vecName)
		return count, 0
	}
}

func (s *Shard) clearDimensionMetrics() {
	clearDimensionMetrics(s.promMetrics, s.index.Config.ClassName.String(), s.name)
}

func clearDimensionMetrics(promMetrics *monitoring.PrometheusMetrics, className, shardName string) {
	if promMetrics != nil {
		sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
		sendVectorSegmentsMetric(promMetrics, className, shardName, 0)
	}
}

func sendVectorSegmentsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	metric, err := promMetrics.VectorSegmentsSum.
		GetMetricWithLabelValues(className, shardName)
	if err == nil {
		metric.Set(float64(count))
	}
}

func sendVectorDimensionsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	// Important: Never group classes/shards for this metric. We need the
	// granularity here as this tracks an absolute value per shard that changes
	// independently over time.
	//
	// If we need to reduce metrics further, an alternative could be to not
	// make dimension tracking shard-centric, but rather make it node-centric.
	// Then have a single metric that aggregates all dimensions first, then
	// observes only the sum
	metric, err := promMetrics.VectorDimensionsSum.
		GetMetricWithLabelValues(className, shardName)
	if err == nil {
		metric.Set(float64(count))
	}
}

func getDimensionCategory(cfg schemaConfig.VectorIndexConfig) (DimensionCategory, int) {
	// We have special dimension tracking for BQ and PQ to represent reduced costs
	// these are published under the separate vector_segments_dimensions metric
	if hnswUserConfig, ok := cfg.(hnswent.UserConfig); ok {
		if hnswUserConfig.PQ.Enabled {
			return DimensionCategoryPQ, hnswUserConfig.PQ.Segments
		}
		if hnswUserConfig.BQ.Enabled {
			return DimensionCategoryBQ, 0
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
