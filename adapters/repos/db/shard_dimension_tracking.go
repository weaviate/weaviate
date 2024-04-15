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
	"encoding/binary"
	"strings"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type DimensionCategory int

const (
	DimensionCategoryStandard DimensionCategory = iota
	DimensionCategoryPQ
	DimensionCategoryBQ
)

func (s *Shard) Dimensions() int {
	keyLen := 4
	return s.calcDimensions(func(k []byte, v []lsmkv.MapPair) int {
		// consider only keys of len 4, skipping ones prefixed with vector name
		if len(k) == keyLen {
			dimLength := binary.LittleEndian.Uint32(k)
			return int(dimLength) * len(v)
		}
		return 0
	})
}

func (s *Shard) DimensionsForVec(vecName string) int {
	nameLen := len(vecName)
	keyLen := nameLen + 4
	return s.calcDimensions(func(k []byte, v []lsmkv.MapPair) int {
		// consider only keys of len vecName + 4, prefixed with vecName
		if len(k) == keyLen && strings.HasPrefix(string(k), vecName) {
			dimLength := binary.LittleEndian.Uint32(k[nameLen:])
			return int(dimLength) * len(v)
		}
		return 0
	})
}

func (s *Shard) QuantizedDimensions(segments int) int {
	// Exit early if segments is 0 (unset), in this case PQ will use the same number of dimensions
	// as the segment size
	if segments <= 0 {
		return s.Dimensions()
	}

	keyLen := 4
	return s.calcDimensions(func(k []byte, v []lsmkv.MapPair) int {
		// consider only keys of len 4, skipping ones prefixed with vector name
		if len(k) == keyLen {
			if dimLength := binary.LittleEndian.Uint32(k); dimLength > 0 {
				return len(v)
			}
		}
		return 0
	}) * segments
}

func (s *Shard) QuantizedDimensionsForVec(segments int, vecName string) int {
	// Exit early if segments is 0 (unset), in this case PQ will use the same number of dimensions
	// as the segment size
	if segments <= 0 {
		return s.DimensionsForVec(vecName)
	}

	nameLen := len(vecName)
	keyLen := nameLen + 4
	return s.calcDimensions(func(k []byte, v []lsmkv.MapPair) int {
		// consider only keys of len vecName + 4, prefixed with vecName
		if len(k) == keyLen && strings.HasPrefix(string(k), vecName) {
			if dimLength := binary.LittleEndian.Uint32(k[nameLen:]); dimLength > 0 {
				return len(v)
			}
		}
		return 0
	}) * segments
}

func (s *Shard) calcDimensions(calcEntry func(k []byte, v []lsmkv.MapPair) int) int {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	defer c.Close()

	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		sum += calcEntry(k, v)
	}

	return sum
}

func (s *Shard) initDimensionTracking() {
	if s.index.Config.TrackVectorDimensions {
		// always send vector dimensions at startup if tracking is enabled
		s.publishDimensionMetrics()
		// start tracking vector dimensions goroutine only when tracking is enabled
		f := func() {
			t := time.NewTicker(5 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-s.stopMetrics:
					return
				case <-t.C:
					s.publishDimensionMetrics()
				}
			}
		}
		enterrors.GoWrapper(f, s.index.logger)
	}
}

func (s *Shard) publishDimensionMetrics() {
	if s.promMetrics != nil {
		className := s.index.Config.ClassName.String()

		if !s.hasTargetVectors() {
			// send stats for legacy vector only
			switch category, segments := getDimensionCategory(s.index.vectorIndexUserConfig); category {
			case DimensionCategoryPQ:
				count := s.QuantizedDimensions(segments)
				sendVectorSegmentsMetric(s.promMetrics, className, s.name, count)
				sendVectorDimensionsMetric(s.promMetrics, className, s.name, 0)
			case DimensionCategoryBQ:
				count := s.Dimensions() / 8 // BQ has a flat 8x reduction in the dimensions metric
				sendVectorSegmentsMetric(s.promMetrics, className, s.name, count)
				sendVectorDimensionsMetric(s.promMetrics, className, s.name, 0)
			default:
				count := s.Dimensions()
				sendVectorDimensionsMetric(s.promMetrics, className, s.name, count)
			}
			return
		}

		sumSegments := 0
		sumDimensions := 0

		// send stats for each target vector
		for vecName, vecCfg := range s.index.vectorIndexUserConfigs {
			switch category, segments := getDimensionCategory(vecCfg); category {
			case DimensionCategoryPQ:
				count := s.QuantizedDimensionsForVec(segments, vecName)
				sumSegments += count
				sendVectorSegmentsForVecMetric(s.promMetrics, className, s.name, count, vecName)
				sendVectorDimensionsForVecMetric(s.promMetrics, className, s.name, 0, vecName)
			case DimensionCategoryBQ:
				count := s.DimensionsForVec(vecName) / 8 // BQ has a flat 8x reduction in the dimensions metric
				sumSegments += count
				sendVectorSegmentsForVecMetric(s.promMetrics, className, s.name, count, vecName)
				sendVectorDimensionsForVecMetric(s.promMetrics, className, s.name, 0, vecName)
			default:
				count := s.DimensionsForVec(vecName)
				sumDimensions += count
				sendVectorDimensionsForVecMetric(s.promMetrics, className, s.name, count, vecName)
			}
		}

		// send sum stats for all target vectors
		sendVectorSegmentsMetric(s.promMetrics, className, s.name, sumSegments)
		sendVectorDimensionsMetric(s.promMetrics, className, s.name, sumDimensions)
	}
}

func (s *Shard) clearDimensionMetrics() {
	clearDimensionMetrics(s.promMetrics, s.index.Config.ClassName.String(),
		s.name, s.index.vectorIndexUserConfig, s.index.vectorIndexUserConfigs)
}

func clearDimensionMetrics(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string,
	cfg schema.VectorIndexConfig, targetCfgs map[string]schema.VectorIndexConfig,
) {
	if promMetrics != nil {
		if !hasTargetVectors(cfg, targetCfgs) {
			// send stats for legacy vector only
			switch category, _ := getDimensionCategory(cfg); category {
			case DimensionCategoryPQ, DimensionCategoryBQ:
				sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
				sendVectorSegmentsMetric(promMetrics, className, shardName, 0)
			default:
				sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
			}
			return
		}

		// send stats for each target vector
		for vecName, vecCfg := range targetCfgs {
			switch category, _ := getDimensionCategory(vecCfg); category {
			case DimensionCategoryPQ, DimensionCategoryBQ:
				sendVectorDimensionsForVecMetric(promMetrics, className, shardName, 0, vecName)
				sendVectorSegmentsForVecMetric(promMetrics, className, shardName, 0, vecName)
			default:
				sendVectorDimensionsForVecMetric(promMetrics, className, shardName, 0, vecName)
			}
		}

		// send sum stats for all target vectors
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

func sendVectorSegmentsForVecMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int, vecName string,
) {
	metric, err := promMetrics.VectorSegmentsSumByVector.
		GetMetricWithLabelValues(className, shardName, vecName)
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

func sendVectorDimensionsForVecMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int, vecName string,
) {
	// Important: Never group classes/shards for this metric. We need the
	// granularity here as this tracks an absolute value per shard that changes
	// independently over time.
	//
	// If we need to reduce metrics further, an alternative could be to not
	// make dimension tracking shard-centric, but rather make it node-centric.
	// Then have a single metric that aggregates all dimensions first, then
	// observes only the sum
	metric, err := promMetrics.VectorDimensionsSumByVector.
		GetMetricWithLabelValues(className, shardName, vecName)
	if err == nil {
		metric.Set(float64(count))
	}
}

func getDimensionCategory(cfg schema.VectorIndexConfig) (DimensionCategory, int) {
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
