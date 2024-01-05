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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (s *Shard) Dimensions() int {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	defer c.Close()
	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		dimLength := binary.LittleEndian.Uint32(k)
		sum += int(dimLength) * len(v)
	}

	return sum
}

func (s *Shard) QuantizedDimensions(segments int) int {
	// Exit early if segments is 0 (unset), in this case PQ will use the same number of dimensions
	// as the segment size
	if segments <= 0 {
		return s.Dimensions()
	}

	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	defer c.Close()
	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		dimLength := binary.LittleEndian.Uint32(k)
		if dimLength > 0 {
			sum += segments * len(v)
		}
	}

	return sum
}

func (s *Shard) clearDimensionMetrics() {
	clearDimensionMetrics(s.promMetrics, s.index.Config.ClassName.String(),
		s.name, s.index.vectorIndexUserConfig)
}

func (s *Shard) publishDimensionMetrics() {
	className := s.index.Config.ClassName.String()

	if pqEnabled, segments := getPQSegments(s.index.vectorIndexUserConfig); pqEnabled {
		count := s.QuantizedDimensions(segments)
		sendVectorSegmentsMetric(s.promMetrics, className, s.name, count)
		sendVectorDimensionsMetric(s.promMetrics, className, s.name, 0)
	} else {
		count := s.Dimensions()
		sendVectorDimensionsMetric(s.promMetrics, className, s.name, count)
	}
}

func (s *Shard) initDimensionTracking() {
	if s.index.Config.TrackVectorDimensions {
		// always send vector dimensions at startup if tracking is enabled
		s.publishDimensionMetrics()
		// start tracking vector dimensions goroutine only when tracking is enabled
		go func() {
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
		}()
	}
}

func getPQSegments(cfg schema.VectorIndexConfig) (bool, int) {
	// Detect if vector index is HNSW
	if hnswUserConfig, ok := cfg.(hnswent.UserConfig); ok && hnswUserConfig.PQ.Enabled {
		return true, hnswUserConfig.PQ.Segments
	}
	return false, 0
}

func sendVectorSegmentsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	if promMetrics != nil {
		metric, err := promMetrics.VectorSegmentsSum.
			GetMetricWithLabelValues(className, shardName)
		if err == nil {
			metric.Set(float64(count))
		}
	}
}

func sendVectorDimensionsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int,
) {
	if promMetrics != nil {
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
}

func clearDimensionMetrics(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, cfg schema.VectorIndexConfig,
) {
	if pqEnabled, _ := getPQSegments(cfg); pqEnabled {
		sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
		sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
	} else {
		sendVectorDimensionsMetric(promMetrics, className, shardName, 0)
	}
}
