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
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type DimensionCategory int

const (
	DimensionCategoryStandard DimensionCategory = iota
	DimensionCategoryPQ
	DimensionCategoryBQ
)

func (s *Shard) Dimensions(ctx context.Context, targetVectorType string) int {
	sum, _ := s.calcTargetVectorDimensions(ctx, targetVectorType, func(dimLength int, v int) (int, int) {
		return dimLength * v, dimLength
	})
	return sum
}

func (s *Shard) calcTargetVectorDimensions(ctx context.Context, targetVector string, calcEntry func(dimLen int, v int) (int, int)) (sum int, dimensions int) {
	if targetVector == "" {
		targetVector = "NN"
	}
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0, 0
	}

	curs := b.CursorRoaringSet()
	defer curs.Close()
	for k, v := curs.First(); k != nil; k, v = curs.Next() {

		bm := v

		quant, dimLength := retrieveDimensionsKey(k)
		if quant != targetVector {
			continue
		}
		count_arr := bm.ToArray()
		if len(count_arr) == 0 {
			continue
		}
		count := count_arr[0]

		size, dim := calcEntry(int(dimLength), int(count))
		if dimensions == 0 && dim > 0 {
			dimensions = dim
		}
		sum += size
	}

	return sum, dimensions
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) int {
	sum, dimensions := s.calcTargetVectorDimensions(ctx, targetVector, func(dimLength int, v int) (int, int) {
		return v, dimLength
	})

	return sum * correctEmptySegments(segments, dimensions)
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

		for _, config := range configs {
			dimensions, segments := s.calcDimensionsAndSegments(ctx, config)
			sumDimensions += dimensions
			sumSegments += segments
		}

		sendVectorSegmentsMetric(s.promMetrics, className, s.name, sumSegments)
		sendVectorDimensionsMetric(s.promMetrics, className, s.name, sumDimensions)
	}
}

func (s *Shard) calcDimensionsAndSegments(ctx context.Context, vecCfg schemaConfig.VectorIndexConfig) (dims int, segs int) {
	switch category, segments := getDimensionCategory(vecCfg); category {
	case DimensionCategoryPQ:
		count := s.QuantizedDimensions(ctx, "PQ", segments)
		return 0, count
	case DimensionCategoryBQ:
		count := s.Dimensions(ctx, "BQ") / 8 // BQ has a flat 8x reduction in the dimensions metric
		return 0, count
	default:
		count := s.Dimensions(ctx, "NN")
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

// Empty the dimensions bucket, quickly and efficiently
func (s *Shard) resetDimensionsLSM() error {
	// Load the current one, or an empty one if it doesn't exist
	err := s.store.CreateOrLoadBucket(context.Background(),
		helpers.DimensionsBucketLSM,
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		s.segmentCleanupConfig(),
	)
	if err != nil {
		return fmt.Errorf("create dimensions bucket: %w", err)
	}

	// Fetch the actual bucket
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("resetDimensionsLSM: no bucket dimensions")
	}

	// Create random bucket name
	name, err := GenerateUniqueString(32)
	if err != nil {
		return errors.Wrap(err, "generate unique bucket name")
	}

	// Create a new bucket with the unique name
	err = s.createDimensionsBucket(context.Background(), name)
	if err != nil {
		return errors.Wrap(err, "create temporary dimensions bucket")
	}

	// Replace the old bucket with the new one
	err = s.store.ReplaceBuckets(context.Background(), helpers.DimensionsBucketLSM, name)
	if err != nil {
		return errors.Wrap(err, "replace dimensions bucket")
	}

	return nil
}

// Key (target vector name and dimensionality) | Value Doc IDs
// targetVector,128 | 1,2,4,5,17
// targetVector,128 | 1,2,4,5,17, Tombstone 4,
func (s *Shard) removeDimensionsLSM(
	dimLength int, docID uint64, targetVector string,
) error {
	return s.addToDimensionBucket(dimLength, docID, targetVector, true)
}

func countDimensionsLSM(b *lsmkv.Bucket, key []byte, dimLength int, tombstone bool) error {
	count := uint64(0)
	bm, err := b.RoaringSetGet(key)
	if err != nil {
		count = 0
	} else {
		count_arr := bm.ToArray()
		if len(count_arr) == 0 {
			count = 0
		} else {
			count = count_arr[0]
		}
		b.RoaringSetRemoveOne(key, count)
	}

	if tombstone {
		if count > 0 {
			count = count - 1
		}
	} else {
		count = count + 1
	}

	return b.RoaringSetAddOne(key, uint64(count))
}

func makeDimensionsKey(quant string, docID uint64) []byte {
	if quant == "" {
		quant = "NN"
	}
	key := make([]byte, 10)
	copy(key[:2], quant)
	binary.BigEndian.PutUint64(key[2:], docID)
	return key
}

func retrieveDimensionsKey(key []byte) (string, uint64) {
	quant := string(key[:2])
	return quant, binary.BigEndian.Uint64(key[2:])
}

func (s *Shard) addToDimensionBucket(
	dimLength int, docID uint64, vectorName string, tombstone bool,
) error {
	configs := s.index.GetVectorIndexConfigs()

	vectorType := ""

	for vecName, config := range configs {
		if vectorName == vecName {
			category, _ := getDimensionCategory(config)
			if category == DimensionCategoryPQ {
				vectorType = "PQ"
			} else if category == DimensionCategoryBQ {
				vectorType = "BQ"
			} else {
				vectorType = "NN"
			}
			break
		}
	}

	err := s.addDimensionsProperty(context.Background())
	if err != nil {
		return errors.Wrap(err, "add dimensions property")
	}
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("add dimension bucket: no bucket dimensions")
	}

	return countDimensionsLSM(b, makeDimensionsKey(vectorType, uint64(dimLength)), dimLength, tombstone)
}
