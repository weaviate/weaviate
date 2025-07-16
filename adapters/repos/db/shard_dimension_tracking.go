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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// DimensionsUsage returns the total number of dimensions and the number of objects for a given vector type
func (s *Shard) DimensionsUsage(ctx context.Context, targetVectorType string) (int64, int64, error) {
	dims, count := calcTargetVectorDimensionsFromStore(ctx, s.store,targetVectorType, func(dimLength int, v int64) (int64, int64) {
		return v, int64(dimLength)
	})
	return  dims, count, nil
}

// Dimensions returns the total number of dimensions for a given vector type
func (s *Shard) Dimensions(ctx context.Context, targetVectorType string) (int64, error) {
	dims, count := calcTargetVectorDimensionsFromStore(ctx, s.store, targetVectorType, func(dimLength int, v int64) (int64, int64) {
		return  v, int64(dimLength)
	})

	return dims*count, nil
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVectorType string, segments int64) int64 {
	dims, _ := calcTargetVectorDimensionsFromStore(ctx, s.store,targetVectorType, func(dimLength int, v int64) (int64, int64) {
		return v, int64(dimLength)
	})


	return dims * correctEmptySegments(int(segments), dims)
}



// calcTargetVectorDimensionsFromStore calculates dimensions and object count for a target vector from an LSMKV store
func calcTargetVectorDimensionsFromStore(ctx context.Context, store *lsmkv.Store, targetVector string, calcEntry func(dimLen int, v int64) (int64, int64)) (int64, int64) {
	b := store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0, 0
	}
	return calcTargetVectorDimensionsFromBucket(ctx, b, targetVector, calcEntry)
}

// calcTargetVectorDimensionsFromBucket calculates dimensions and object count for a target vector from an LSMKV bucket
func calcTargetVectorDimensionsFromBucket(ctx context.Context, b *lsmkv.Bucket, targetVector string, calcEntry func(dimLen int, v int64) (int64, int64)) (int64, int64) {

	c := b.Cursor()
	defer c.Close()

	var (
		dimensions     = int64(0)
		count          = int64(0)
	)

	for k, vb := c.First(); k != nil; k, vb = c.Next() {
		vecName := string(k[4:])
		fmt.Printf("calcTargetVectorDimensionsFromBucket: vecName=%s, targetVector=%s\n", vecName, targetVector)
		// for named vectors we have to additionally check if the key is prefixed with the vector name

		if vecName != targetVector {
			continue
		}

		v := int64(binary.LittleEndian.Uint64(vb))
		dimLength := int(binary.LittleEndian.Uint32(k[:4]))
		cnt, dim := calcEntry(dimLength, v)

		if dimensions == 0 && dim > 0 {
			dimensions = dim
		}
		count += cnt
		fmt.Printf("calcTargetVectorDimensionsFromBucket: targetVector=%s, dimLength=%d, dimensions=%d, count=%d\n", targetVector, dimLength, dimensions, count)
	}

	return dimensions, count
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
			sumSegments   = int64(0)
			sumDimensions = int64(0)
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


func (s *Shard) calcDimensionsAndSegments(ctx context.Context, vecCfg schemaConfig.VectorIndexConfig) (dims int64, segs int64) {
	switch category, segments := GetDimensionCategory(vecCfg); category {
	case DimensionCategoryPQ:

		count := s.QuantizedDimensions(ctx, "PQ", int64(segments))
		return 0, count
	case DimensionCategoryBQ:
		// BQ: 1 bit per dimension, packed into uint64 blocks (8 bytes per 64 dimensions)
		// [1..64] dimensions -> 8 bytes, [65..128] dimensions -> 16 bytes, etc.
		// Roundup is required because BQ packs bits into uint64 blocks - you can't have
		// a partial uint64 block. Even 1 dimension needs a full 8-byte uint64 block.
		count, err := s.Dimensions(ctx, "BQ")
		count = (count +63) / 64 * 8 // Round up to next uint64 block, then multiply by 8 bytes
		if err != nil {
			s.index.logger.WithField("shard", s.name).
				Errorf("error while getting dimensions for shard %s: %v", s.name, err)
			return 0, 0
		}
		return 0, count
	default:

		count, err := s.Dimensions(ctx, "NN")
		if err != nil {
			s.index.logger.WithField("shard", s.name).
				Errorf("error while getting dimensions for shard %s: %v", s.name, err)
			return 0, 0
		}
		return count, 0
	}
}

// Empty the dimensions bucket, quickly and efficiently
func (s *Shard) resetDimensionsLSM() error {
	// Load the current one, or an empty one if it doesn't exist
	err := s.store.CreateOrLoadBucket(context.Background(),
		helpers.DimensionsBucketLSM,
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
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


func (s *Shard) calcDimensionMetrics(ctx context.Context, vecCfg schemaConfig.VectorIndexConfig, vecName string) (int64, int64) {
	switch category, segments := GetDimensionCategory(vecCfg); category {
	case DimensionCategoryPQ:
		return  0, s.QuantizedDimensions(ctx, vecName, int64(segments))
	case DimensionCategoryBQ:
		// BQ: 1 bit per dimension, packed into uint64 blocks (8 bytes per 64 dimensions)
		// [1..64] dimensions -> 8 bytes, [65..128] dimensions -> 16 bytes, etc.
		// Roundup is required because BQ packs bits into uint64 blocks - you can't have
		// a partial uint64 block. Even 1 dimension needs a full 8-byte uint64 block.
		count, _ := s.Dimensions(ctx, vecName)
		bytes := (count + 63) / 64 * 8 // Round up to next uint64 block, then multiply by 8 bytes
		return 0, bytes
	default:
		count, _ := s.Dimensions(ctx, vecName)
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
	className, shardName string, count int64,
) {
	metric, err := promMetrics.VectorSegmentsSum.
		GetMetricWithLabelValues(className, shardName)
	if err == nil {
		metric.Set(float64(count))
	}
}

func sendVectorDimensionsMetric(promMetrics *monitoring.PrometheusMetrics,
	className, shardName string, count int64,
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

func correctEmptySegments(segments int, dimensions int64) int64 {
	// If segments is 0 (unset), in this case PQ will calculate the number of segments
	// based on the number of dimensions
	if segments > 0 {
		return int64(segments)
	}
	return int64(common.CalculateOptimalSegments(dimensions))
}
