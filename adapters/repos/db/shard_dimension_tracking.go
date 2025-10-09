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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/cluster/usage/types"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
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

// since v1.34 StrategyRoaringSet is default strategy for dimensions bucket,
// StrategyMapCollection is left as backward compatibility for buckets created earlier
var DimensionsBucketPrioritizedStrategies = []string{
	lsmkv.StrategyRoaringSet,
	lsmkv.StrategyMapCollection,
}

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
	return s.calcTargetVectorDimensions(ctx, targetVector)
}

// Dimensions returns the total number of dimensions for a given vector
func (s *Shard) Dimensions(ctx context.Context, targetVector string) (int, error) {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector)
	if err != nil {
		return 0, err
	}
	return dimensionality.Count * dimensionality.Dimensions, nil
}

func (s *Shard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) (int, error) {
	dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector)
	if err != nil {
		return 0, err
	}
	return dimensionality.Count * correctEmptySegments(segments, dimensionality.Dimensions), nil
}

func (s *Shard) calcTargetVectorDimensions(ctx context.Context, targetVector string,
) (types.Dimensionality, error) {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return types.Dimensionality{}, errors.Errorf("calcTargetVectorDimensions: no bucket dimensions")
	}
	return calcTargetVectorDimensionsFromBucket(ctx, b, targetVector)
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

func GetDimensionCategoryLegacy(cfg schemaConfig.VectorIndexConfig) (DimensionCategory, int) {
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

func GetDimensionCategory(cfg schemaConfig.VectorIndexConfig) (DimensionCategory, int) {
	// We have special dimension tracking for BQ and PQ to represent reduced costs
	// these are published under the separate vector_segments_dimensions metric
	switch config := cfg.(type) {
	case hnswent.UserConfig:
		return getHNSWCompression(config)
	case flatent.UserConfig:
		return getFlatCompression(config)
	case dynamicent.UserConfig:
		return getDynamicCompression(config)
	default:
		return DimensionCategoryStandard, 0
	}
}

// getHNSWCompression extracts compression info from HNSW configuration
func getHNSWCompression(config hnswent.UserConfig) (DimensionCategory, int) {
	if config.PQ.Enabled {
		return DimensionCategoryPQ, config.PQ.Segments
	}
	if config.BQ.Enabled {
		return DimensionCategoryBQ, 0
	}
	if config.SQ.Enabled {
		return DimensionCategorySQ, 0
	}
	if config.RQ.Enabled {
		return DimensionCategoryRQ, 0
	}
	return DimensionCategoryStandard, 0
}

// getFlatCompression extracts compression info from Flat configuration
func getFlatCompression(config flatent.UserConfig) (DimensionCategory, int) {
	if config.BQ.Enabled {
		return DimensionCategoryBQ, 0
	}
	// Note: Flat indices only support BQ compression currently
	return DimensionCategoryStandard, 0
}

// getDynamicCompression extracts compression info from Dynamic configuration
// Dynamic indices can switch between HNSW and Flat based on data size.
// For metrics purposes, we check both configurations and return the first enabled compression.
func getDynamicCompression(config dynamicent.UserConfig) (DimensionCategory, int) {
	// Check HNSW configuration first (higher priority for dynamic indices)
	if category, segments := getHNSWCompression(config.HnswUC); category != DimensionCategoryStandard {
		return category, segments
	}

	// Fall back to Flat configuration
	return getFlatCompression(config.FlatUC)
}

func correctEmptySegments(segments int, dimensions int) int {
	// If segments is 0 (unset), in this case PQ will calculate the number of segments
	// based on the number of dimensions
	if segments > 0 {
		return segments
	}
	return common.CalculateOptimalSegments(dimensions)
}

// calcTargetVectorDimensionsFromBucket calculates dimensions and object count for a target vector from an LSMKV bucket
func calcTargetVectorDimensionsFromBucket(ctx context.Context, b *lsmkv.Bucket, targetVector string,
) (types.Dimensionality, error) {
	dimensionality := types.Dimensionality{}

	if err := lsmkv.CheckExpectedStrategy(b.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyRoaringSet); err != nil {
		return dimensionality, fmt.Errorf("calcTargetVectorDimensionsFromBucket: %w", err)
	}

	nameLen := len(targetVector)
	expectedKeyLen := nameLen + 4 // vector name + uint32
	var k []byte

	switch b.Strategy() {
	case lsmkv.StrategyMapCollection:
		// Since weaviate 1.34 default dimension bucket strategy is StrategyRoaringSet.
		// For backward compatibility StrategyMapCollection is still supported.

		c := b.MapCursor()
		defer c.Close()

		var v []lsmkv.MapPair
		if nameLen == 0 {
			k, v = c.First(ctx)
		} else {
			k, v = c.Seek(ctx, []byte(targetVector))
		}
		for ; k != nil; k, v = c.Next(ctx) {
			// for named vectors we have to additionally check if the key is prefixed with the vector name
			if len(k) != expectedKeyLen || !strings.HasPrefix(string(k), targetVector) {
				break
			}

			dimLength := binary.LittleEndian.Uint32(k[nameLen:])
			if dimLength > 0 && (dimensionality.Dimensions == 0 || dimensionality.Count == 0) {
				dimensionality.Dimensions = int(dimLength)
				dimensionality.Count = len(v)
			}
		}
	default:
		c := b.CursorRoaringSet()
		defer c.Close()

		var v *sroar.Bitmap
		if nameLen == 0 {
			k, v = c.First()
		} else {
			k, v = c.Seek([]byte(targetVector))
		}
		for ; k != nil; k, v = c.Next() {
			// for named vectors we have to additionally check if the key is prefixed with the vector name
			if len(k) != expectedKeyLen || !strings.HasPrefix(string(k), targetVector) {
				break
			}

			dimLength := binary.LittleEndian.Uint32(k[nameLen:])
			if dimLength > 0 && (dimensionality.Dimensions == 0 || dimensionality.Count == 0) {
				dimensionality.Dimensions = int(dimLength)
				dimensionality.Count = v.GetCardinality()
			}
		}
	}

	return dimensionality, nil
}

func (s *Shard) extendDimensionTrackerLSM(dimLength int, docID uint64, targetVector string) error {
	return s.addToDimensionBucket(dimLength, docID, targetVector, false)
}

// Key (target vector name and dimensionality) | Value Doc IDs
// targetVector,128 | 1,2,4,5,17
// targetVector,128 | 1,2,4,5,17, Tombstone 4,
func (s *Shard) removeDimensionsLSM(dimLength int, docID uint64, targetVector string) error {
	return s.addToDimensionBucket(dimLength, docID, targetVector, true)
}

func (s *Shard) addToDimensionBucket(dimLength int, docID uint64, vecName string, tombstone bool) error {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("add dimension bucket: no bucket dimensions")
	}

	if err := lsmkv.CheckExpectedStrategy(b.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyRoaringSet); err != nil {
		return fmt.Errorf("addToDimensionBucket: %w", err)
	}

	vecNameBytes := []byte(vecName)
	nameLen := len(vecNameBytes)
	dim := uint32(dimLength)

	switch b.Strategy() {
	case lsmkv.StrategyMapCollection:
		// Since weaviate 1.34 default dimension bucket strategy is StrategyRoaringSet.
		// For backward compatibility StrategyMapCollection is still supported.

		// 8 bytes for doc id (map key)
		// 4 bytes for dim count (row key)
		// len(vecName) bytes for vector name (prefix of row key)
		buf := make([]byte, 12+nameLen)
		binary.LittleEndian.PutUint64(buf[:8], docID)
		binary.LittleEndian.PutUint32(buf[8+nameLen:], dim)
		copy(buf[8:], vecNameBytes)

		return b.MapSet(buf[8:], lsmkv.MapPair{
			Key:       buf[:8],
			Value:     []byte{},
			Tombstone: tombstone,
		})
	default:
		key := make([]byte, nameLen+4) // 4 for uint32 dimLength
		copy(key[:nameLen], vecNameBytes)
		binary.LittleEndian.PutUint32(key[nameLen:], dim)

		if tombstone {
			return b.RoaringSetRemoveOne(key, docID)
		}
		return b.RoaringSetAddOne(key, docID)
	}
}
