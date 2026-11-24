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
	"context"
	"maps"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmemory"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// EstimateShardMemory returns an estimate of the Go heap the shard holds once
// loaded. It never loads the shard: an unloaded shard is sized from its files.
// It returns nil, nil when the class is gone from the schema.
func (i *Index) EstimateShardMemory(ctx context.Context, shardName string) (*shardmemory.Result, error) {
	inputs, err := i.shardMemoryInputs(ctx, shardName)
	if err != nil || inputs == nil {
		return nil, err
	}
	result := shardmemory.Estimate(*inputs)
	return &result, nil
}

// shardMemoryInputs gathers the inputs of [shardmemory.Estimate] for one shard.
// It returns nil, nil where [Index.EstimateShardMemory] does.
func (i *Index) shardMemoryInputs(ctx context.Context, shardName string) (*shardmemory.Inputs, error) {
	class := i.getClass()
	if class == nil {
		return nil, nil
	}
	vectorConfigs, err := schemaConfig.ExtractVectorConfigs(class)
	if err != nil {
		return nil, err
	}
	activeConfigs, fingerprint, err := activeVectorConfigs(vectorConfigs)
	if err != nil {
		return nil, err
	}

	usage, segmentHeap, err := i.usageAndSegmentHeapForShard(ctx, shardName, false, activeConfigs, fingerprint, true)
	if err != nil || usage == nil {
		return nil, err
	}

	inputs := &shardmemory.Inputs{ObjectCount: usage.ObjectsCount}
	if segmentHeap != nil {
		inputs.BloomBytes = segmentHeap.BloomBytes
		inputs.SmallSegmentBytes = segmentHeap.SmallSegmentBytes
	}

	// An unloaded shard holds no hashtree, so the index config stands in for it.
	// The global override is applied the same way initNonVector applies it at load.
	enabled, asyncConfig := i.asyncReplicationStateForShard(shardName)
	if i.globalreplicationConfig != nil {
		asyncConfig = asyncConfig.Effective(*i.globalreplicationConfig)
	}
	inputs.AsyncReplication = enabled
	inputs.HashtreeHeight = asyncConfig.hashtreeHeight

	rows := make(map[string]*types.VectorUsage, len(usage.NamedVectors))
	for _, row := range usage.NamedVectors {
		rows[row.Name] = row
	}
	userConfigs := i.GetVectorIndexConfigs()
	loadStates := make([]vectorLoadState, 0, len(activeConfigs))
	for _, targetVector := range slices.Sorted(maps.Keys(activeConfigs)) {
		vectorConfig := activeConfigs[targetVector]
		row, ok := rows[targetVector]
		if !ok {
			// A tenant with no directory reports no named vectors. Use the row
			// the unloaded path reports for a vector with no files.
			row, err = unloadedVectorUsage(targetVector, vectorConfig, types.Dimensionality{}, unloadedVectorState{})
			if err != nil {
				return nil, err
			}
		}
		vector, loadState := shardMemoryVector(row, userConfigs[targetVector])
		inputs.Vectors = append(inputs.Vectors, vector)
		loadStates = append(loadStates, loadState)
	}

	inputs.BucketCount = i.loadedBucketCount(class, loadStates)
	return inputs, nil
}

// vectorLoadState is what the vector bucket rules of [Index.loadedBucketCount]
// read for one vector index.
type vectorLoadState struct {
	// indexType names the index that holds the vectors; a dynamic index is
	// the flat or hnsw index it currently runs as.
	indexType string
	// compression is the configured [DimensionCategory] of that index.
	compression string
	// compressed is true when the index is compressed on disk already.
	compressed bool
	// skip is the hnsw Skip setting of a non-dynamic hnsw config.
	skip bool
}

// shardMemoryVector maps one usage row and its vector index config to the
// estimator's vector entry and to the state the bucket rules read.
func shardMemoryVector(row *types.VectorUsage, userConfig schemaConfig.VectorIndexConfig) (shardmemory.VectorIndex, vectorLoadState) {
	// An uncompressed index reports a ratio of exactly 1. A compressed index
	// can report less than 1, as 8-bit RQ does at 5 dimensions or fewer.
	vector := shardmemory.VectorIndex{
		IndexType:        row.VectorIndexType,
		CompressionRatio: row.VectorCompressionRatio,
		Compressed:       row.VectorCompressionRatio != 1,
		Multivector:      row.MultiVectorConfig != nil && row.MultiVectorConfig.Enabled,
		PQ:               row.Compression == DimensionCategoryPQ.String(),
	}
	if len(row.Dimensionalities) > 0 {
		vector.DimensionsTracked = true
		vector.Dimensions = int64(row.Dimensionalities[0].Dimensions)
		vector.VectorCount = int64(row.Dimensionalities[0].Count)
	}
	loadState := vectorLoadState{
		indexType:   row.VectorIndexType,
		compression: row.Compression,
		compressed:  vector.Compressed,
	}

	// A dynamic index builds the flat or hnsw index it runs as from the matching
	// sub-config (vector/dynamic/index.go). Skip is checked only on a plain hnsw
	// config (initVectorIndex in shard_init_vector.go).
	if dynamicConfig, ok := userConfig.(dynamicent.UserConfig); ok {
		if row.VectorIndexType == common.IndexTypeHNSW {
			userConfig = dynamicConfig.HnswUC
		} else {
			userConfig = dynamicConfig.FlatUC
		}
	} else if hnswConfig, ok := userConfig.(enthnsw.UserConfig); ok {
		loadState.skip = hnswConfig.Skip
	}
	switch uc := userConfig.(type) {
	case enthnsw.UserConfig:
		vector.MaxConnections = int64(uc.MaxConnections)
		vector.CacheLimit = int64(uc.VectorCacheMaxObjects)
	case flatent.UserConfig:
		vector.CacheLimit = int64(uc.VectorCacheMaxObjects)
		// same order as extractCache in vector/flat/index.go: BQ wins over RQ
		switch {
		case uc.BQ.Enabled:
			vector.FlatQuantizerCache = uc.BQ.Cache
		case uc.RQ.Enabled:
			vector.FlatQuantizerCache = uc.RQ.Cache
		}
	}
	return vector, loadState
}

// loadedBucketCount returns the number of lsm buckets that loading the shard
// under class opens. It follows the load rules in shard_init_lsm.go,
// shard_init_properties.go, and the flat and hnsw constructors. It does not
// count nested-property buckets or migration-overlay buckets. It does not count
// buckets that exist only for hfresh, muvera or multivector mappings.
func (i *Index) loadedBucketCount(class *models.Class, vectors []vectorLoadState) int64 {
	invertedConfig := i.GetInvertedIndexConfig()

	// objects and id
	count := int64(2)
	if invertedConfig.IndexTimestamps {
		count += 2 // creation and last update time
	}
	if i.Config.TrackVectorDimensions {
		count++
	}

	for _, prop := range class.Properties {
		if _, nested := schema.AsNested(prop.DataType); nested {
			continue
		}
		if !inverted.HasAnyInvertedIndex(prop) {
			continue
		}
		if !createsGeoIndex(prop) {
			count += propertyValueBucketCount(prop)
		}
		if invertedConfig.IndexNullState {
			count++
		}
		if invertedConfig.IndexPropertyLength && hasPropertyLengthBucket(prop) {
			count++
		}
	}

	for _, vector := range vectors {
		count += vectorBucketCount(vector)
	}
	return count
}

// propertyValueBucketCount mirrors createPropertyValueIndex for a property that
// builds no geo index.
func propertyValueBucketCount(prop *models.Property) int64 {
	var count int64
	if inverted.HasFilterableIndex(prop) {
		count++
		if schema.IsRefDataType(prop.DataType) {
			count++ // meta count
		}
	}
	if inverted.HasSearchableIndex(prop) {
		count++
	}
	if inverted.HasRangeableIndex(prop) {
		count++
	}
	return count
}

// hasPropertyLengthBucket mirrors createPropertyLengthIndex. That function
// checks only DataType[0] as written, so array and reference types get a
// length bucket.
func hasPropertyLengthBucket(prop *models.Property) bool {
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeBlobHash, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return false
	default:
		return true
	}
}

// vectorBucketCount is the number of buckets the load of one vector index
// opens beyond the objects bucket.
func vectorBucketCount(vector vectorLoadState) int64 {
	switch vector.indexType {
	case common.IndexTypeFlat:
		// vectors, plus compressed vectors when a quantizer is configured
		if vector.compression != DimensionCategoryStandard.String() {
			return 2
		}
		return 1
	case common.IndexTypeHNSW:
		if vector.skip {
			return 0
		}
		// Building a compressor creates the compressed bucket. BQ builds its
		// compressor in hnsw.New. An index already compressed on disk rebuilds
		// its compressor at load (applyLoadedState in vector/hnsw/startup.go).
		if vector.compression == DimensionCategoryBQ.String() || vector.compressed {
			return 1
		}
		return 0
	default:
		// hfresh buckets are not counted
		return 0
	}
}
