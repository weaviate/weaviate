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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/usecases/schema"
)

// MapToBlockmaxStrategy implements MigrationStrategy for the
// MapCollection → Inverted (blockmax WAND) migration of searchable properties.
type MapToBlockmaxStrategy struct {
	schemaManager *schema.Manager
}

func (s *MapToBlockmaxStrategy) MigrationDirName() string {
	return "searchable_map_to_blockmax"
}

func (s *MapToBlockmaxStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

// Backward compat: existing in-progress migrations use __blockmax_reindex on disk.
func (s *MapToBlockmaxStrategy) ReindexSuffix() string {
	return "__blockmax_reindex"
}

// Backward compat: existing in-progress migrations use __blockmax_ingest on disk.
func (s *MapToBlockmaxStrategy) IngestSuffix() string {
	return "__blockmax_ingest"
}

// Backward compat: existing in-progress migrations use __blockmax_map on disk.
func (s *MapToBlockmaxStrategy) BackupSuffix() string {
	return "__blockmax_map"
}

func (s *MapToBlockmaxStrategy) SourceStrategy() string {
	return lsmkv.StrategyMapCollection
}

func (s *MapToBlockmaxStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *MapToBlockmaxStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *MapToBlockmaxStrategy) BackupStrategy() string {
	return lsmkv.StrategyMapCollection
}

func (s *MapToBlockmaxStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	propLen := calcPropLenInverted(prop.Items)
	for _, item := range prop.Items {
		pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
		if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

func (s *MapToBlockmaxStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasSearchableIndex
}

func (s *MapToBlockmaxStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	calcPropLen := calcPropLenMap
	if forTargetStrategy {
		calcPropLen = calcPropLenInverted
	}

	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		propLen := calcPropLen(property.Items)
		for _, item := range property.Items {
			pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *MapToBlockmaxStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.deleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *MapToBlockmaxStrategy) PreReindexHook(shard *Shard, props []string) {
	shard.markSearchableBlockmaxProperties(props...)
}

func (s *MapToBlockmaxStrategy) OnMigrationComplete(ctx context.Context, className string) error {
	return updateToBlockMaxInvertedIndexConfig(ctx, s.schemaManager, className)
}

// calcPropLenInverted computes property length as the sum of term frequencies.
func calcPropLenInverted(items []inverted.Countable) float32 {
	propLen := float32(0)
	for _, item := range items {
		propLen += item.TermFrequency
	}
	return propLen
}

// calcPropLenMap computes property length as the count of items.
func calcPropLenMap(items []inverted.Countable) float32 {
	return float32(len(items))
}
