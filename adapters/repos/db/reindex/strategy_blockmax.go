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

package reindex

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
	noAnalyzerOverlay
	schemaManager *schema.Manager
	generation    int // see genSuffix godoc
}

func (s *MapToBlockmaxStrategy) MigrationDirName() string {
	return MigrationDirSearchableMapToBlockmax + genSuffix(s.generation)
}

func (s *MapToBlockmaxStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *MapToBlockmaxStrategy) ReindexSuffix() string {
	return "__blockmax_reindex" + genSuffix(s.generation)
}

func (s *MapToBlockmaxStrategy) IngestSuffix() string {
	return "__blockmax_ingest" + genSuffix(s.generation)
}

func (s *MapToBlockmaxStrategy) BackupSuffix() string {
	return "__blockmax_map" + genSuffix(s.generation)
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
		pair := shard.PairPropertyWithFrequency(docID, item.TermFrequency, propLen)
		if err := shard.AddToPropertyMapBucket(bucket, pair, item.Data); err != nil {
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

	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		propLen := calcPropLen(property.Items)
		for _, item := range property.Items {
			pair := shard.PairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.AddToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *MapToBlockmaxStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.DeleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *MapToBlockmaxStrategy) PreReindexHook(shard ShardLike, props []string) {
	shard.MarkSearchableBlockmaxProperties(props...)
}

// OnMigrationComplete flips the UsingBlockMaxWAND class flag, but only once
// every searchable bucket on this shard has already been migrated to the
// target (blockmax) strategy. When a per-property rebuild targets a subset
// of the class's searchable properties, the remaining properties still use
// the source (map) strategy and the class-level flag must stay off until
// they are migrated too — otherwise BM25 queries over the still-map
// properties would hit the wrong query path.
//
// The check is shard-local, which is sufficient because each shard runs its
// own migration independently and this hook fires after that shard's swap
// phase has finished. Once the last property on the last shard reaches this
// hook, the class-level flag is flipped (the flag write itself is a single
// RAFT entry guarded by an "already set" short-circuit).
func (s *MapToBlockmaxStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	className := shard.ParentIndex().ClassName().String()

	for name, bucket := range shard.Store().GetBucketsByName() {
		_, indexType := GetPropNameAndIndexTypeFromBucketName(name)
		if indexType != IndexTypePropSearchableValue {
			continue
		}
		if bucket.Strategy() == s.SourceStrategy() {
			// At least one searchable property on this shard still uses the
			// pre-migration (map) strategy. Defer flipping the class flag
			// until a subsequent per-property rebuild completes the migration.
			return nil
		}
	}

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
