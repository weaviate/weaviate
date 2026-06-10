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
		var swapFallback string
		if forTargetStrategy {
			swapFallback = s.SourceBucketName(property.Name)
		}
		bucket := resolveDoubleWriteBucket(shard, bucketName, swapFallback)
		if bucket == nil {
			// Backup sidecar already tidied — skip the mirror write. See
			// resolveDoubleWriteBucket for the post-swap nil semantics.
			return nil
		}
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
		var swapFallback string
		if forTargetStrategy {
			swapFallback = s.SourceBucketName(property.Name)
		}
		bucket := resolveDoubleWriteBucket(shard, bucketName, swapFallback)
		if bucket == nil {
			// Backup sidecar already tidied — skip the mirror write. See
			// resolveDoubleWriteBucket for the post-swap nil semantics.
			return nil
		}
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

// OnMigrationComplete: no-op for a semantic migration. Class-level flip
// happens cluster-wide in flipSemanticMigrationSchema (weaviate/0-weaviate-issues#254).
func (s *MapToBlockmaxStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
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
