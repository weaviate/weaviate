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
)

// RebuildSearchableStrategy rebuilds an existing BlockMax (StrategyInverted)
// searchable bucket from the objects store while preserving the property's
// current tokenization and BM25 algorithm. The strategy assumes the property
// already has IndexSearchable=true on the BlockMax algorithm; the API
// dispatch rejects WAND properties before this strategy is constructed.
type RebuildSearchableStrategy struct {
	propNames  []string
	generation int
}

func (s *RebuildSearchableStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixRebuildSearchable, s.propNames) + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *RebuildSearchableStrategy) ReindexSuffix() string {
	return "__rebuild_searchable_reindex" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) IngestSuffix() string {
	return "__rebuild_searchable_ingest" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) BackupSuffix() string {
	return "__rebuild_searchable_backup" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) SourceStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *RebuildSearchableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *RebuildSearchableStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *RebuildSearchableStrategy) BackupStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *RebuildSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
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

// ShouldProcessProperty is true for every targeted property — selection is
// driven by selectedPropsByCollection in the task config.
func (s *RebuildSearchableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *RebuildSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}
		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		propLen := calcPropLenInverted(property.Items)
		for _, item := range property.Items {
			pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RebuildSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
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

// PreReindexHook is a no-op — the target BlockMax bucket already exists
// (the API dispatch rejects WAND properties before this strategy runs).
func (s *RebuildSearchableStrategy) PreReindexHook(shard *Shard, props []string) {
	_ = shard
	_ = props
}

// AnalyzerOverlay returns nil so the analyzer uses the property's stored
// tokenization. Rebuild MUST NOT change tokenization — that's a separate
// verb ({searchable:{tokenization:X}}).
func (s *RebuildSearchableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	return nil
}

// OnMigrationComplete is a no-op. Rebuild doesn't flip any schema flags
// (the property is already searchable+BlockMax pre-migration).
func (s *RebuildSearchableStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
