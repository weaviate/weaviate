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
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/usecases/schema"
)

// EnableSearchableStrategy implements MigrationStrategy for creating a
// blockmax (StrategyInverted) searchable index on a text or text[] property
// that currently has none. It builds the bucket from the objects store with
// the target tokenization, then flips both IndexSearchable=true and
// Tokenization on the targeted properties in one RAFT update.
//
// New searchable buckets are created directly as blockmax; no map→blockmax
// transition is ever needed for a from-scratch enable.
type EnableSearchableStrategy struct {
	schemaManager *schema.Manager
	propNames     []string
	tokenization  string
}

func (s *EnableSearchableStrategy) MigrationDirName() string {
	if len(s.propNames) > 0 {
		return "enable_searchable_" + strings.Join(s.propNames, "_")
	}
	return "enable_searchable"
}

func (s *EnableSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *EnableSearchableStrategy) ReindexSuffix() string {
	return "__enable_searchable_reindex"
}

func (s *EnableSearchableStrategy) IngestSuffix() string {
	return "__enable_searchable_ingest"
}

func (s *EnableSearchableStrategy) BackupSuffix() string {
	return "__enable_searchable_backup"
}

func (s *EnableSearchableStrategy) SourceStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *EnableSearchableStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) BackupStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
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

// ShouldProcessProperty always returns true — scope is driven by
// selectedPropsByCollection (see NewRuntimeEnableSearchableTask). The
// HasSearchableIndex schema flag is still false on targeted properties
// until OnMigrationComplete flips it.
func (s *EnableSearchableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *EnableSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
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

func (s *EnableSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
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

// PreReindexHook creates empty blockmax searchable buckets for the targeted
// properties and marks them as blockmax, so queries route to the new bucket
// as soon as it exists.
func (s *EnableSearchableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) == nil {
			opts := shard.makeDefaultBucketOptions(lsmkv.StrategyInverted)
			if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
				shard.index.logger.WithField("bucket", bucketName).
					WithError(err).Error("PreReindexHook: failed to create searchable bucket")
			}
		}
	}
	shard.markSearchableBlockmaxProperties(props...)
}

// OnMigrationComplete flips IndexSearchable=true and sets Tokenization on
// the targeted properties via per-property RAFT UpdateProperty commands.
func (s *EnableSearchableStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	className := shard.Index().Config.ClassName.String()
	class := s.schemaManager.ReadOnlyClass(className)
	if class == nil {
		return fmt.Errorf("class %q not found", className)
	}

	propSet := make(map[string]struct{}, len(s.propNames))
	for _, p := range s.propNames {
		propSet[p] = struct{}{}
	}

	trueVal := true
	for _, prop := range class.Properties {
		if _, ok := propSet[prop.Name]; !ok {
			continue
		}
		if prop.IndexSearchable != nil && *prop.IndexSearchable && prop.Tokenization == s.tokenization {
			continue // already enabled with the target tokenization
		}
		updated := *prop
		updated.IndexSearchable = &trueVal
		updated.Tokenization = s.tokenization
		if err := schema.UpdatePropertyInternal(&s.schemaManager.Handler, ctx, className, &updated); err != nil {
			return fmt.Errorf("updating property %q IndexSearchable: %w", prop.Name, err)
		}
	}
	return nil
}
