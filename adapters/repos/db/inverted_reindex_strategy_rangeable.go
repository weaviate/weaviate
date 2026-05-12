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
	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// FilterableToRangeableStrategy implements MigrationStrategy for building
// RoaringSetRange (rangeable) indexes from existing RoaringSet (filterable)
// data. This creates new rangeable buckets alongside existing filterable ones.
type FilterableToRangeableStrategy struct {
	noAnalyzerOverlay
	schemaManager *schema.Manager
	propNames     []string
}

func (s *FilterableToRangeableStrategy) MigrationDirName() string {
	// Include property names in the dir so multiple per-property tasks
	// on the same shard don't share tracker state.
	return migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, s.propNames)
}

func (s *FilterableToRangeableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketRangeableFromPropNameLSM(propName)
}

func (s *FilterableToRangeableStrategy) ReindexSuffix() string {
	return "__rangeable_reindex"
}

func (s *FilterableToRangeableStrategy) IngestSuffix() string {
	return "__rangeable_ingest"
}

func (s *FilterableToRangeableStrategy) BackupSuffix() string {
	return "__rangeable_backup"
}

func (s *FilterableToRangeableStrategy) SourceStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableToRangeableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropValue
}

func (s *FilterableToRangeableStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSetRange
}

func (s *FilterableToRangeableStrategy) BackupStrategy() string {
	return lsmkv.StrategyRoaringSetRange
}

func (s *FilterableToRangeableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	for _, item := range prop.Items {
		if len(item.Data) != 8 {
			return fmt.Errorf("rangeable prop '%s' docID %d: invalid value length %d, should be 8 bytes",
				prop.Name, docID, len(item.Data))
		}
		if err := bucket.RoaringSetRangeAdd(binary.BigEndian.Uint64(item.Data), docID); err != nil {
			return fmt.Errorf("adding rangeable prop '%s' docID %d: %w", prop.Name, docID, err)
		}
	}
	return nil
}

func (s *FilterableToRangeableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasFilterableIndex
}

func (s *FilterableToRangeableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.addToPropertyRangeBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding rangeable prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableToRangeableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.deleteFromPropertyRangeBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting rangeable prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// PreReindexHook creates empty rangeable buckets so the swap phase has a
// "source" bucket to replace with the populated ingest bucket.
func (s *FilterableToRangeableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) != nil {
			continue
		}
		opts := shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)
		if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.index.logger.WithField("bucket", bucketName).
				WithError(err).Error("PreReindexHook: failed to create rangeable bucket")
		}
	}
}

// OnMigrationComplete updates the schema to set IndexRangeFilters=true on
// the migrated properties. It uses per-property UpdateProperty RAFT commands
// instead of UpdateClass, because UpdateClass rejects property field changes
// via validatePropertiesForUpdate on RAFT replay.
//
// Concurrency note: MergeProps in cluster/schema/meta_class.go overwrites ALL
// FOUR property fields (IndexRangeFilters, IndexFilterable, IndexSearchable,
// and Tokenization when non-empty) from the incoming message, not just the
// one this strategy intends to change. If two strategies run concurrently on
// the same property (e.g. enable-rangeable + enable-filterable), each could
// read a stale view of the schema and clobber the other's flag on RAFT
// apply.
//
// We cannot simply nil out the flags we don't want to change: the schema
// handler's setPropertyDefaults fills nil flags with defaults (true for
// IndexFilterable / IndexSearchable on text properties) before the RAFT
// message is built, which would clobber a previously committed `false`
// value. So we re-read the class right before each per-property update to
// minimize the staleness window, and carry the freshly observed values for
// the other three fields through unchanged.
//
// TODO(fieldmask): the proper long-term fix is a fieldmask on UpdateProperty
// so only named fields are merged, but that requires changes across
// cluster/schema/manager.go and meta_class.go.
func (s *FilterableToRangeableStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	className := shard.Index().Config.ClassName.String()
	trueVal := true
	// Missing properties are tolerated: a property dropped between
	// scheduling and completion is the same outcome we'd want anyway.
	_, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, s.propNames,
		func(prop *models.Property) bool {
			if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				return false // already enabled
			}
			prop.IndexRangeFilters = &trueVal
			return true
		})
	return err
}
