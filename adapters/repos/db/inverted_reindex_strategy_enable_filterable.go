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
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// EnableFilterableStrategy implements MigrationStrategy for creating a
// RoaringSet filterable index on a property that currently has none. It
// builds the bucket from the objects store, then flips IndexFilterable=true
// on the targeted properties via a per-property RAFT update.
//
// Contrast with RoaringSetRefreshStrategy, which refreshes an already-
// enabled filterable index: this strategy cannot rely on the schema flag
// during the migration (it's still false), so it scopes work via
// selectedPropsByCollection and drops the HasFilterableIndex guard in the
// double-write callbacks.
type EnableFilterableStrategy struct {
	schemaManager *schema.Manager
	propNames     []string
}

func (s *EnableFilterableStrategy) MigrationDirName() string {
	// Include property names in the dir so multiple per-property tasks
	// on the same shard don't share tracker state.
	return migrationDirWithProps(MigrationDirPrefixEnableFilterable, s.propNames)
}

func (s *EnableFilterableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketFromPropNameLSM(propName)
}

func (s *EnableFilterableStrategy) ReindexSuffix() string {
	return "__enable_filterable_reindex"
}

func (s *EnableFilterableStrategy) IngestSuffix() string {
	return "__enable_filterable_ingest"
}

func (s *EnableFilterableStrategy) BackupSuffix() string {
	return "__enable_filterable_backup"
}

func (s *EnableFilterableStrategy) SourceStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *EnableFilterableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropValue
}

func (s *EnableFilterableStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *EnableFilterableStrategy) BackupStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *EnableFilterableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	for _, item := range prop.Items {
		if err := bucket.RoaringSetAddOne(item.Data, docID); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

// ShouldProcessProperty always returns true. Scope is driven by the
// reindexTaskConfig.selectedPropsByCollection set in the task constructor,
// not by the schema flag — during this migration IndexFilterable is still
// false on every targeted property.
func (s *EnableFilterableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *EnableFilterableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — it's false on the target
		// property until OnMigrationComplete flips it.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *EnableFilterableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// PreReindexHook creates empty filterable (RoaringSet) buckets for the
// targeted properties so the generic state machine has a "source" bucket
// to swap with the populated ingest bucket.
func (s *EnableFilterableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		bucketName := helpers.BucketFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) != nil {
			continue
		}
		opts := shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)
		if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.index.logger.WithField("bucket", bucketName).
				WithError(err).Error("PreReindexHook: failed to create filterable bucket")
		}
	}
}

// AnalyzerOverlay forces IndexFilterable=true on the targeted properties
// while the backfill iterator scans the objects bucket. Until
// OnMigrationComplete flips the RAFT-stored schema flag, the analyzer would
// otherwise skip the property entirely (see HasAnyInvertedIndex in
// inverted/objects.go) and the new bucket would come out empty.
func (s *EnableFilterableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]inverted.PropertyOverlay, len(props))
	for _, p := range props {
		out[p] = inverted.PropertyOverlay{ForceFilterable: true}
	}
	return out
}

// OnMigrationComplete flips IndexFilterable=true on the targeted properties
// via per-property RAFT UpdateProperty commands — matching the pattern used
// by FilterableToRangeableStrategy.
//
// Concurrency note: MergeProps in cluster/schema/meta_class.go overwrites ALL
// FOUR property fields (IndexRangeFilters, IndexFilterable, IndexSearchable,
// and Tokenization when non-empty) from the incoming message, not just the
// one this strategy intends to change. If two strategies run concurrently on
// the same property, each could read a stale view of the schema and clobber
// the other's flag on RAFT apply. We cannot nil out the other flags because
// setPropertyDefaults would re-fill them with type-based defaults. So we
// re-read the class right before each per-property update to minimize the
// staleness window.
//
// TODO(fieldmask): the proper long-term fix is a fieldmask on UpdateProperty
// so only named fields are merged.
func (s *EnableFilterableStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	className := shard.Index().Config.ClassName.String()
	trueVal := true
	// Missing properties are tolerated: a property dropped between
	// scheduling and completion is the same outcome we'd want anyway.
	_, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, s.propNames,
		[]string{api.PropertyFieldIndexFilterable},
		func(prop *models.Property) bool {
			if prop.IndexFilterable != nil && *prop.IndexFilterable {
				return false // already enabled (possibly by a racing shard)
			}
			prop.IndexFilterable = &trueVal
			return true
		})
	return err
}
