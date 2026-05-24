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
	propNames  []string
	generation int // see genSuffix godoc
}

func (s *EnableFilterableStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixEnableFilterable, s.propNames) + genSuffix(s.generation)
}

func (s *EnableFilterableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketFromPropNameLSM(propName)
}

func (s *EnableFilterableStrategy) ReindexSuffix() string {
	return "__enable_filterable_reindex" + genSuffix(s.generation)
}

func (s *EnableFilterableStrategy) IngestSuffix() string {
	return "__enable_filterable_ingest" + genSuffix(s.generation)
}

func (s *EnableFilterableStrategy) BackupSuffix() string {
	return "__enable_filterable_backup" + genSuffix(s.generation)
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
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — it's false on the target
		// property until OnMigrationComplete flips it.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.AddToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *EnableFilterableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.DeleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// PreReindexHook creates empty filterable (RoaringSet) buckets for the
// targeted properties so the generic state machine has a "source" bucket
// to swap with the populated ingest bucket.
func (s *EnableFilterableStrategy) PreReindexHook(shard ShardLike, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		bucketName := helpers.BucketFromPropNameLSM(propName)
		if shard.Store().Bucket(bucketName) != nil {
			continue
		}
		opts := shard.MakeDefaultBucketOptions(lsmkv.StrategyRoaringSet)
		if err := shard.Store().CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.ParentIndex().Logger().WithField("bucket", bucketName).
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

// OnMigrationComplete is a no-op for this semantic migration. The schema
// cutover (IndexFilterable=true flip via RAFT) now happens once
// cluster-wide from [ReindexProvider.OnTaskCompleted] after every node's
// local OnGroupCompleted has run the bucket pointer swap. See the
// Journey 3 canonical pattern in cluster/distributedtask/doc.go:111-137.
//
// Per-shard schema flips would re-introduce the first-shard-flips
// problem: the first shard on the first node to call RunSwapOnShard
// would flip the cluster-wide IndexFilterable flag while other nodes /
// other shards still serve the old (filterable-disabled) state.
func (s *EnableFilterableStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
