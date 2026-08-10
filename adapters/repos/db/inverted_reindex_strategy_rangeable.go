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
)

// FilterableToRangeableStrategy implements MigrationStrategy for building
// RoaringSetRange (rangeable) indexes. The strategy is used both as
// "enable-rangeable" (the property currently has no rangeable index) and as
// "repair-rangeable" (the rangeable index already exists and is being
// refreshed).
//
// Backfill source. The reindex iterator scans the objects bucket and runs
// the inverted analyzer to derive per-property values. It does NOT read
// from the filterable bucket — that bucket may not even exist (e.g. a
// numeric property created with IndexFilterable=false explicitly). The
// "FilterableToRangeable" name is historical; treat it as "build rangeable
// from objects".
//
// Schema-flag gating. The two migration types differ here. Under
// enable-rangeable IndexRangeFilters stays false for the whole run: the
// property is not yet enabled and the cluster-wide flip happens once, at
// task completion, in ReindexProvider.flipSemanticMigrationSchema. Under
// repair-rangeable the flag is already true at submit and never changes —
// repair alters no schema at all. The overlay below is written for the
// enable case and is a harmless no-op for repair. Without
// an AnalyzerOverlay forcing the rangeable flag on, the analyzer would
// either drop the property entirely (HasAnyInvertedIndex=false when the
// property is also IndexFilterable=false) or emit it with
// HasRangeableIndex=false. Either way the new rangeable bucket would end
// up empty and the task would still report FINISHED — the silent
// data-loss bug pinned by the int__filt=false_range=nil/false matrix
// cells.
type FilterableToRangeableStrategy struct {
	propNames  []string
	generation int // see genSuffix godoc
}

func (s *FilterableToRangeableStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, s.propNames) + genSuffix(s.generation)
}

func (s *FilterableToRangeableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketRangeableFromPropNameLSM(propName)
}

func (s *FilterableToRangeableStrategy) ReindexSuffix() string {
	return "__rangeable_reindex" + genSuffix(s.generation)
}

func (s *FilterableToRangeableStrategy) IngestSuffix() string {
	return "__rangeable_ingest" + genSuffix(s.generation)
}

func (s *FilterableToRangeableStrategy) BackupSuffix() string {
	return "__rangeable_backup" + genSuffix(s.generation)
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

// ShouldProcessProperty always returns true. Scope is driven by the
// reindexTaskConfig.selectedPropsByCollection set in the task constructor,
// not by the live schema flag — during this migration IndexRangeFilters
// is still false on every targeted property, and IndexFilterable may also
// be false (the data is rebuilt from the objects bucket, not from
// filterable).
func (s *FilterableToRangeableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *FilterableToRangeableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — the property may be
		// IndexFilterable=false, and we still need to populate the
		// rangeable bucket from the live write. Scope is enforced via
		// propsByName.
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			propsByName, bucketNamer, s.SourceBucketName, forTargetStrategy)
		if skip {
			return nil
		}
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
		// Don't gate on HasFilterableIndex — see MakeAddCallback.
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			propsByName, bucketNamer, s.SourceBucketName, forTargetStrategy)
		if skip {
			return nil
		}
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
//
// Under enable-rangeable the empty bucket is unreachable by queries while
// the migration runs: IndexRangeFilters stays false until the task
// completes cluster-wide, so the query planner takes the filterable walk.
// Under repair-rangeable the skip-if-exists below keeps whatever bucket
// is already there, which on the shards repair exists for is the empty
// one that is the damage. Those shards return zero rows for range
// filters until the atomic swap lands, exactly as they did before the
// repair started. On a shard whose bucket is populated the existing data
// keeps serving until the swap, and if the swap never lands the backup
// dir is what holds it — nothing in this hook protects it.
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

// AnalyzerOverlay forces IndexRangeFilters=true on the targeted properties
// while the backfill iterator scans the objects bucket. Until
// flipSemanticMigrationSchema flips the RAFT-stored schema flag, the analyzer
// would otherwise emit the property with HasRangeableIndex=false (and skip it
// entirely via HasAnyInvertedIndex when IndexFilterable is also false),
// leaving the new rangeable bucket empty — the silent-FINISHED data-loss
// failure mode pinned by the property-state matrix.
func (s *FilterableToRangeableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]inverted.PropertyOverlay, len(props))
	for _, p := range props {
		out[p] = inverted.PropertyOverlay{ForceRangeable: true}
	}
	return out
}

// OnMigrationComplete is a no-op for both migration types this strategy
// serves. enable-rangeable is semantic: its IndexRangeFilters=true flip
// happens once cluster-wide at task completion, in
// [ReindexProvider.flipSemanticMigrationSchema], after every node's local
// swap has committed. repair-rangeable has no schema change to make at
// all.
func (s *FilterableToRangeableStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
