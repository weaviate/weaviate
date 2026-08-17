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
// Schema-flag gating. During the backfill scan, IndexRangeFilters is still
// false on the target property until the cluster-wide flip. Without
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
// It also pessimistically marks each migrated property as
// "not locally ready" on this shard. The query path consults this via
// [*Shard.IsRangeableLocallyReady] and falls back to the filterable
// bucket walk while the rangeable bucket is empty. See
// `Shard.rangeableLocalReady` for the full GH https://github.com/weaviate/0-weaviate-issues/issues/212
// Issue C rationale. The post-runtimeSwap finalize flips the prop back
// to "ready" after `markTidied()`.
func (s *FilterableToRangeableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		shard.setRangeableLocallyReady(propName, false)
		bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) != nil {
			continue
		}
		opts := shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)
		if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.index.logger.WithField("bucket", bucketName).
				Errorf("PreReindexHook: failed to create rangeable bucket: %v", err)
		}
	}
	ensureNullStateAndLengthBuckets(ctx, shard, props)
}

// ensureNullStateAndLengthBuckets creates the null-state / property-length
// buckets the migrated properties are about to need. A numeric property
// created with IndexFilterable=false carries no inverted index at all, so
// shard init skipped both buckets for it, and nothing creates them when a
// flag later flips. The first write to reach the property once it has a
// rangeable index would fail on the missing bucket.
//
// Best-effort and idempotent: a failure here is logged, and the write that
// would have used the bucket reports it far more loudly than this line does.
func ensureNullStateAndLengthBuckets(ctx context.Context, shard *Shard, props []string) {
	cfg := shard.index.invertedIndexConfig
	if !cfg.IndexNullState && !cfg.IndexPropertyLength {
		return
	}
	className := shard.index.Config.ClassName.String()
	class := shard.index.getSchema.ReadOnlyClass(className)
	if class == nil {
		return
	}
	byName := make(map[string]*models.Property, len(class.Properties))
	for _, prop := range class.Properties {
		byName[prop.Name] = prop
	}
	for _, propName := range props {
		prop, ok := byName[propName]
		if !ok {
			continue
		}
		if cfg.IndexNullState {
			if err := shard.createPropertyNullIndex(ctx, prop, shard.makeDefaultBucketOptions); err != nil {
				shard.index.logger.WithField("property", propName).
					Errorf("PreReindexHook: failed to create null-state bucket: %v", err)
			}
		}
		if cfg.IndexPropertyLength {
			if err := shard.createPropertyLengthIndex(ctx, prop, shard.makeDefaultBucketOptions); err != nil {
				shard.index.logger.WithField("property", propName).
					Errorf("PreReindexHook: failed to create property-length bucket: %v", err)
			}
		}
	}
}

// AnalyzerOverlay forces IndexRangeFilters=true on the targeted properties
// while the backfill iterator scans the objects bucket and while the
// double-write callbacks mirror live writes. Until the cluster-wide flip in
// [ReindexProvider.OnTaskCompleted], the analyzer would otherwise emit the
// property with HasRangeableIndex=false (and skip it entirely via
// HasAnyInvertedIndex when IndexFilterable is also false), leaving the new
// rangeable bucket empty — the silent-FINISHED data-loss failure mode
// pinned by the property-state matrix.
//
// This overlay ends with the double-write callbacks, at the end of
// runtimeSwap. The per-shard [Shard.rangeableWriteOverlay] takes over from
// there until the flip lands on this node.
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

// OnMigrationComplete marks each migrated property "locally ready" so this
// shard's query path stops falling back to the filterable bucket walk. See
// GH https://github.com/weaviate/0-weaviate-issues/issues/212 Issue C +
// Shard.rangeableLocalReady.
//
// It does NOT touch the schema. enable-rangeable is a semantic migration
// ([IsSemanticMigration]), so IndexRangeFilters is flipped once
// cluster-wide from [ReindexProvider.OnTaskCompleted] after every node has
// swapped. Flipping it here instead advertised range-query support from the
// first shard to reach this line, and a task that then stopped left the
// flag on over shards holding an empty bucket
// (weaviate/0-weaviate-issues#464). repair-rangeable reaches this line with
// the flag already true, so it has nothing to flip either.
//
// Unwrap before the assertion: a *LazyLoadShard wraps the concrete *Shard
// we need to flip the flag on. unwrapShard returns the concrete pointer for
// both *Shard and *LazyLoadShard.
func (s *FilterableToRangeableStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	concrete, err := unwrapShard(ctx, shard)
	if err != nil || concrete == nil {
		return nil
	}
	for _, propName := range s.propNames {
		concrete.setRangeableLocallyReady(propName, true)
	}
	return nil
}
