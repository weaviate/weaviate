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
	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
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
// false on the target property until OnMigrationComplete flips it. Without
// an AnalyzerOverlay forcing the rangeable flag on, the analyzer would
// either drop the property entirely (HasAnyInvertedIndex=false when the
// property is also IndexFilterable=false) or emit it with
// HasRangeableIndex=false. Either way the new rangeable bucket would end
// up empty and the task would still report FINISHED — the silent
// data-loss bug pinned by the int__filt=false_range=nil/false matrix
// cells.
type FilterableToRangeableStrategy struct {
	schemaManager *schema.Manager
	propNames     []string
	generation    int // see genSuffix godoc
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
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — the property may be
		// IndexFilterable=false, and we still need to populate the
		// rangeable bucket from the live write. Scope is enforced via
		// propsByName.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.AddToPropertyRangeBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding rangeable prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableToRangeableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — see MakeAddCallback.
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.DeleteFromPropertyRangeBucket(bucket, docID, item.Data); err != nil {
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
// [ShardLike.IsRangeableLocallyReady] and falls back to the filterable
// bucket walk while the rangeable bucket is empty. See
// `Shard.rangeableLocalReady` for the full GH https://github.com/weaviate/0-weaviate-issues/issues/212
// Issue C rationale. The post-runtimeSwap finalize flips the prop back
// to "ready" after `markTidied()`.
func (s *FilterableToRangeableStrategy) PreReindexHook(shard ShardLike, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		shard.SetRangeableLocallyReady(propName, false)
		bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
		if shard.Store().Bucket(bucketName) != nil {
			continue
		}
		opts := shard.MakeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)
		if err := shard.Store().CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
			shard.Index().Logger().WithField("bucket", bucketName).
				WithError(err).Error("PreReindexHook: failed to create rangeable bucket")
		}
	}
}

// AnalyzerOverlay forces IndexRangeFilters=true on the targeted properties
// while the backfill iterator scans the objects bucket. Until
// OnMigrationComplete flips the RAFT-stored schema flag, the analyzer would
// otherwise emit the property with HasRangeableIndex=false (and skip it
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
	// Mark each prop as "locally ready" so the query path stops falling
	// back to the filterable bucket for this shard. This MUST happen
	// before the schema-flag flip below — once the schema flip RAFTs
	// cluster-wide, other replicas may also be at this same point or
	// just-about-to-swap, but the per-shard ready flag controls THIS
	// shard's behavior in isolation. Set it before the schema update so
	// THIS shard's queries that observe the new schema flag also see
	// ready=true. See GH https://github.com/weaviate/0-weaviate-issues/issues/212 Issue C +
	// Shard.rangeableLocalReady.
	//
	// Unwrap before the assertion: a *LazyLoadShard wraps the concrete
	// ShardLike we need to flip the flag on. unwrapShard returns the
	// concrete pointer for both ShardLike and *LazyLoadShard.
	if concrete, err := unwrapShard(ctx, shard); err == nil && concrete != nil {
		for _, propName := range s.propNames {
			concrete.SetRangeableLocallyReady(propName, true)
		}
	}

	className := shard.Index().ClassName().String()
	trueVal := true
	// Missing properties are tolerated: a property dropped between
	// scheduling and completion is the same outcome we'd want anyway.
	_, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, s.propNames,
		[]string{api.PropertyFieldIndexRangeFilters},
		func(prop *models.Property) bool {
			if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				return false // already enabled
			}
			prop.IndexRangeFilters = &trueVal
			return true
		})
	return err
}
