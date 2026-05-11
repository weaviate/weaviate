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
	"strings"

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
	schemaManager *schema.Manager
	propNames     []string
}

func (s *FilterableToRangeableStrategy) MigrationDirName() string {
	// Include property names in the dir so multiple per-property tasks
	// on the same shard don't share tracker state.
	if len(s.propNames) > 0 {
		return "filterable_to_rangeable_" + strings.Join(s.propNames, "_")
	}
	return "filterable_to_rangeable"
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

// AnalyzerOverlay returns nil: the source data here is the *existing*
// filterable bucket whose IndexFilterable flag is already true in the live
// schema, so the analyzer sees the property without help.
func (s *FilterableToRangeableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	return nil
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

	propSet := make(map[string]struct{}, len(s.propNames))
	for _, p := range s.propNames {
		propSet[p] = struct{}{}
	}

	trueVal := true
	for propName := range propSet {
		// Re-read the class right before each property update to minimize the
		// staleness window where a concurrent strategy could clobber our flag.
		class := s.schemaManager.ReadOnlyClass(className)
		if class == nil {
			return fmt.Errorf("class %q not found", className)
		}

		var prop *models.Property
		for _, p := range class.Properties {
			if p.Name == propName {
				prop = p
				break
			}
		}
		if prop == nil {
			continue
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			continue // already enabled
		}
		// Shallow-copy to avoid mutating the in-memory schema before the RAFT
		// entry is committed. We carry through the freshly read values of
		// IndexFilterable / IndexSearchable / Tokenization, only changing
		// IndexRangeFilters here.
		updated := *prop
		updated.IndexRangeFilters = &trueVal
		if err := schema.UpdatePropertyInternal(&s.schemaManager.Handler, ctx, className, &updated); err != nil {
			return fmt.Errorf("updating property %q IndexRangeFilters: %w", propName, err)
		}
	}
	return nil
}
