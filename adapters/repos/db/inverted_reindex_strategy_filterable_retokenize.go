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

// FilterableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// filterable (RoaringSet) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD). This runs after the searchable strategy and
// performs the schema update in OnMigrationComplete.
type FilterableRetokenizeStrategy struct {
	noAnalyzerOverlay
	schemaManager      *schema.Manager
	propName           string
	targetTokenization string
	className          string
}

func (s *FilterableRetokenizeStrategy) MigrationDirName() string {
	// Include property name in the dir so concurrent retokenize tasks on
	// different properties don't share tracker state.
	return MigrationDirPrefixFilterableRetokenize + "_" + s.propName
}

func (s *FilterableRetokenizeStrategy) SourceBucketName(_ string) string {
	return helpers.BucketFromPropNameLSM(s.propName)
}

func (s *FilterableRetokenizeStrategy) ReindexSuffix() string {
	return "__filt_retokenize_reindex"
}

func (s *FilterableRetokenizeStrategy) IngestSuffix() string {
	return "__filt_retokenize_ingest"
}

func (s *FilterableRetokenizeStrategy) BackupSuffix() string {
	return "__filt_retokenize_backup"
}

func (s *FilterableRetokenizeStrategy) SourceStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropValue
}

func (s *FilterableRetokenizeStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) BackupStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) WriteToReindexBucket(_ ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if len(prop.RawValues) == 0 {
		return nil
	}

	analyzer := inverted.NewAnalyzer(nil, s.className)
	items := analyzer.TextArray(s.targetTokenization, prop.RawValues)

	for _, item := range items {
		if err := bucket.RoaringSetAddOne(item.Data, docID); err != nil {
			return fmt.Errorf("filterable retokenize prop '%s': %w", prop.Name, err)
		}
	}
	return nil
}

func (s *FilterableRetokenizeStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasFilterableIndex && property.Name == s.propName
}

// MakeAddCallback returns a callback for adding documents to the filterable (RoaringSet) index.
// forTargetStrategy controls which tokenization is used: true uses the new target
// tokenization (for the reindex bucket), false uses the existing tokenization
// (for the ingest/double-write bucket that must match the currently live index).
func (s *FilterableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			items = analyzer.TextArray(s.targetTokenization, property.RawValues)
		} else {
			items = property.Items
		}

		for _, item := range items {
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize add prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// MakeDeleteCallback returns a callback for removing documents from the filterable index.
// forTargetStrategy has the same semantics as in MakeAddCallback.
func (s *FilterableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			items = analyzer.TextArray(s.targetTokenization, property.RawValues)
		} else {
			items = property.Items
		}

		for _, item := range items {
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize delete prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableRetokenizeStrategy) PreReindexHook(_ *Shard, _ []string) {
	// No-op: the filterable bucket already exists.
}

// OnMigrationComplete updates the schema to set Tokenization to the target
// value. This runs after both searchable and filterable retokenization are done.
//
// Concurrency note: MergeProps in cluster/schema/meta_class.go overwrites ALL
// FOUR property fields (IndexRangeFilters, IndexFilterable, IndexSearchable,
// and Tokenization when non-empty) from the incoming message, not just the
// one this strategy intends to change. If two strategies run concurrently
// on the same property, each could read a stale view of the schema and
// clobber the other's flag on RAFT apply. We cannot nil out the other flags
// because setPropertyDefaults would re-fill them with type-based defaults.
// So we re-read the class right before constructing the update message to
// minimize the staleness window.
//
// TODO(fieldmask): the proper long-term fix is a fieldmask on UpdateProperty
// so only named fields are merged.
func (s *FilterableRetokenizeStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	className := shard.Index().Config.ClassName.String()
	missing, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, []string{s.propName},
		[]string{api.PropertyFieldTokenization},
		func(prop *models.Property) bool {
			if prop.Tokenization == s.targetTokenization {
				return false // already correct
			}
			prop.Tokenization = s.targetTokenization
			return true
		})
	if err != nil {
		return err
	}
	// Single-property strategy: a missing target property is a hard error
	// (matches the pre-helper behavior). The retokenize migration was
	// specifically about this one property, so if it's gone we cannot
	// claim the migration completed.
	if len(missing) > 0 {
		return fmt.Errorf("property %q not found in class %q", s.propName, className)
	}
	return nil
}
