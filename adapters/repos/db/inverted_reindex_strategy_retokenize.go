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

// SearchableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// searchable (BM25) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD).
//
// Historically the schema's Tokenization flip was deferred to the sibling
// FilterableRetokenize strategy (which runs second for the dual-index
// ChangeTokenization migration) so the update happened exactly once. With
// the createReindexTasks defense that skips the filterable sub-task when the
// property has no filterable bucket (IndexFilterable=false), the searchable
// strategy is the only one that runs, and the schema flip MUST happen here
// — otherwise the searchable bucket gets re-tokenized but the schema still
// reports the old tokenization, queries are tokenized against the old
// schema while the bucket holds the new tokens, and BM25 returns 0 hits
// (the failure shape pinned by the property_state_migration_matrix
// dt=text__filt=false_srch=*_PUT_searchable_tokenization_field cells).
//
// applyPerPropertySchemaUpdate is idempotent (no-op when the schema already
// reports the target tokenization), so it's safe to call from both
// strategies when both run.
type SearchableRetokenizeStrategy struct {
	noAnalyzerOverlay
	schemaManager      *schema.Manager
	propName           string
	targetTokenization string
	className          string
	bucketStrategy     string // StrategyMapCollection or StrategyInverted
}

func (s *SearchableRetokenizeStrategy) MigrationDirName() string {
	// Include property name in the dir so concurrent retokenize tasks on
	// different properties don't share tracker state.
	return MigrationDirPrefixSearchableRetokenize + "_" + s.propName
}

func (s *SearchableRetokenizeStrategy) SourceBucketName(_ string) string {
	return helpers.BucketSearchableFromPropNameLSM(s.propName)
}

func (s *SearchableRetokenizeStrategy) ReindexSuffix() string {
	return "__retokenize_reindex"
}

func (s *SearchableRetokenizeStrategy) IngestSuffix() string {
	return "__retokenize_ingest"
}

func (s *SearchableRetokenizeStrategy) BackupSuffix() string {
	return "__retokenize_backup"
}

func (s *SearchableRetokenizeStrategy) SourceStrategy() string {
	return s.bucketStrategy
}

func (s *SearchableRetokenizeStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *SearchableRetokenizeStrategy) TargetStrategy() string {
	return s.bucketStrategy
}

func (s *SearchableRetokenizeStrategy) BackupStrategy() string {
	return s.bucketStrategy
}

func (s *SearchableRetokenizeStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if len(prop.RawValues) == 0 {
		return nil
	}

	analyzer := inverted.NewAnalyzer(nil, s.className)
	items := analyzer.TextArray(s.targetTokenization, prop.RawValues, prop.Name, nil)
	propLen := s.calcPropLen(items)

	for _, item := range items {
		pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
		if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
			return fmt.Errorf("retokenize prop '%s': %w", prop.Name, err)
		}
	}
	return nil
}

func (s *SearchableRetokenizeStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasSearchableIndex && property.Name == s.propName
}

// MakeAddCallback returns a callback for adding documents to the searchable index.
// forTargetStrategy controls which tokenization is used: true uses the new target
// tokenization (for the reindex bucket), false uses the existing tokenization
// (for the ingest/double-write bucket that must match the currently live index).
func (s *SearchableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	// The analyzer is stateless once constructed (it carries only className
	// and a function pointer); hoist it out of the per-callback hot path so
	// we don't allocate a fresh struct on every Add to a reindexed prop.
	// Skip the allocation entirely when forTargetStrategy is false — the
	// closure won't touch the analyzer in that branch.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			// Re-tokenize with the target tokenization for the new index.
			items = analyzer.TextArray(s.targetTokenization, property.RawValues, property.Name, nil)
		} else {
			// Use existing items (old tokenization) for the old index.
			items = property.Items
		}

		propLen := s.calcPropLen(items)
		for _, item := range items {
			pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("retokenize add prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// MakeDeleteCallback returns a callback for removing documents from the searchable index.
// forTargetStrategy has the same semantics as in MakeAddCallback.
func (s *SearchableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	// See the MakeAddCallback comment — same rationale: hoist the analyzer
	// out of the per-callback hot path.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			items = analyzer.TextArray(s.targetTokenization, property.RawValues, property.Name, nil)
		} else {
			items = property.Items
		}

		for _, item := range items {
			if err := shard.deleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("retokenize delete prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *SearchableRetokenizeStrategy) PreReindexHook(_ *Shard, _ []string) {
	// No-op: the searchable bucket already exists.
}

// OnMigrationComplete flips the schema's Tokenization for this property.
// Mirrors the equivalent update in FilterableRetokenizeStrategy so that
// ChangeTokenization migrations on properties with NO filterable index
// (where createReindexTasks skips the filterable sub-task) still update
// the schema. Both strategies use applyPerPropertySchemaUpdate, which
// is idempotent: when both sub-tasks run, the second call is a no-op
// because the schema already reports the target tokenization.
func (s *SearchableRetokenizeStrategy) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	if s.schemaManager == nil {
		// Defensive: legacy call sites that constructed the strategy
		// without a schema manager (mostly tests) skip the schema flip;
		// the bucket has already been re-tokenized by runtimeSwap.
		return nil
	}
	className := shard.Index().Config.ClassName.String()
	missing, err := applyPerPropertySchemaUpdate(ctx, s.schemaManager, className, []string{s.propName},
		[]string{api.PropertyFieldTokenization},
		func(prop *models.Property) bool {
			if prop.Tokenization == s.targetTokenization {
				return false // already correct (sibling strategy flipped it)
			}
			prop.Tokenization = s.targetTokenization
			return true
		})
	if err != nil {
		return err
	}
	// Single-property strategy: a missing target property is a hard error.
	if len(missing) > 0 {
		return fmt.Errorf("property %q not found in class %q", s.propName, className)
	}
	return nil
}

func (s *SearchableRetokenizeStrategy) calcPropLen(items []inverted.Countable) float32 {
	if s.bucketStrategy == lsmkv.StrategyInverted {
		return calcPropLenInverted(items)
	}
	return calcPropLenMap(items)
}
