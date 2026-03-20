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
)

// schemaManager field kept for future use but OnMigrationComplete is a no-op
// for searchable — the filterable strategy (which runs second) updates the schema.

// SearchableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// searchable (BM25) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD).
type SearchableRetokenizeStrategy struct {
	propName           string
	targetTokenization string
	className          string
	bucketStrategy     string // StrategyMapCollection or StrategyInverted
}

func (s *SearchableRetokenizeStrategy) MigrationDirName() string {
	// Include property name in the dir so concurrent retokenize tasks on
	// different properties don't share tracker state.
	return "searchable_retokenize_" + s.propName
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
	items := analyzer.TextArray(s.targetTokenization, prop.RawValues)
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
	calcPropLen := calcPropLenMap
	if s.bucketStrategy == lsmkv.StrategyInverted {
		calcPropLen = calcPropLenInverted
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
			analyzer := inverted.NewAnalyzer(nil, s.className)
			items = analyzer.TextArray(s.targetTokenization, property.RawValues)
		} else {
			// Use existing items (old tokenization) for the old index.
			items = property.Items
		}

		propLen := calcPropLen(items)
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
			analyzer := inverted.NewAnalyzer(nil, s.className)
			items = analyzer.TextArray(s.targetTokenization, property.RawValues)
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

// OnMigrationComplete is a no-op for the searchable strategy. The schema
// update happens in the filterable strategy which runs after this one.
func (s *SearchableRetokenizeStrategy) OnMigrationComplete(_ context.Context, _ string) error {
	return nil
}

func (s *SearchableRetokenizeStrategy) calcPropLen(items []inverted.Countable) float32 {
	if s.bucketStrategy == lsmkv.StrategyInverted {
		return calcPropLenInverted(items)
	}
	return calcPropLenMap(items)
}
