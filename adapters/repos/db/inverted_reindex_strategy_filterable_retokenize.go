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
	"github.com/weaviate/weaviate/usecases/schema"
)

// FilterableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// filterable (RoaringSet) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD). This runs after the searchable strategy and
// performs the schema update in OnMigrationComplete.
type FilterableRetokenizeStrategy struct {
	schemaManager      *schema.Manager
	propName           string
	targetTokenization string
	className          string
}

func (s *FilterableRetokenizeStrategy) MigrationDirName() string {
	return "filterable_retokenize"
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

func (s *FilterableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
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

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			analyzer := inverted.NewAnalyzer(nil, s.className)
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

func (s *FilterableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
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

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			analyzer := inverted.NewAnalyzer(nil, s.className)
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
func (s *FilterableRetokenizeStrategy) OnMigrationComplete(ctx context.Context, className string) error {
	class := s.schemaManager.ReadOnlyClass(className)
	if class == nil {
		return fmt.Errorf("class %q not found", className)
	}

	for _, prop := range class.Properties {
		if prop.Name != s.propName {
			continue
		}
		if prop.Tokenization == s.targetTokenization {
			return nil // already correct
		}
		updated := *prop
		updated.Tokenization = s.targetTokenization
		if err := schema.UpdatePropertyInternal(&s.schemaManager.Handler, ctx, className, &updated); err != nil {
			return fmt.Errorf("updating property %q Tokenization: %w", prop.Name, err)
		}
		return nil
	}
	return fmt.Errorf("property %q not found in class %q", s.propName, className)
}
