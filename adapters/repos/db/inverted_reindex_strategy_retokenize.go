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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// SearchableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// searchable (BM25) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD).
type SearchableRetokenizeStrategy struct {
	noAnalyzerOverlay
	propName           string
	targetTokenization string
	className          string
	bucketStrategy     string // StrategyMapCollection or StrategyInverted
	generation         int    // see genSuffix godoc for the per-migration generation contract
}

func (s *SearchableRetokenizeStrategy) SourceBucketName(_ string) string {
	return helpers.BucketSearchableFromPropNameLSM(s.propName)
}

func (s *SearchableRetokenizeStrategy) TargetStrategy() string {
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
		if err := shard.addToPropertyMapBucket(bucket, docID, item.Data, item.TermFrequency, propLen); err != nil {
			return fmt.Errorf("retokenize prop '%s': %w", prop.Name, err)
		}
	}
	return nil
}

func (s *SearchableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	// The analyzer is stateless once constructed (it carries only className
	// and a function pointer); hoist it out of the per-callback hot path so
	// we don't allocate a fresh struct on every Add to a reindexed prop.
	analyzer := inverted.NewAnalyzer(nil, s.className)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}

		var items []inverted.Countable
		if len(property.RawValues) > 0 {
			// Re-tokenize with the target tokenization for the new index.
			items = analyzer.TextArray(s.targetTokenization, property.RawValues, property.Name, nil)
		} else {
			// Use existing items (old tokenization) for the old index.
			items = property.Items
		}

		propLen := s.calcPropLen(items)
		for _, item := range items {
			if err := shard.addToPropertyMapBucket(bucket, docID, item.Data, item.TermFrequency, propLen); err != nil {
				return fmt.Errorf("retokenize add prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *SearchableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	// See the MakeAddCallback comment — same rationale: hoist the analyzer
	// out of the per-callback hot path.
	analyzer := inverted.NewAnalyzer(nil, s.className)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}

		var items []inverted.Countable
		if len(property.RawValues) > 0 {
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

func (s *SearchableRetokenizeStrategy) calcPropLen(items []inverted.Countable) float32 {
	if s.bucketStrategy == lsmkv.StrategyInverted {
		return calcPropLenInverted(items)
	}
	return calcPropLenMap(items)
}
