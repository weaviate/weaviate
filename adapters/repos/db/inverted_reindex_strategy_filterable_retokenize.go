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

// FilterableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// filterable (RoaringSet) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD).
type FilterableRetokenizeStrategy struct {
	noAnalyzerOverlay
	propName           string
	targetTokenization string
	className          string
	generation         int // see genSuffix godoc
}

func (s *FilterableRetokenizeStrategy) SourceBucketName(_ string) string {
	return helpers.BucketFromPropNameLSM(s.propName)
}

func (s *FilterableRetokenizeStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) WriteToReindexBucket(_ ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if len(prop.RawValues) == 0 {
		return nil
	}

	analyzer := inverted.NewAnalyzer(nil, s.className)
	items := analyzer.TextArray(s.targetTokenization, prop.RawValues, prop.Name, nil)

	for _, item := range items {
		if err := bucket.RoaringSetAddOne(item.Data, docID); err != nil {
			return fmt.Errorf("filterable retokenize prop '%s': %w", prop.Name, err)
		}
	}
	return nil
}

func (s *FilterableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	analyzer := inverted.NewAnalyzer(nil, s.className)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
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
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize add prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	analyzer := inverted.NewAnalyzer(nil, s.className)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
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
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize delete prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}
