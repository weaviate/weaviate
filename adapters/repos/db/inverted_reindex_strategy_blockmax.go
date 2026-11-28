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

// MapToBlockmaxStrategy implements MigrationStrategy for the
// MapCollection → Inverted (blockmax WAND) migration of searchable properties.
type MapToBlockmaxStrategy struct {
	noAnalyzerOverlay
	generation int // see genSuffix godoc
}

func (s *MapToBlockmaxStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *MapToBlockmaxStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *MapToBlockmaxStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	propLen := calcPropLenInverted(prop.Items)
	for _, item := range prop.Items {
		if err := shard.addToPropertyMapBucket(bucket, docID, item.Data, item.TermFrequency, propLen); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

func (s *MapToBlockmaxStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		propLen := calcPropLenInverted(property.Items)
		for _, item := range property.Items {
			if err := shard.addToPropertyMapBucket(bucket, docID, item.Data, item.TermFrequency, propLen); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *MapToBlockmaxStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		for _, item := range property.Items {
			if err := shard.deleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// calcPropLenInverted computes property length as the sum of term frequencies.
func calcPropLenInverted(items []inverted.Countable) float32 {
	propLen := float32(0)
	for _, item := range items {
		propLen += item.TermFrequency
	}
	return propLen
}

// calcPropLenMap computes property length as the count of items.
func calcPropLenMap(items []inverted.Countable) float32 {
	return float32(len(items))
}
