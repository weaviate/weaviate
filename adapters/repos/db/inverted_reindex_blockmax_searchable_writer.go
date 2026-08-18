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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// Shared BlockMax (StrategyInverted) searchable-bucket writers. Used by
// both EnableSearchableStrategy (creating from scratch) and
// RebuildSearchableStrategy (rebuilding from objects). The write side is
// identical — same posting layout, same proplen calculation — so both
// strategies delegate here.

func writeBlockmaxSearchablePostings(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	propLen := calcPropLenInverted(prop.Items)
	for _, item := range prop.Items {
		pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
		if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

// blockmaxSearchableAddCallback is the add-leg double-write callback
// shared by every strategy whose target bucket is a BlockMax searchable
// index (enable-searchable, rebuild-searchable).
func blockmaxSearchableAddCallback(scope doubleWriteScope) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property, scope)
		if skip {
			return nil
		}
		propLen := calcPropLenInverted(property.Items)
		for _, item := range property.Items {
			pair := shard.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// blockmaxSearchableDeleteCallback is the delete-leg counterpart of
// [blockmaxSearchableAddCallback].
func blockmaxSearchableDeleteCallback(scope doubleWriteScope) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property, scope)
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
