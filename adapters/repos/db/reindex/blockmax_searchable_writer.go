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
		pair := shard.PairPropertyWithFrequency(docID, item.TermFrequency, propLen)
		if err := shard.AddToPropertyMapBucket(bucket, pair, item.Data); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

func blockmaxSearchableAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{},
) OnAddToPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}
		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		propLen := calcPropLenInverted(property.Items)
		for _, item := range property.Items {
			pair := shard.PairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := shard.AddToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func blockmaxSearchableDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{},
) OnDeleteFromPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}
		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.DeleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}
