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

// RoaringSetRefreshStrategy implements MigrationStrategy for a same-strategy
// reindex of filterable (RoaringSet) properties. This rebuilds the filterable
// index from the objects bucket without changing the storage format, useful for
// corruption recovery.
type RoaringSetRefreshStrategy struct {
	noAnalyzerOverlay
	generation int // see genSuffix godoc
}

func (s *RoaringSetRefreshStrategy) SourceBucketName(propName string) string {
	return helpers.BucketFromPropNameLSM(propName)
}

func (s *RoaringSetRefreshStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *RoaringSetRefreshStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	for _, item := range prop.Items {
		if err := bucket.RoaringSetAddOne(item.Data, docID); err != nil {
			return fmt.Errorf("adding prop '%s': %w", item.Data, err)
		}
	}
	return nil
}

func (s *RoaringSetRefreshStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		for _, item := range property.Items {
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RoaringSetRefreshStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		for _, item := range property.Items {
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}
