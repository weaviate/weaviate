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

// RoaringSetRefreshStrategy implements MigrationStrategy for a same-strategy
// reindex of filterable (RoaringSet) properties. This rebuilds the filterable
// index from the objects bucket without changing the storage format, useful for
// corruption recovery.
type RoaringSetRefreshStrategy struct{}

func (s *RoaringSetRefreshStrategy) MigrationDirName() string {
	return "filterable_roaringset_refresh"
}

func (s *RoaringSetRefreshStrategy) SourceBucketName(propName string) string {
	return helpers.BucketFromPropNameLSM(propName)
}

func (s *RoaringSetRefreshStrategy) ReindexSuffix() string {
	return "__roaringset_reindex"
}

func (s *RoaringSetRefreshStrategy) IngestSuffix() string {
	return "__roaringset_ingest"
}

func (s *RoaringSetRefreshStrategy) BackupSuffix() string {
	return "__roaringset_backup"
}

func (s *RoaringSetRefreshStrategy) SourceStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *RoaringSetRefreshStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropValue
}

func (s *RoaringSetRefreshStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *RoaringSetRefreshStrategy) BackupStrategy() string {
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

func (s *RoaringSetRefreshStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasFilterableIndex
}

func (s *RoaringSetRefreshStrategy) MakeAddCallback(bucketNamer func(string) string,
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
		for _, item := range property.Items {
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RoaringSetRefreshStrategy) MakeDeleteCallback(bucketNamer func(string) string,
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
		for _, item := range property.Items {
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RoaringSetRefreshStrategy) PreReindexHook(shard *Shard, props []string) {
	// No-op: no property marking needed for same-strategy refresh.
}

func (s *RoaringSetRefreshStrategy) OnMigrationComplete(ctx context.Context, className string) error {
	// No-op: no schema update needed for same-strategy refresh.
	return nil
}
