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
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// Test-only export: relocation follow-up tracked separately; no new external callers.
//
// RoaringSetRefreshStrategy implements MigrationStrategy for a same-strategy
// reindex of filterable (RoaringSet) properties. This rebuilds the filterable
// index from the objects bucket without changing the storage format, useful for
// corruption recovery.
type RoaringSetRefreshStrategy struct {
	noAnalyzerOverlay
	Generation int // see GenSuffix godoc
}

func (s *RoaringSetRefreshStrategy) MigrationDirName() string {
	return MigrationDirFilterableRoaringsetRefresh + GenSuffix(s.Generation)
}

func (s *RoaringSetRefreshStrategy) SourceBucketName(propName string) string {
	return helpers.BucketFromPropNameLSM(propName)
}

func (s *RoaringSetRefreshStrategy) ReindexSuffix() string {
	return "__roaringset_reindex" + GenSuffix(s.Generation)
}

func (s *RoaringSetRefreshStrategy) IngestSuffix() string {
	return "__roaringset_ingest" + GenSuffix(s.Generation)
}

func (s *RoaringSetRefreshStrategy) BackupSuffix() string {
	return "__roaringset_backup" + GenSuffix(s.Generation)
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
) OnAddToPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.AddToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RoaringSetRefreshStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) OnDeleteFromPropertyValueIndex {
	return func(shard ShardLike, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.Store().Bucket(bucketName)
		for _, item := range property.Items {
			if err := shard.DeleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *RoaringSetRefreshStrategy) PreReindexHook(shard ShardLike, props []string) {
	// No-op: no property marking needed for same-strategy refresh.
}

func (s *RoaringSetRefreshStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	// No-op: no schema update needed for same-strategy refresh.
	return nil
}
