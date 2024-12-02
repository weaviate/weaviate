//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type ShardInvertedReindexTask_BrokenIndex struct{}

func (t *ShardInvertedReindexTask_BrokenIndex) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}
	defer func() {
		fmt.Printf("  ==> shard className [%s]\n", shard.Index().Config.ClassName.String())
		fmt.Printf("  ==> reindexable props found %+v\n\n", reindexableProperties)
	}()

	// shard of selected class
	if _, ok := t.getClassNames()[shard.Index().Config.ClassName.String()]; !ok {
		return reindexableProperties, nil
	}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter) * time.Second),
	}

	for name := range shard.Store().GetBucketsByName() {
		// skip non prop buckets
		switch name {
		case helpers.ObjectsBucketLSM:
		case helpers.VectorsBucketLSM:
		case helpers.VectorsCompressedBucketLSM:
		case helpers.DimensionsBucketLSM:
			continue
		}

		propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)
		if _, ok := t.getPropertyNames()[propName]; !ok {
			continue
		}

		switch indexType {
		case IndexTypePropValue:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropValue,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropSearchableValue:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropSearchableValue,
					DesiredStrategy: lsmkv.StrategyMapCollection,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropLength:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropLength,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropNull:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropNull,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		// case IndexTypePropMetaCount:
		// 	reindexableProperties = append(reindexableProperties,
		// 		ReindexableProperty{
		// 			PropertyName:    propName,
		// 			IndexType:       IndexTypePropMetaCount,
		// 			DesiredStrategy: lsmkv.StrategyRoaringSet,
		// 			BucketOptions:   bucketOptions,
		// 		},
		// 	)
		default:
			// skip remaining
		}

	}

	return reindexableProperties, nil
}

func (t *ShardInvertedReindexTask_BrokenIndex) OnPostResumeStore(ctx context.Context, shard ShardLike) error {
	return nil
}

func (t *ShardInvertedReindexTask_BrokenIndex) getClassNames() map[string]struct{} {
	return map[string]struct{}{"City": {}}
}

func (t *ShardInvertedReindexTask_BrokenIndex) getPropertyNames() map[string]struct{} {
	return map[string]struct{}{"inCountry": {}}
}
