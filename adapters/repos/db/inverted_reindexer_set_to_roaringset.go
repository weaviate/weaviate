//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type ShardInvertedReindexTaskSetToRoaringSet struct{}

func (t *ShardInvertedReindexTaskSetToRoaringSet) GetPropertiesToReindex(ctx context.Context,
	shard *Shard,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithIdleThreshold(time.Duration(shard.index.Config.MemtablesFlushIdleAfter) * time.Second),
	}

	for name, bucket := range shard.store.GetBucketsByName() {
		if bucket.Strategy() == lsmkv.StrategySetCollection &&
			bucket.DesiredStrategy() == lsmkv.StrategyRoaringSet {

			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)
			switch indexType {
			case IndexTypePropValue:
				reindexableProperties = append(reindexableProperties,
					ReindexableProperty{
						PropertyName:    propName,
						IndexType:       IndexTypePropValue,
						DesiredStrategy: lsmkv.StrategyRoaringSet,
						BucketOptions:   bucketOptions,
					},
					ReindexableProperty{
						PropertyName:    propName,
						IndexType:       IndexTypeHashPropValue,
						DesiredStrategy: lsmkv.StrategyReplace,
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
					ReindexableProperty{
						PropertyName:    propName,
						IndexType:       IndexTypeHashPropLength,
						DesiredStrategy: lsmkv.StrategyReplace,
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
					ReindexableProperty{
						PropertyName:    propName,
						IndexType:       IndexTypeHashPropNull,
						DesiredStrategy: lsmkv.StrategyReplace,
						BucketOptions:   bucketOptions,
					},
				)
			default:
				// skip remaining
			}
		}
	}

	return reindexableProperties, nil
}
