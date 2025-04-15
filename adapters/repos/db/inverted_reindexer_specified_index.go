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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

type ShardInvertedReindexTask_SpecifiedIndex struct {
	classNamesWithPropertyNames map[string]map[string]struct{}
}

func (t *ShardInvertedReindexTask_SpecifiedIndex) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	// shard of selected class
	props, ok := t.classNamesWithPropertyNames[shard.Index().Config.ClassName.String()]
	if !ok {
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
		if _, ok := props[propName]; !ok {
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
		case IndexTypePropMetaCount:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropMetaCount,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		default:
			// skip remaining
		}

	}

	return reindexableProperties, nil
}

func (t *ShardInvertedReindexTask_SpecifiedIndex) OnPostResumeStore(ctx context.Context, shard ShardLike) error {
	return nil
}

func (t *ShardInvertedReindexTask_SpecifiedIndex) ObjectsIterator(shard ShardLike) objectsIterator {
	class := shard.Index().Config.ClassName.String()
	props, ok := t.classNamesWithPropertyNames[class]
	if !ok || len(props) == 0 {
		return nil
	}

	propertyPaths := make([][]string, 0, len(props))
	for prop := range props {
		propertyPaths = append(propertyPaths, []string{prop})
	}

	propsExtraction := &storobj.PropertyExtraction{
		PropertyPaths: propertyPaths,
	}

	objectsBucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	return func(ctx context.Context, fn func(object *storobj.Object) error) error {
		cursor := objectsBucket.Cursor()
		defer cursor.Close()

		i := 0
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			obj, err := storobj.FromBinaryOptional(v, additional.Properties{}, propsExtraction)
			if err != nil {
				return fmt.Errorf("cannot unmarshal object %d, %w", i, err)
			}
			if err := fn(obj); err != nil {
				return fmt.Errorf("callback on object '%d' failed: %w", obj.DocID, err)
			}
			i++
		}
		return nil
	}
}
