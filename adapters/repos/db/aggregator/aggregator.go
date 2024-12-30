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

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type vectorIndex interface {
	SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32, maxLimit int64,
		allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, targetDistance float32,
		maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
}

type Aggregator struct {
	logger                 logrus.FieldLogger
	store                  *lsmkv.Store
	params                 aggregation.Params
	getSchema              schemaUC.SchemaGetter
	classSearcher          inverted.ClassSearcher // to support ref-filters
	vectorIndex            vectorIndex
	stopwords              stopwords.StopwordDetector
	shardVersion           uint16
	propLenTracker         *inverted.JsonShardMetaData
	isFallbackToSearchable inverted.IsFallbackToSearchable
	tenant                 string
	nestedCrossRefLimit    int64
	bitmapFactory          *roaringset.BitmapFactory
	modules                *modules.Provider
}

func New(store *lsmkv.Store, params aggregation.Params,
	getSchema schemaUC.SchemaGetter, classSearcher inverted.ClassSearcher,
	stopwords stopwords.StopwordDetector, shardVersion uint16,
	vectorIndex vectorIndex, logger logrus.FieldLogger,
	propLenTracker *inverted.JsonShardMetaData,
	isFallbackToSearchable inverted.IsFallbackToSearchable,
	tenant string, nestedCrossRefLimit int64,
	bitmapFactory *roaringset.BitmapFactory,
	modules *modules.Provider,
) *Aggregator {
	return &Aggregator{
		logger:                 logger,
		store:                  store,
		params:                 params,
		getSchema:              getSchema,
		classSearcher:          classSearcher,
		stopwords:              stopwords,
		shardVersion:           shardVersion,
		vectorIndex:            vectorIndex,
		propLenTracker:         propLenTracker,
		isFallbackToSearchable: isFallbackToSearchable,
		tenant:                 tenant,
		nestedCrossRefLimit:    nestedCrossRefLimit,
		bitmapFactory:          bitmapFactory,
		modules:                modules,
	}
}

func (a *Aggregator) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	return a.propLenTracker
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	if a.params.GroupBy != nil {
		return newGroupedAggregator(a).Do(ctx)
	}

	isVectorEmpty, err := dto.IsVectorEmpty(a.params.SearchVector)
	if err != nil {
		return nil, fmt.Errorf("aggregator: %w", err)
	}

	if a.params.Filters != nil || !isVectorEmpty || a.params.Hybrid != nil {
		return newFilteredAggregator(a).Do(ctx)
	}

	return newUnfilteredAggregator(a).Do(ctx)
}

func (a *Aggregator) aggTypeOfProperty(
	name schema.PropertyName,
) (aggregation.PropertyType, schema.DataType, error) {
	class := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if class == nil {
		return "", "", fmt.Errorf("could not find class %s in schema", a.params.ClassName)
	}
	schemaProp, err := schema.GetPropertyByName(class, name.String())
	if err != nil {
		return "", "", errors.Wrapf(err, "property %s", name)
	}

	if schema.IsRefDataType(schemaProp.DataType) {
		return aggregation.PropertyTypeReference, schema.DataTypeCRef, nil
	}

	dt := schema.DataType(schemaProp.DataType[0])
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeIntArray,
		schema.DataTypeNumberArray:
		return aggregation.PropertyTypeNumerical, dt, nil
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		return aggregation.PropertyTypeBoolean, dt, nil
	case schema.DataTypeText, schema.DataTypeTextArray:
		return aggregation.PropertyTypeText, dt, nil
	case schema.DataTypeDate, schema.DataTypeDateArray:
		return aggregation.PropertyTypeDate, dt, nil
	case schema.DataTypeGeoCoordinates:
		return "", "", fmt.Errorf("dataType geoCoordinates can't be aggregated")
	case schema.DataTypePhoneNumber:
		return "", "", fmt.Errorf("dataType phoneNumber can't be aggregated")
	default:
		return "", "", fmt.Errorf("unrecoginzed dataType %v", schemaProp.DataType[0])
	}
}
