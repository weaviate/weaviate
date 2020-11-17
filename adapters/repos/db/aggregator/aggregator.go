//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type Aggregator struct {
	db               *bolt.DB
	params           traverser.AggregateParams
	getSchema        schemaUC.SchemaGetter
	invertedRowCache *inverted.RowCacher
	classSearcher    inverted.ClassSearcher // to support ref-filters
}

func New(db *bolt.DB, params traverser.AggregateParams,
	getSchema schemaUC.SchemaGetter, cache *inverted.RowCacher,
	classSearcher inverted.ClassSearcher) *Aggregator {
	return &Aggregator{
		db:               db,
		params:           params,
		getSchema:        getSchema,
		invertedRowCache: cache,
		classSearcher:    classSearcher,
	}
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	if a.params.GroupBy != nil {
		return newGroupedAggregator(a).Do(ctx)
	}

	if a.params.Filters != nil {
		return newFilteredAggregator(a).Do(ctx)
	}

	return newUnfilteredAggregator(a).Do(ctx)
}

func (a *Aggregator) aggTypeOfProperty(
	name schema.PropertyName) (aggregation.PropertyType, schema.DataType, error) {
	s := a.getSchema.GetSchemaSkipAuth()
	schemaProp, err := s.GetProperty(a.params.Kind, a.params.ClassName, name)
	if err != nil {
		return "", "", errors.Wrapf(err, "property %s", name)
	}

	if schema.IsRefDataType(schemaProp.DataType) {
		return aggregation.PropertyTypeReference, "", nil
	}

	dt := schema.DataType(schemaProp.DataType[0])
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber:
		return aggregation.PropertyTypeNumerical, dt, nil
	case schema.DataTypeBoolean:
		return aggregation.PropertyTypeBoolean, dt, nil
	case schema.DataTypeText, schema.DataTypeString:
		return aggregation.PropertyTypeText, dt, nil
	case schema.DataTypeGeoCoordinates:
		return "", "", fmt.Errorf("dataType geoCoordinates can't be aggregated")
	case schema.DataTypePhoneNumber:
		return "", "", fmt.Errorf("dataType phoneNumber can't be aggregated")
	default:
		return "", "", fmt.Errorf("unrecoginzed dataType %v", schemaProp.DataType[0])
	}
}
