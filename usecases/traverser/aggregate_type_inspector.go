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

package traverser

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type typeInspector struct {
	schema schema.Schema
}

func newTypeInspector(schemaGetter schemaUC.SchemaGetter) *typeInspector {
	schema := schemaGetter.GetSchemaSkipAuth()
	return &typeInspector{
		schema: schema,
	}
}

func (i *typeInspector) WithTypes(res *aggregation.Result, params aggregation.Params) (*aggregation.Result, error) {
	if res == nil {
		return nil, nil
	}

	for _, prop := range params.Properties {
		if !i.hasTypeAggregator(prop.Aggregators) {
			// nothing to do for us
			continue
		}

		schemaProp, err := i.schema.GetProperty(params.ClassName, prop.Name)
		if err != nil {
			return nil, fmt.Errorf("with types: prop %s: %v", prop.Name, err)
		}

		err = i.extendResWithType(res, prop.Name.String(), schemaProp.DataType)
		if err != nil {
			return nil, fmt.Errorf("with types: prop %s: %v", prop.Name, err)
		}
	}

	return res, nil
}

func (i *typeInspector) hasTypeAggregator(aggs []aggregation.Aggregator) bool {
	for _, agg := range aggs {
		if agg == aggregation.TypeAggregator {
			return true
		}
	}

	return false
}

func (i *typeInspector) extendResWithType(res *aggregation.Result, propName string, dataType []string) error {
	for groupIndex, group := range res.Groups {
		prop, ok := group.Properties[propName]
		if !ok {
			prop = aggregation.Property{}
		}

		propType, err := i.schema.FindPropertyDataType(dataType)
		if err != nil {
			return err
		}

		if propType.IsPrimitive() {
			prop.SchemaType = string(propType.AsPrimitive())
		} else if propType.IsNested() { // TODO nested -> check if sufficient just schematype
			prop.SchemaType = string(propType.AsNested())
		} else {
			prop.Type = aggregation.PropertyTypeReference
			prop.SchemaType = string(schema.DataTypeCRef)
			prop.ReferenceAggregation.PointingTo = dataType
		}

		res.Groups[groupIndex].Properties[propName] = prop
	}

	return nil
}
