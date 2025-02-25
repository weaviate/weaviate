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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type typeInspector struct {
	getClass func(string) *models.Class
}

func newTypeInspector(getClass func(string) *models.Class) *typeInspector {
	return &typeInspector{
		getClass: getClass,
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

		class := i.getClass(params.ClassName.String())
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", params.ClassName)
		}

		schemaProp, err := schema.GetPropertyByName(class, prop.Name.String())
		if err != nil {
			return nil, err
		}

		err = i.extendResWithType(res, prop.Name.String(), schemaProp.DataType)
		if err != nil {
			return nil, fmt.Errorf("with types: prop %s: %w", prop.Name, err)
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

		propType, err := schema.FindPropertyDataTypeWithRefs(i.getClass, dataType, false, "")
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
			if res.Groups[groupIndex].Properties == nil {
				// prevent nil pointer panic
				res.Groups[groupIndex].Properties = map[string]aggregation.Property{}
			}
		}

		res.Groups[groupIndex].Properties[propName] = prop
	}

	return nil
}
