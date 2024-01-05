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

package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (b *classBuilder) nestedField(propertyType schema.PropertyDataType,
	property *models.Property, className string,
) *graphql.Field {
	return b.parseNestedProperties(property.NestedProperties, className, property.Name, property.DataType)
}

func (b *classBuilder) parseNestedProperties(nestedProps []*models.NestedProperty,
	className, prefix string, propDataType []string,
) *graphql.Field {
	fields := graphql.Fields{}
	for _, prop := range nestedProps {
		if prop.NestedProperties != nil {
			fields[prop.Name] = b.parseNestedProperties(prop.NestedProperties,
				className, fmt.Sprintf("%s_%s", prefix, prop.Name), prop.DataType)
		} else {
			fields[prop.Name] = &graphql.Field{
				Name: fmt.Sprintf("%s_%s_%s_field", className, prefix, prop.Name),
				Type: b.determinNestedPropertyType(prop.DataType, prop.Name),
			}
		}
	}

	fieldType := graphql.NewObject(graphql.ObjectConfig{
		Name:   fmt.Sprintf("%s_%s_object", className, prefix),
		Fields: fields,
	})

	if len(propDataType) == 1 && propDataType[0] == schema.DataTypeObjectArray.String() {
		return &graphql.Field{Type: graphql.NewList(fieldType)}
	}
	return &graphql.Field{Type: fieldType}
}

func (b *classBuilder) determinNestedPropertyType(dataType []string, propName string) graphql.Output {
	switch schema.DataType(dataType[0]) {
	case schema.DataTypeText, schema.DataTypeString:
		return graphql.String
	case schema.DataTypeInt:
		return graphql.Int
	case schema.DataTypeNumber:
		return graphql.Float
	case schema.DataTypeBoolean:
		return graphql.Boolean
	case schema.DataTypeDate:
		return graphql.String
	case schema.DataTypeBlob:
		return graphql.String
	case schema.DataTypeUUID:
		return graphql.String
	case schema.DataTypeTextArray, schema.DataTypeStringArray:
		return graphql.NewList(graphql.String)
	case schema.DataTypeIntArray:
		return graphql.NewList(graphql.Int)
	case schema.DataTypeNumberArray:
		return graphql.NewList(graphql.Float)
	case schema.DataTypeBooleanArray:
		return graphql.NewList(graphql.Boolean)
	case schema.DataTypeDateArray:
		return graphql.NewList(graphql.String)
	case schema.DataTypeUUIDArray:
		return graphql.NewList(graphql.String)
	default:
		panic(fmt.Sprintf("determinNestedPropertyType: unknown primitive type for property %s: %s",
			propName, dataType[0]))
	}
}
