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
	return b.parseNestedProperties(property.NestedProperties, className, "", nil)
}

func (b *classBuilder) parseNestedProperties(nestedProps []*models.NestedProperty,
	className, propName string, propDataType []string,
) *graphql.Field {
	fields := graphql.Fields{}
	for _, prop := range nestedProps {
		if prop.NestedProperties != nil {
			fields[prop.Name] = b.parseNestedProperties(prop.NestedProperties, className, prop.Name, prop.DataType)
		} else {
			fields[prop.Name] = &graphql.Field{
				Name: fmt.Sprintf("%sObject%s%sField", className, propName, prop.Name),
				Type: b.determinNestedPropertyType(prop.DataType),
			}
		}
	}

	fieldType := graphql.NewObject(graphql.ObjectConfig{
		Name:   fmt.Sprintf("%sObject%sField", className, propName),
		Fields: fields,
	})

	// determine Object[] and Object types
	if len(propDataType) == 1 && propDataType[0] == schema.DataTypeObjectArray.String() {
		return &graphql.Field{
			Type: graphql.NewList(fieldType),
		}
	}

	return &graphql.Field{
		Type: fieldType,
	}
}

func (b *classBuilder) determinNestedPropertyType(dataType []string) graphql.Output {
	switch schema.DataType(dataType[0]) {
	case schema.DataTypeText, schema.DataTypeString:
		return graphql.String
	case schema.DataTypeInt:
		return graphql.Int
	case schema.DataTypeNumber:
		return graphql.Float
	case schema.DataTypeBoolean:
		return graphql.Boolean
	case schema.DataTypeNumberArray:
		return graphql.NewList(graphql.Float)
	// case schema.DataTypeObjectArray:
	// 	return graphql.NewList(graphql.NewObject())
	default:
		return nil
	}
}
