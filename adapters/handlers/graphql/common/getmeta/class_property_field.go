/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

// Package getmeta aims to reduce the repition between loca/getmeta and
// network/getmeta
package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/graphql-go/graphql"
)

// ClassPropertyField builds one class field for a GetMeta Query based on the
// underlying schema type
func ClassPropertyField(dataType schema.DataType, class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) (*graphql.Field, error) {
	switch dataType {
	case schema.DataTypeString, schema.DataTypeText, schema.DataTypeDate:
		return makePropertyField(class, property, stringPropertyFields, prefix)
	case schema.DataTypeInt, schema.DataTypeNumber:
		return makePropertyField(class, property, numericalPropertyField, prefix)
	case schema.DataTypeBoolean:
		return makePropertyField(class, property, booleanPropertyFields, prefix)
	case schema.DataTypeCRef:
		return makePropertyField(class, property, refPropertyObj, prefix)
	case schema.DataTypeGeoCoordinates:
		// simply skip for now, see gh-729
		return nil, nil
	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype+": %s", dataType)
	}
}

type propertyFieldMaker func(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object

func makePropertyField(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty,
	fieldMaker propertyFieldMaker, prefix string) (*graphql.Field, error) {
	return &graphql.Field{
		Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaProperty, property.Name),
		Type:        fieldMaker(class, property, prefix),
	}, nil
}

// MetaPropertyField is a special kind of Property field that adds Meta.Count
// info to a GetMeta Query regardless of the underlying schema
func MetaPropertyField(class *models.SemanticSchemaClass, prefix string) (*graphql.Field, error) {
	return &graphql.Field{
		Description: descriptions.GetMetaMetaProperty,
		Type:        metaPropertyObj(class, prefix),
	}, nil
}

func metaPropertyObj(class *models.SemanticSchemaClass, prefix string) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sMetaCount", prefix, class.Class),
			Description: descriptions.GetMetaClassMetaCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%sMetaObj", prefix, class.Class),
		Fields:      getMetaPropertyFields,
		Description: descriptions.GetMetaClassMetaObj,
	}

	return graphql.NewObject(metaPropertyFields)
}
