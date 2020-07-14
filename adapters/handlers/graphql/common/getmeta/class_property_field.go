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

// Package getmeta aims to reduce the repition between loca/getmeta and
// network/getmeta
package getmeta

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// ClassPropertyField builds one class field for a Meta Query based on the
// underlying schema type
func ClassPropertyField(dataType schema.DataType, class *models.Class,
	property *models.Property, prefix string) (*graphql.Field, error) {
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

type propertyFieldMaker func(class *models.Class,
	property *models.Property, prefix string) *graphql.Object

func makePropertyField(class *models.Class, property *models.Property,
	fieldMaker propertyFieldMaker, prefix string) (*graphql.Field, error) {
	return &graphql.Field{
		Description: fmt.Sprintf(`%s"%s"`, descriptions.MetaProperty, property.Name),
		Type:        fieldMaker(class, property, prefix),
	}, nil
}

// MetaPropertyField is a special kind of Property field that adds Meta.Count
// info to a Meta Query regardless of the underlying schema
func MetaPropertyField(class *models.Class, prefix string) (*graphql.Field, error) {
	return &graphql.Field{
		Description: descriptions.MetaMetaProperty,
		Type:        metaPropertyObj(class, prefix),
	}, nil
}

func metaPropertyObj(class *models.Class, prefix string) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sMetaCount", prefix, class.Class),
			Description: descriptions.MetaClassMetaCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%sMetaObj", prefix, class.Class),
		Fields:      getMetaPropertyFields,
		Description: descriptions.MetaClassMetaObj,
	}

	return graphql.NewObject(metaPropertyFields)
}
