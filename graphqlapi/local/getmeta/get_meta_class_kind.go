/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package getmeta provides the Local GetMeta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated GetMeta Things part of the schema
func classFields(databaseSchema []*models.SemanticSchemaClass, k kind.Kind) (*graphql.Object, error) {
	fields := graphql.Fields{}
	var (
		name        string
		description string
	)

	switch k {
	case kind.THING_KIND:
		name = "WeaviateLocalGetMetaThingsObj"
		description = descriptions.LocalGetMetaThingsObjDesc
	case kind.ACTION_KIND:
		name = "WeaviateLocalGetMetaActionsObj"
		description = descriptions.LocalGetMetaActionsObjDesc
	default:
		return nil, fmt.Errorf("unrecoginzed kind '%#v", k)
	}

	for _, class := range databaseSchema {
		field, err := classField(k, class, class.Description)

		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        name,
		Fields:      fields,
		Description: description,
	}), nil
}

func classField(k kind.Kind, class *models.SemanticSchemaClass, description string) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("Meta%s", class.Class)

	fields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			fields, err := classPropertyFields(class)
			if err != nil {
				// we cannot return an error in this FieldsThunk and have to panic unfortunately
				panic(fmt.Sprintf("Failed to assemble single Local Meta Class field: %s", err))
			}

			return fields
		}),
		Description: description,
	}

	fieldsObject := graphql.NewObject(fields)
	fieldsField := &graphql.Field{
		Type:        fieldsObject,
		Description: description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateLocalGetMeta%s%sWhereInpObj", k.Name(), class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateLocalGetMeta%s%s", k.Name(), class.Class)),
						Description: descriptions.LocalGetWhereInpObjDesc,
					},
				),
			},
		},
		Resolve: makeResolveClass(k),
	}

	return fieldsField, nil
}

func classPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {
	fields := graphql.Fields{}
	metaField, err := metaPropertyField(class)
	if err != nil {
		return nil, err
	}

	fields["meta"] = metaField
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := classPropertyField(*propertyType, class, property)
		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			fields[strings.Title(property.Name)] = convertedDataType
		} else {
			fields[property.Name] = convertedDataType
		}
	}

	return fields, nil
}

func classPropertyField(dataType schema.DataType, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {
	switch dataType {
	case schema.DataTypeString:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeText:
		return makePropertyField(class, property, textPropertyFields)
	case schema.DataTypeInt:
		return makePropertyField(class, property, intPropertyFields)
	case schema.DataTypeNumber:
		return makePropertyField(class, property, numberPropertyFields)
	case schema.DataTypeBoolean:
		return makePropertyField(class, property, booleanPropertyFields)
	case schema.DataTypeDate:
		return makePropertyField(class, property, datePropertyFields)
	case schema.DataTypeCRef:
		return makePropertyField(class, property, refPropertyObj)
	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

type propertyFieldMaker func(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty) *graphql.Object

func makePropertyField(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty,
	fieldMaker propertyFieldMaker) (*graphql.Field, error) {
	return &graphql.Field{
		Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
		Type:        fieldMaker(class, property),
	}, nil
}

func metaPropertyField(class *models.SemanticSchemaClass) (*graphql.Field, error) {
	return &graphql.Field{
		Description: descriptions.GetMetaMetaPropertyDesc,
		Type:        metaPropertyObj(class),
	}, nil
}

func metaPropertyObj(class *models.SemanticSchemaClass) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("Meta%sMetaCount", class.Class),
			Description: descriptions.GetMetaClassMetaCountDesc,
			Type:        graphql.Int,
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%sMetaObj", class.Class),
		Fields:      getMetaPropertyFields,
		Description: descriptions.GetMetaClassMetaObjDesc,
	}

	return graphql.NewObject(metaPropertyFields)
}
