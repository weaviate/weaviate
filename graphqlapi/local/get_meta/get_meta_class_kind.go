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

// Package get_meta provides the Local GetMeta graphql endpoint for Weaviate
package get_meta

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated GetMeta Things part of the schema
func classFields(databaseSchema []*models.SemanticSchemaClass, classParentTypeIsAction bool) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	kindName := "Things"
	name := "WeaviateLocalGetMetaThingsObj"
	description := descriptions.LocalGetMetaThingsObjDesc
	if classParentTypeIsAction {
		name = "WeaviateLocalGetMetaActionsObj"
		description = descriptions.LocalGetMetaActionsObjDesc
		kindName = "Actions"
	}

	for _, class := range databaseSchema {
		field, err := classField(kindName, class, class.Description)

		if err != nil {
			return nil, err
		}

		classFields[class.Class] = field
	}

	LocalGetMetaClasses := graphql.ObjectConfig{
		Name:        name,
		Fields:      classFields,
		Description: description,
	}

	return graphql.NewObject(LocalGetMetaClasses), nil
}

func classField(kindName string, class *models.SemanticSchemaClass, description string) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("Meta%s", class.Class)

	fields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			fields, err := classPropertyFields(class)

			if err != nil {
				panic("Failed to assemble single Local Meta Class field")
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
						Name:        fmt.Sprintf("WeaviateLocalGetMeta%s%sWhereInpObj", kindName, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateLocalGetMeta%s%s", kindName, class.Class)),
						Description: descriptions.LocalGetWhereInpObjDesc,
					},
				),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			source, ok := p.Source.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected source to be a map, but was %t", p.Source)
			}

			resolver, ok := source["Resolver"].(Resolver)
			if !ok {
				return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %t", p.Source)
			}

			return resolver.LocalGetMeta(nil)
		},
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
			return nil, err
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
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported - class property field")
		},
	}, nil
}

func metaPropertyObj(class *models.SemanticSchemaClass) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("Meta%sMetaCount", class.Class),
			Description: descriptions.GetMetaClassMetaCountDesc,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%sMetaObj", class.Class),
		Fields:      getMetaPropertyFields,
		Description: descriptions.GetMetaClassMetaObjDesc,
	}

	return graphql.NewObject(metaPropertyFields)
}
