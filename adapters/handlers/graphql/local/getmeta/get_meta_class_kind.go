/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

// Package getmeta provides the Local GetMeta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"
	"strings"

	commonGetMeta "github.com/semi-technologies/weaviate/adapters/handlers/graphql/common/getmeta"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated GetMeta Things part of the schema
func classFields(databaseSchema []*models.SemanticSchemaClass, k kind.Kind,
	config config.Config) (*graphql.Object, error) {
	fields := graphql.Fields{}
	var (
		description string
	)

	switch k {
	case kind.Thing:
		description = descriptions.LocalGetMetaThingsObj
	case kind.Action:
		description = descriptions.LocalGetMetaActionsObj
	default:
		return nil, fmt.Errorf("unrecoginzed kind '%#v", k)
	}

	for _, class := range databaseSchema {
		field, err := classField(k, class, class.Description, config)

		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalGetMeta%ssObj", k.TitleizedName()),
		Fields:      fields,
		Description: description,
	}), nil
}

func classField(k kind.Kind, class *models.SemanticSchemaClass, description string,
	config config.Config) (*graphql.Field, error) {
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
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.After,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateLocalGetMeta%s%sWhereInpObj", k.Name(), class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateLocalGetMeta%s%s", k.Name(), class.Class)),
						Description: descriptions.LocalGetWhereInpObj,
					},
				),
			},
		},
		Resolve: makeResolveClass(k),
	}

	fieldsField = extendArgsWithAnalyticsConfig(fieldsField, config)
	return fieldsField, nil
}

func extendArgsWithAnalyticsConfig(field *graphql.Field, config config.Config) *graphql.Field {
	if !config.AnalyticsEngine.Enabled {
		return field
	}

	field.Args["useAnalyticsEngine"] = &graphql.ArgumentConfig{
		DefaultValue: config.AnalyticsEngine.DefaultUseAnalyticsEngine,
		Type:         graphql.Boolean,
	}

	field.Args["forceRecalculate"] = &graphql.ArgumentConfig{
		DefaultValue: false,
		Type:         graphql.Boolean,
	}

	return field
}

func classPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {
	fields := graphql.Fields{}
	metaField, err := commonGetMeta.MetaPropertyField(class, "Meta")
	if err != nil {
		return nil, err
	}

	fields["meta"] = metaField
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := commonGetMeta.ClassPropertyField(*propertyType, class, property, "Meta")
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
