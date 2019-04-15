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

// Package getmeta provides the network getmeta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"
	"strings"

	commonGetMeta "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common/getmeta"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated GetMeta Things part of the schema
func classFields(databaseSchema []*models.SemanticSchemaClass, k kind.Kind, peerName string) (*graphql.Object, error) {
	fields := graphql.Fields{}
	var (
		description string
	)

	switch k {
	case kind.THING_KIND:
		description = descriptions.LocalGetMetaThingsObj
	case kind.ACTION_KIND:
		description = descriptions.LocalGetMetaActionsObj
	default:
		return nil, fmt.Errorf("unrecoginzed kind '%#v", k)
	}

	for _, class := range databaseSchema {
		field, err := classField(k, class, class.Description, peerName)
		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateNetworkGetMeta%s%ssObj", peerName, k.TitleizedName()),
		Fields:      fields,
		Description: description,
	}), nil
}

func classField(k kind.Kind, class *models.SemanticSchemaClass, description string,
	peerName string) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("%sMeta%s", peerName, class.Class)

	fields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			fields, err := classPropertyFields(class, peerName)
			if err != nil {
				// we cannot return an error in this FieldsThunk and have to panic unfortunately
				panic(fmt.Sprintf("Failed to assemble single Network Meta Class field: %s", err))
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
						Name:        fmt.Sprintf("WeaviateNetworkGetMeta%s%s%sWhereInpObj", peerName, k.Name(), class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkGetMeta%s%s%s", peerName, k.Name(), class.Class)),
						Description: descriptions.LocalGetWhereInpObj,
					},
				),
			},
		},
	}

	return fieldsField, nil
}

func classPropertyFields(class *models.SemanticSchemaClass, peerName string) (graphql.Fields, error) {
	fields := graphql.Fields{}
	prefix := fmt.Sprintf("%sMeta", peerName)
	metaField, err := commonGetMeta.MetaPropertyField(class, prefix)
	if err != nil {
		return nil, err
	}

	fields["meta"] = metaField
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := commonGetMeta.ClassPropertyField(*propertyType, class, property, prefix)
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
