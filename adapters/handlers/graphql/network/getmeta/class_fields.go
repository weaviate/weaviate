//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package getmeta provides the network getmeta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	commonMeta "github.com/semi-technologies/weaviate/adapters/handlers/graphql/common/getmeta"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Build the dynamically generated Meta Things part of the schema
func classFields(databaseSchema []*models.Class, k kind.Kind, peerName string) (*graphql.Object, error) {
	fields := graphql.Fields{}
	var (
		description string
	)

	switch k {
	case kind.Thing:
		description = descriptions.LocalMetaThingsObj
	case kind.Action:
		description = descriptions.LocalMetaActionsObj
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
		Name:        fmt.Sprintf("WeaviateNetworkMeta%s%ssObj", peerName, k.TitleizedName()),
		Fields:      fields,
		Description: description,
	}), nil
}

func classField(k kind.Kind, class *models.Class, description string,
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
						Name:        fmt.Sprintf("WeaviateNetworkMeta%s%s%sWhereInpObj", peerName, k.Name(), class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkMeta%s%s%s", peerName, k.Name(), class.Class)),
						Description: descriptions.LocalGetWhereInpObj,
					},
				),
			},
		},
	}

	return fieldsField, nil
}

func classPropertyFields(class *models.Class, peerName string) (graphql.Fields, error) {
	fields := graphql.Fields{}
	prefix := fmt.Sprintf("%sMeta", peerName)
	metaField, err := commonMeta.MetaPropertyField(class, prefix)
	if err != nil {
		return nil, err
	}

	fields["meta"] = metaField
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := commonMeta.ClassPropertyField(*propertyType, class, property, prefix)
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
