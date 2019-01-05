/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package network_introspect

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func GenWeaviateNetworkIntrospectPropertiesObjField() *graphql.Field {
	weaviateNetworkIntrospectPropertiesObject := graphql.NewObject(
		graphql.ObjectConfig{
			Name: "WeaviateNetworkIntrospectPropertiesObj",
			Fields: graphql.Fields{
				"propertyName": &graphql.Field{
					Type:        graphql.String,
					Description: descriptions.WherePropertiesPropertyNameDesc,
				},
				"certainty": &graphql.Field{
					Type:        graphql.Float,
					Description: descriptions.WhereCertaintyDesc,
				},
			},
			Description: descriptions.WherePropertiesObjDesc,
		},
	)

	weaviateNetworkIntrospectPropertiesObjField := &graphql.Field{
		Name:        "WeaviateNetworkIntrospectPropertiesObj",
		Description: descriptions.WherePropertiesObjDesc,
		Type:        graphql.NewList(weaviateNetworkIntrospectPropertiesObject),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return weaviateNetworkIntrospectPropertiesObjField
}

func thingsAndActionsFilterFields(filterContainer *utils.FilterContainer) graphql.InputObjectConfigFieldMap {
	wherePropertiesObj := wherePropertiesObj(filterContainer)
	whereClassObj := whereClassObj(filterContainer)

	fields := graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(whereClassObj),
			Description: descriptions.WhereClassDesc,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(wherePropertiesObj),
			Description: descriptions.WherePropertiesDesc,
		},
	}

	return fields
}

func wherePropertiesObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
	filterPropertiesElements := graphql.InputObjectConfigFieldMap{
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.FirstDesc,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereCertaintyDesc,
		},
		"name": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereNameDesc,
		},
		"keywords": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(filterContainer.WeaviateNetworkWhereKeywordsInpObj),
			Description: descriptions.WhereKeywordsDesc,
		},
	}

	wherePropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWherePropertiesInpObj",
			Fields:      filterPropertiesElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)

	return wherePropertiesObj
}

func whereClassObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
	filterClassElements := graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereNameDesc,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereCertaintyDesc,
		},
		"keywords": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(filterContainer.WeaviateNetworkWhereKeywordsInpObj),
			Description: descriptions.WhereKeywordsDesc,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.FirstDesc,
		},
	}

	classObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWhereClassInpObj",
			Fields:      filterClassElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)
	return classObj
}
