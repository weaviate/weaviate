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

// Package network provides the network graphql endpoint for Weaviate
package network

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

// generate these elements once
func genNetworkStaticWhereFilterElements() graphql.InputObjectConfigFieldMap {
	staticFilterElements := graphql.InputObjectConfigFieldMap{
		"operator": &graphql.InputObjectFieldConfig{
			Type:        common_filters.GetOperatorEnum(),
			Description: descriptions.WhereOperatorDesc,
		},
		"path": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: descriptions.WherePathDesc,
		},
		"valueInt": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.WhereValueIntDesc,
		},
		"valueNumber": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereValueNumberDesc,
		},
		"valueBoolean": &graphql.InputObjectFieldConfig{
			Type:        graphql.Boolean,
			Description: descriptions.WhereValueBooleanDesc,
		},
		"valueString": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereValueStringDesc,
		},
	}

	return staticFilterElements
}

// This is an exact translation of the Prototype from JS to Go. In the prototype some filter elements are declared as global variables, this is recreated here.
func genGlobalNetworkFilterElements(filterContainer *utils.FilterContainer) {
	filterContainer.WeaviateNetworkWhereKeywordsInpObj = genWeaviateNetworkWhereNameKeywordsInpObj()
	filterContainer.WeaviateNetworkIntrospectPropertiesObjField = genWeaviateNetworkIntrospectPropertiesObjField()
}

func genWeaviateNetworkWhereNameKeywordsInpObj() *graphql.InputObject {
	weaviateNetworkWhereNameKeywordsInpObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WeaviateNetworkWhereNameKeywordsInpObj",
			Fields: graphql.InputObjectConfigFieldMap{
				"value": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: descriptions.WhereKeywordsValueDesc,
				},
				"weight": &graphql.InputObjectFieldConfig{
					Type:        graphql.Float,
					Description: descriptions.WhereKeywordsWeightDesc,
				},
			},
			Description: descriptions.WhereKeywordsInpObjDesc,
		},
	)
	return weaviateNetworkWhereNameKeywordsInpObj
}

func genWeaviateNetworkIntrospectPropertiesObjField() *graphql.Field {
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

func genNetworkFetchThingsAndActionsFilterFields(filterContainer *utils.FilterContainer) graphql.InputObjectConfigFieldMap {
	networkFetchWhereInpObjPropertiesObj := genNetworkFetchWhereInpObjPropertiesObj(filterContainer)
	networkFetchWhereInpObjClassInpObj := genNetworkFetchWhereInpObjClassInpObj(filterContainer)

	networkFetchThingsAndActionsFilterFields := graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(networkFetchWhereInpObjClassInpObj),
			Description: descriptions.WhereClassDesc,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(networkFetchWhereInpObjPropertiesObj),
			Description: descriptions.WherePropertiesDesc,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.FirstDesc,
		},
	}

	return networkFetchThingsAndActionsFilterFields
}

func genNetworkFetchWhereInpObjPropertiesObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
	filterPropertiesElements := genNetworkStaticWhereFilterElements()
	// delete "path" key/value set
	delete(filterPropertiesElements, "path")

	filterPropertiesElements["certainty"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.Float,
		Description: descriptions.WhereCertaintyDesc,
	}
	filterPropertiesElements["valueDate"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.String,
		Description: descriptions.WhereValueDateesc,
	}
	filterPropertiesElements["name"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.String,
		Description: descriptions.WhereNameDesc,
	}
	filterPropertiesElements["keywords"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(genNetworkFetchWherePropertyWhereKeywordsInpObj()),
		Description: descriptions.WhereKeywordsDesc,
	}

	networkFetchWhereInpObjPropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkFetchWhereInpObjProperties",
			Fields:      filterPropertiesElements,
			Description: descriptions.WherePropertiesDesc,
		},
	)

	return networkFetchWhereInpObjPropertiesObj
}

func genNetworkFetchWherePropertyWhereKeywordsInpObj() *graphql.InputObject {
	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "NetworkFetchWherePropertyWhereKeywordsInpObj",
			Fields: graphql.InputObjectConfigFieldMap{
				"value": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: descriptions.WhereKeywordsValueDesc,
				},
				"weight": &graphql.InputObjectFieldConfig{
					Type:        graphql.Float,
					Description: descriptions.WhereKeywordsWeightDesc,
				},
			},
			Description: descriptions.WhereKeywordsInpObjDesc,
		},
	)
	return outputObject
}

func genNetworkFetchWhereInpObjClassInpObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
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

	networkFetchWhereInpObjClassInpObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkFetchWhereInpObjClassInpObj",
			Fields:      filterClassElements,
			Description: descriptions.WhereClassDesc,
		},
	)
	return networkFetchWhereInpObjClassInpObj
}

func genNetworkIntrospectThingsAndActionsFilterFields(filterContainer *utils.FilterContainer) graphql.InputObjectConfigFieldMap {
	weaviateNetworkIntrospectWherePropertiesObj := genWeaviateNetworkIntrospectWherePropertiesObj(filterContainer)
	weaviateNetworkIntrospectWhereClassObj := genWeaviateNetworkIntrospectWhereClassObj(filterContainer)

	networkIntrospectThingsAndActionsFilterFields := graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(weaviateNetworkIntrospectWhereClassObj),
			Description: descriptions.WhereClassDesc,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(weaviateNetworkIntrospectWherePropertiesObj),
			Description: descriptions.WherePropertiesDesc,
		},
	}

	return networkIntrospectThingsAndActionsFilterFields
}

func genWeaviateNetworkIntrospectWherePropertiesObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
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

	weaviateNetworkIntrospectWherePropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWherePropertiesObj",
			Fields:      filterPropertiesElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)

	return weaviateNetworkIntrospectWherePropertiesObj
}

func genWeaviateNetworkIntrospectWhereClassObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
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

	weaviateNetworkIntrospectWhereClassObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWhereClassObj",
			Fields:      filterClassElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)
	return weaviateNetworkIntrospectWhereClassObj
}
