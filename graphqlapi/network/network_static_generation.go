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
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

// temporary function that does nothing but display a Weaviate instance // TODO: delete this once p2p functionality is up
func insertDummyNetworkWeaviateField(weaviatesWithGetFields map[string]*graphql.Object, weaviatesWithMetaGetFields map[string]*graphql.Object) (*graphql.Object, *graphql.Object) {

	getWeaviates := graphql.Fields{}
	metaGetWeaviates := graphql.Fields{}

	for weaviate, weaviateFields := range weaviatesWithGetFields {
		getWeaviates[weaviate] = &graphql.Field{
			Name:        weaviate,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviateDesc, weaviate),
			Type:        weaviateFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}
		metaGetWeaviates[weaviate] = &graphql.Field{
			Name:        fmt.Sprintf("%s%s", "Meta", weaviate),
			Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviateDesc, weaviate),
			Type:        weaviatesWithMetaGetFields[weaviate],
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}
	}

	dummyWeaviateGetObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetObj",
		Fields:      getWeaviates,
		Description: descriptions.NetworkGetObjDesc,
	}
	dummyWeaviateGetMetaObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetMetaObj",
		Fields:      metaGetWeaviates,
		Description: descriptions.NetworkGetMetaObjDesc,
	}

	return graphql.NewObject(dummyWeaviateGetObject), graphql.NewObject(dummyWeaviateGetMetaObject)
}

func genFieldsObjForNetworkFetch(filterContainer *utils.FilterContainer) *graphql.Object {
	networkFetchActionsFields := genNetworkFetchActionsFieldsObj()
	networkFetchThingsFields := genNetworkFetchThingsFieldsObj()
	networkFetchFuzzyFields := genNetworkFetchFuzzyFieldsObj()
	networkFetchWhereFilterFields := genNetworkFetchThingsActionsWhereFilterFields(filterContainer)

	networkFetchFields := graphql.Fields{

		"Actions": &graphql.Field{
			Name:        "WeaviateNetworkFetchActions",
			Description: descriptions.NetworkFetchActionsDesc,
			Type:        graphql.NewList(networkFetchActionsFields),
			Args: graphql.FieldConfigArgument{
				"where": networkFetchWhereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Things": &graphql.Field{
			Name:        "WeaviateNetworkFetchThings",
			Description: descriptions.NetworkFetchThingsDesc,
			Type:        graphql.NewList(networkFetchThingsFields),
			Args: graphql.FieldConfigArgument{
				"where": networkFetchWhereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Fuzzy": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzy",
			Description: descriptions.NetworkFetchFuzzyDesc,
			Type:        graphql.NewList(networkFetchFuzzyFields),
			Args: graphql.FieldConfigArgument{
				"value": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyValueDesc,
					Type:        graphql.NewNonNull(graphql.String),
				},
				"certainty": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyCertaintyDesc,
					Type:        graphql.NewNonNull(graphql.Float),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	networkFetchFieldsObj := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchObj",
		Fields:      networkFetchFields,
		Description: descriptions.NetworkFetchObjDesc,
	}

	return graphql.NewObject(networkFetchFieldsObj)
}

func genNetworkFetchActionsFieldsObj() *graphql.Object {
	getNetworkFetchActionsFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchActionsBeacon",
			Description: descriptions.NetworkFetchActionBeaconDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchActionsCertainty",
			Description: descriptions.NetworkFetchActionCertaintyDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchActionsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchActionsObj",
		Fields:      getNetworkFetchActionsFields,
		Description: descriptions.NetworkFetchActionsObjDesc,
	}

	return graphql.NewObject(getNetworkFetchActionsFieldsObject)
}

func genNetworkFetchThingsFieldsObj() *graphql.Object {
	getNetworkFetchThingsFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchThingsBeacon",
			Description: descriptions.NetworkFetchThingBeaconDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchThingsCertainty",
			Description: descriptions.NetworkFetchThingCertaintyDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchThingsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchThingsObj",
		Fields:      getNetworkFetchThingsFields,
		Description: descriptions.NetworkFetchThingsObjDesc,
	}

	return graphql.NewObject(getNetworkFetchThingsFieldsObject)
}

func genNetworkFetchFuzzyFieldsObj() *graphql.Object {
	getNetworkFetchFuzzyFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzyBeacon",
			Description: descriptions.NetworkFetchFuzzyBeaconDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzyCertainty",
			Description: descriptions.NetworkFetchFuzzyCertaintyDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchFuzzyFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchFuzzyObj",
		Fields:      getNetworkFetchFuzzyFields,
		Description: descriptions.NetworkFetchFuzzyObjDesc,
	}

	return graphql.NewObject(getNetworkFetchFuzzyFieldsObject)
}

func genNetworkFetchThingsActionsWhereFilterFields(filterContainer *utils.FilterContainer) *graphql.ArgumentConfig {
	whereFilterFields := &graphql.ArgumentConfig{
		Description: descriptions.FetchWhereFilterFieldsDesc,
		Type: graphql.NewNonNull(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "WeaviateNetworkFetchWhereInpObj",
				Fields:      genNetworkFetchThingsAndActionsFilterFields(filterContainer),
				Description: descriptions.FetchWhereFilterFieldsInpObjDesc,
			},
		)),
	}

	return whereFilterFields
}

func genFieldsObjForNetworkIntrospect(filterContainer *utils.FilterContainer) *graphql.Object {
	networkIntrospectActionsFields := genNetworkIntrospectActionsFieldsObj(filterContainer)
	networkIntrospectThingsFields := genNetworkIntrospectThingsFieldsObj(filterContainer)
	networkIntrospectBeaconFields := genNetworkIntrospectBeaconFieldsObj()
	networkIntrospectWhereFilterFields := genNetworkIntrospectThingsActionsWhereFilterFields(filterContainer)

	networkIntrospectFields := graphql.Fields{

		"Actions": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectActions",
			Description: descriptions.NetworkIntrospectActionsDesc,
			Type:        graphql.NewList(networkIntrospectActionsFields),
			Args: graphql.FieldConfigArgument{
				"where": networkIntrospectWhereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Things": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectThings",
			Description: descriptions.NetworkIntrospectThingsDesc,
			Type:        graphql.NewList(networkIntrospectThingsFields),
			Args: graphql.FieldConfigArgument{
				"where": networkIntrospectWhereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Beacon": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeacon",
			Description: descriptions.NetworkIntrospectBeaconDesc,
			Type:        networkIntrospectBeaconFields,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Description: descriptions.IntrospectBeaconIdDesc,
					Type:        graphql.NewNonNull(graphql.String),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	networkIntrospectFieldsObj := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectObj",
		Fields:      networkIntrospectFields,
		Description: descriptions.NetworkIntrospectObjDesc,
	}

	return graphql.NewObject(networkIntrospectFieldsObj)
}

func genNetworkIntrospectActionsFieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	getNetworkIntrospectActionsFields := graphql.Fields{

		"weaviate": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectActionsWeaviate",
			Description: descriptions.NetworkIntrospectWeaviateDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"className": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectActionsClassName",
			Description: descriptions.NetworkIntrospectClassNameDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectActionsCertainty",
			Description: descriptions.NetworkIntrospectCertaintyDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"properties": filterContainer.WeaviateNetworkIntrospectPropertiesObjField,
	}

	getNetworkIntrospectActionsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectActionsObj",
		Fields:      getNetworkIntrospectActionsFields,
		Description: descriptions.NetworkIntrospectActionsObjDesc,
	}

	return graphql.NewObject(getNetworkIntrospectActionsFieldsObject)
}

func genNetworkIntrospectThingsFieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	getNetworkIntrospectThingsFields := graphql.Fields{

		"weaviate": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectThingsWeaviate",
			Description: descriptions.NetworkIntrospectWeaviateDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"className": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectThingsClassName",
			Description: descriptions.NetworkIntrospectClassNameDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectThingsCertainty",
			Description: descriptions.NetworkIntrospectCertaintyDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"properties": filterContainer.WeaviateNetworkIntrospectPropertiesObjField,
	}

	getNetworkIntrospectThingsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectThingsObj",
		Fields:      getNetworkIntrospectThingsFields,
		Description: descriptions.NetworkIntrospectThingsObjDesc,
	}

	return graphql.NewObject(getNetworkIntrospectThingsFieldsObject)
}

func genNetworkIntrospectBeaconFieldsObj() *graphql.Object {
	beaconPropertiesObj := genWeaviateNetworkIntrospectBeaconPropertiesObj()

	introspectBeaconFields := graphql.Fields{

		"weaviate": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeaconWeaviate",
			Description: descriptions.NetworkIntrospectWeaviateDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"className": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeaconclassName",
			Description: descriptions.NetworkIntrospectClassNameDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"properties": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeaconPropertiesObj",
			Description: descriptions.NetworkIntrospectBeaconPropertiesDesc,
			Type:        graphql.NewList(beaconPropertiesObj),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchFuzzyFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectBeaconObj",
		Fields:      introspectBeaconFields,
		Description: descriptions.NetworkIntrospectBeaconObjDesc,
	}

	return graphql.NewObject(getNetworkFetchFuzzyFieldsObject)
}

func genWeaviateNetworkIntrospectBeaconPropertiesObj() *graphql.Object {
	beaconPropertiesFields := graphql.Fields{

		"propertyName": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeaconPropertiesObjPropertyName",
			Description: descriptions.NetworkIntrospectBeaconPropertiesPropertyNameDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	beaconPropertiesObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectBeaconPropertiesObj",
		Fields:      beaconPropertiesFields,
		Description: descriptions.NetworkIntrospectBeaconPropertiesDesc,
	}

	return graphql.NewObject(beaconPropertiesObject)
}

func genNetworkIntrospectThingsActionsWhereFilterFields(filterContainer *utils.FilterContainer) *graphql.ArgumentConfig {
	whereFilterFields := &graphql.ArgumentConfig{
		Description: descriptions.IntrospectWhereFilterFieldsDesc,
		Type: graphql.NewNonNull(graphql.NewList(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "WeaviateNetworkIntrospectWhereInpObj",
				Fields:      genNetworkIntrospectThingsAndActionsFilterFields(filterContainer),
				Description: descriptions.IntrospectWhereFilterFieldsInpObjDesc,
			},
		))),
	}

	return whereFilterFields
}

func genNetworkFields(graphQLNetworkFieldContents *utils.GraphQLNetworkFieldContents) *graphql.Object {
	networkGetAndGetMetaFields := graphql.Fields{

		"Get": &graphql.Field{
			Name:        descriptions.NetworkGetDesc,
			Type:        graphQLNetworkFieldContents.NetworkGetObject,
			Description: descriptions.NetworkGetDesc,
			Args: graphql.FieldConfigArgument{
				"where": &graphql.ArgumentConfig{
					Description: descriptions.NetworkGetWhereDesc,
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateNetworkGetWhereInpObj",
							Fields:      common_filters.GetGetAndGetMetaWhereFilters(),
							Description: descriptions.NetworkGetWhereInpObjDesc,
						},
					),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"GetMeta": &graphql.Field{
			Name:        "WeaviateNetworkGetMeta",
			Type:        graphQLNetworkFieldContents.NetworkGetMetaObject,
			Description: descriptions.NetworkGetMetaDesc,
			Args: graphql.FieldConfigArgument{
				"where": &graphql.ArgumentConfig{
					Description: descriptions.NetworkGetWhereDesc,
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateNetworkGetMetaWhereInpObj",
							Fields:      common_filters.GetGetAndGetMetaWhereFilters(),
							Description: descriptions.NetworkGetWhereInpObjDesc,
						},
					),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Fetch": &graphql.Field{
			Name:        "WeaviateNetworkFetch",
			Type:        graphQLNetworkFieldContents.NetworkFetchObject,
			Description: descriptions.NetworkFetchDesc,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Introspect": &graphql.Field{
			Name:        "WeaviateNetworkIntrospection",
			Type:        graphQLNetworkFieldContents.NetworkIntrospectObject,
			Description: descriptions.NetworkIntrospectDesc,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	weaviateNetworkObject := &graphql.ObjectConfig{
		Name:        "WeaviateNetworkObj",
		Fields:      networkGetAndGetMetaFields,
		Description: descriptions.NetworkObjDesc,
	}

	return graphql.NewObject(*weaviateNetworkObject)
}
