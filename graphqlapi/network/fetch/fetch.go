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
package network_fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func FieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	actionsFields := actionsFieldsObj()
	thingsFields := thingsFieldsObj()
	fuzzyFields := fuzzyFieldsObj()
	whereFilterFields := thingsActionsWhereFilterFields(filterContainer)

	fields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateNetworkFetchActions",
			Description: descriptions.NetworkFetchActions,
			Type:        graphql.NewList(actionsFields),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Things": &graphql.Field{
			Name:        "WeaviateNetworkFetchThings",
			Description: descriptions.NetworkFetchThings,
			Type:        graphql.NewList(thingsFields),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Fuzzy": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzy",
			Description: descriptions.NetworkFetchFuzzy,
			Type:        graphql.NewList(fuzzyFields),
			Args: graphql.FieldConfigArgument{
				"value": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyValue,
					Type:        graphql.NewNonNull(graphql.String),
				},
				"certainty": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyCertainty,
					Type:        graphql.NewNonNull(graphql.Float),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	fieldsObj := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchObj",
		Fields:      fields,
		Description: descriptions.NetworkFetchObj,
	}

	return graphql.NewObject(fieldsObj)
}

func actionsFieldsObj() *graphql.Object {
	getNetworkFetchActionsFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchActionsBeacon",
			Description: descriptions.NetworkFetchActionBeacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchActionsCertainty",
			Description: descriptions.NetworkFetchActionCertainty,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchActionsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchActionsObj",
		Fields:      getNetworkFetchActionsFields,
		Description: descriptions.NetworkFetchActionsObj,
	}

	return graphql.NewObject(getNetworkFetchActionsFieldsObject)
}

func thingsFieldsObj() *graphql.Object {
	getNetworkFetchThingsFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchThingsBeacon",
			Description: descriptions.NetworkFetchThingBeacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchThingsCertainty",
			Description: descriptions.NetworkFetchThingCertainty,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchThingsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchThingsObj",
		Fields:      getNetworkFetchThingsFields,
		Description: descriptions.NetworkFetchThingsObj,
	}

	return graphql.NewObject(getNetworkFetchThingsFieldsObject)
}

func fuzzyFieldsObj() *graphql.Object {
	getNetworkFetchFuzzyFields := graphql.Fields{

		"beacon": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzyBeacon",
			Description: descriptions.NetworkFetchFuzzyBeacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzyCertainty",
			Description: descriptions.NetworkFetchFuzzyCertainty,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getNetworkFetchFuzzyFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkFetchFuzzyObj",
		Fields:      getNetworkFetchFuzzyFields,
		Description: descriptions.NetworkFetchFuzzyObj,
	}

	return graphql.NewObject(getNetworkFetchFuzzyFieldsObject)
}

func thingsActionsWhereFilterFields(filterContainer *utils.FilterContainer) *graphql.ArgumentConfig {
	whereFilterFields := &graphql.ArgumentConfig{
		Description: descriptions.FetchWhereFilterFields,
		Type: graphql.NewNonNull(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "WeaviateNetworkFetchWhereInpObj",
				Fields:      thingsAndActionsFilterFields(filterContainer),
				Description: descriptions.FetchWhereFilterFieldsInpObj,
			},
		)),
	}

	return whereFilterFields
}
