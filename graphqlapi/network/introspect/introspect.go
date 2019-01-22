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
package network_introspect

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func FieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	actionsFields := actionsFieldsObj(filterContainer)
	thingsFields := thingsFieldsObj(filterContainer)
	beaconFields := beaconFieldsObj()
	whereFilterFields := thingsActionsWhereFilterFields(filterContainer)

	fields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectActions",
			Description: descriptions.NetworkIntrospectActionsDesc,
			Type:        graphql.NewList(actionsFields),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Things": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectThings",
			Description: descriptions.NetworkIntrospectThingsDesc,
			Type:        graphql.NewList(thingsFields),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterFields,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Beacon": &graphql.Field{
			Name:        "WeaviateNetworkIntrospectBeacon",
			Description: descriptions.NetworkIntrospectBeaconDesc,
			Type:        beaconFields,
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

	fieldsObj := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectObj",
		Fields:      fields,
		Description: descriptions.NetworkIntrospectObjDesc,
	}

	return graphql.NewObject(fieldsObj)
}

func actionsFieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	fields := graphql.Fields{
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

	fieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectActionsObj",
		Fields:      fields,
		Description: descriptions.NetworkIntrospectActionsObjDesc,
	}

	return graphql.NewObject(fieldsObject)
}

func thingsFieldsObj(filterContainer *utils.FilterContainer) *graphql.Object {
	fields := graphql.Fields{
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

	fieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectThingsObj",
		Fields:      fields,
		Description: descriptions.NetworkIntrospectThingsObjDesc,
	}

	return graphql.NewObject(fieldsObject)
}

func beaconFieldsObj() *graphql.Object {
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

	fetchFuzzyFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkIntrospectBeaconObj",
		Fields:      introspectBeaconFields,
		Description: descriptions.NetworkIntrospectBeaconObjDesc,
	}

	return graphql.NewObject(fetchFuzzyFieldsObject)
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

func thingsActionsWhereFilterFields(filterContainer *utils.FilterContainer) *graphql.ArgumentConfig {
	whereFilterFields := &graphql.ArgumentConfig{
		Description: descriptions.IntrospectWhereFilterFieldsDesc,
		Type: graphql.NewNonNull(graphql.NewList(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "WeaviateNetworkIntrospectWhereInpObj",
				Fields:      thingsAndActionsFilterFields(filterContainer),
				Description: descriptions.IntrospectWhereFilterFieldsInpObjDesc,
			},
		))),
	}

	return whereFilterFields
}
