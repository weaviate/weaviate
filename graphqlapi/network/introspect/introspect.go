package network_introspect

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func GenFieldsObjForNetworkIntrospect(filterContainer *utils.FilterContainer) *graphql.Object {
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
