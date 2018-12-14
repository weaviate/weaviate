package network_fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func GenFieldsObjForNetworkFetch(filterContainer *utils.FilterContainer) *graphql.Object {
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
