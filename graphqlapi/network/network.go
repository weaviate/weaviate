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

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	network_fetch "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/fetch"
	network_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	network_getmeta "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get_meta"
	network_introspect "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/introspect"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

// Build the network queries from the database schema.
func Build(dbSchema *schema.Schema, peers []string) (*graphql.Field, error) {

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are no Actions or Things classes defined yet.")
	}

	filterContainer := &utils.FilterContainer{}

	weaviateNetworkGetResults := make(map[string]*graphql.Object)
	weaviateNetworkGetMetaResults := make(map[string]*graphql.Object)

	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterContainer.NetworkFilterOptions = make(map[string]*graphql.InputObject)

	// TODO implement function that capitalizes all Weaviate names

	if len(peers) == 0 {
		// Don't error, but also don't register the Network  field if we don't
		// have any peers. This build function will be called again if the
		// peers change, so next time it might advance past here.
		return nil, nil
	}

	for _, peer := range peers {
		// This map is used to store all the Thing and Action Objects, so that we can use them in references.
		getNetworkActionsAndThings := make(map[string]*graphql.Object)

		getKinds := graphql.Fields{}
		getMetaKinds := graphql.Fields{}

		if len(dbSchema.Actions.Classes) > 0 {
			networkGetActions, networkGetErr := network_get.GenNetworkActionClassFieldsFromSchema(dbSchema, &getNetworkActionsAndThings, peer)
			if networkGetErr != nil {
				return nil, fmt.Errorf("failed to generate action fields from schema for network Get because: %v", networkGetErr)
			}

			getKinds["Actions"] = &graphql.Field{
				Name:        "WeaviateNetworkGetActions",
				Description: descriptions.NetworkGetActionsDesc,
				Type:        networkGetActions,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("- NetworkGetActions (pass on Source)\n")
					// Does nothing; pass through the filters
					return p.Source, nil
				},
			}

			classParentTypeIsAction := true
			networkGetMetaActions, networkGetMetaErr := network_getmeta.GenNetworkMetaClassFieldsFromSchema(dbSchema.Actions.Classes, classParentTypeIsAction, peer)
			if networkGetMetaErr != nil {
				return nil, fmt.Errorf("failed to generate action fields from schema for network MetaGet because: %v", networkGetMetaErr)
			}

			getMetaKinds["Actions"] = &graphql.Field{
				Name:        "WeaviateNetworkGetMetaActions",
				Description: descriptions.NetworkGetMetaActionsDesc,
				Type:        networkGetMetaActions,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("- NetworkGetMetaActions (pass on Source)\n")
					// Does nothing; pass through the filters
					return p.Source, nil
				},
			}
		}

		if len(dbSchema.Things.Classes) > 0 {
			networkGetThings, networkGetErr := network_get.GenNetworkThingClassFieldsFromSchema(dbSchema, &getNetworkActionsAndThings, peer)
			if networkGetErr != nil {
				return nil, fmt.Errorf("failed to generate thing fields from schema for network Get because: %v", networkGetErr)
			}

			getKinds["Things"] = &graphql.Field{
				Name:        "WeaviateNetworkGetThings",
				Description: descriptions.NetworkGetThingsDesc,
				Type:        networkGetThings,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("- NetworkGetThings (pass on Source)\n")
					// Does nothing; pass through the filters
					return p.Source, nil
				},
			}

			classParentTypeIsAction := false
			networkGetMetaThings, networkGetMetaErr := network_getmeta.GenNetworkMetaClassFieldsFromSchema(dbSchema.Things.Classes, classParentTypeIsAction, peer)
			if networkGetMetaErr != nil {
				return nil, fmt.Errorf("failed to generate thing fields from schema for network MetaGet because: %v", networkGetMetaErr)
			}

			getMetaKinds["Things"] = &graphql.Field{
				Name:        "WeaviateNetworkGetMetaThings",
				Description: descriptions.NetworkGetMetaThingsDesc,
				Type:        networkGetMetaThings,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("- NetworkGetMetaThings (pass on Source)\n")
					// Does nothing; pass through the filters
					return p.Source, nil
				},
			}
		}

		networkGetObject := graphql.NewObject(graphql.ObjectConfig{
			Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", peer, "Obj"),
			Fields:      getKinds,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkGetWeaviateObjDesc, peer),
		})

		networkGetMetaObject := graphql.NewObject(graphql.ObjectConfig{
			Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGetMeta", peer, "Obj"),
			Fields:      getMetaKinds,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkGetMetaWeaviateObjDesc, peer),
		})

		weaviateNetworkGetResults[peer] = networkGetObject
		weaviateNetworkGetMetaResults[peer] = networkGetMetaObject

	}
	// TODO this is a temp function, inserts a temp weaviate obj in between Get and Things/Actions
	networkGetObject, networkGetMetaObject := insertDummyNetworkWeaviateField(weaviateNetworkGetResults, weaviateNetworkGetMetaResults)

	genGlobalNetworkFilterElements(filterContainer)

	networkFetchObj := network_fetch.GenFieldsObjForNetworkFetch(filterContainer)

	networkIntrospectObj := network_introspect.GenFieldsObjForNetworkIntrospect(filterContainer)

	graphQLNetworkFieldContents := utils.GraphQLNetworkFieldContents{
		networkGetObject,
		networkGetMetaObject,
		networkFetchObj,
		networkIntrospectObj,
	}

	networkGetAndGetMetaObject := genNetworkFields(&graphQLNetworkFieldContents /*, filterContainer*/)

	networkField := &graphql.Field{
		Type:        networkGetAndGetMetaObject,
		Description: descriptions.WeaviateNetworkDesc,
		Args: graphql.FieldConfigArgument{
			"networkTimeout": &graphql.ArgumentConfig{
				Description: descriptions.NetworkTimeoutDesc,
				Type:        graphql.Int,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// return no error, so we bubble up to the next resolver
			return p.Source, nil
		},
	}

	return networkField, nil
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
							Fields:      common_filters.BuildNew("WeaviateNetworkGet"),
							Description: descriptions.NetworkGetWhereInpObjDesc,
						},
					),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				peers, ok := p.Source.(map[string]interface{})["NetworkPeers"].([]string)
				if !ok {
					return nil, fmt.Errorf("source does not contain NetworkPeers, but \n%#v", p.Source)
				}

				filters, err := network_get.FiltersForNetworkInstances(p.Args, peers)
				if err != nil {
					return nil, err
				}

				resolver, ok := p.Source.(map[string]interface{})["NetworkResolver"].(network_get.Resolver)
				if !ok {
					return nil, fmt.Errorf("source does not contain a NetworkResolver, but \n%#v", p.Source)
				}

				return network_get.FiltersAndResolver{
					Filters:  filters,
					Resolver: resolver,
				}, nil
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
							Fields:      common_filters.BuildNew("WeaviateNetworkGetMeta"),
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

// temporary function that does nothing but display a Weaviate instance // TODO: delete this once p2p functionality is up
func insertDummyNetworkWeaviateField(weaviatesWithGetFields map[string]*graphql.Object, weaviatesWithMetaGetFields map[string]*graphql.Object) (*graphql.Object, *graphql.Object) {

	getWeaviates := graphql.Fields{}
	metaGetWeaviates := graphql.Fields{}

	for weaviate, weaviateFields := range weaviatesWithGetFields {
		getWeaviates[weaviate] = &graphql.Field{
			Name:        weaviate,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviateDesc, weaviate),
			Type:        weaviateFields,
			Resolve:     network_get.NetworkGetInstanceResolve,
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
