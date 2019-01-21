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
	network_aggregate "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/aggregate"
	network_fetch "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/fetch"
	network_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	network_getmeta "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/getmeta"
	network_introspect "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/introspect"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/graphql-go/graphql"
)

// Build the network queries from the database schema.
func Build(peers peers.Peers) (*graphql.Field, error) {

	filterContainer := &utils.FilterContainer{}

	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterContainer.NetworkFilterOptions = make(map[string]*graphql.InputObject)

	// TODO implement function that capitalizes all Weaviate names

	if len(peers) == 0 {
		// Don't error, but also don't register the Network  field if we don't
		// have any peers. This build function will be called again if the
		// peers change, so next time it might advance past here.
		return nil, nil
	}

	networkGetObject, networkGetMetaObject, networkAggregateObject := buildGetAndGetMeta(peers)
	if networkGetObject == nil && networkGetMetaObject == nil && networkAggregateObject == nil {
		// if we don't have any peers with schemas, we effectively don't have
		// a Network Field.
		// We should not error though, because local queries are still possible.
		return nil, nil
	}

	genGlobalNetworkFilterElements(filterContainer)

	networkFetchObject := network_fetch.FieldsObj(filterContainer)

	networkIntrospectObject := network_introspect.FieldsObj(filterContainer)

	graphQLNetworkFieldContents := utils.GraphQLNetworkFieldContents{
		NetworkGetObject:        networkGetObject,
		NetworkGetMetaObject:    networkGetMetaObject,
		NetworkFetchObject:      networkFetchObject,
		NetworkIntrospectObject: networkIntrospectObject,
		NetworkAggregateObject:  networkAggregateObject,
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
			Resolve:     passThroughFiltersAndResolvers,
		},

		"GetMeta": &graphql.Field{
			Name:        "WeaviateNetworkGetMeta",
			Type:        graphQLNetworkFieldContents.NetworkGetMetaObject,
			Description: descriptions.NetworkGetMetaDesc,
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
		"Aggregate": &graphql.Field{
			Name:        "WeaviateNetworkAggregate",
			Type:        graphQLNetworkFieldContents.NetworkAggregateObject,
			Description: descriptions.NetworkAggregateDesc,
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

func buildGetAndGetMeta(peers peers.Peers) (*graphql.Object, *graphql.Object, *graphql.Object) {

	if len(peers) == 0 {
		// if we don't have any peers, we must return nil
		// otherwise we'd have an empty Get and GetMeta object, which
		// is not valid GraphQL
		return nil, nil, nil
	}

	getWeaviates := graphql.Fields{}
	metaGetWeaviates := graphql.Fields{}
	aggregateWeaviates := graphql.Fields{}

	for _, peer := range peers {
		// TODO: replace panics with actual error handling

		get, err := network_get.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			panic(fmt.Errorf("could not build Get for peer '%s': %s", peer.Name, err))
		}
		getWeaviates[peer.Name] = get

		getMeta, err := network_getmeta.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			panic(fmt.Errorf("could not build GetMeta for peer '%s': %s", peer.Name, err))
		}
		metaGetWeaviates[peer.Name] = getMeta

		aggregate, err := network_aggregate.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			panic(fmt.Errorf("could not build Aggregate for peer '%s': %s", peer.Name, err))
		}
		aggregateWeaviates[peer.Name] = aggregate
	}

	GetObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetObj",
		Fields:      getWeaviates,
		Description: descriptions.NetworkGetObjDesc,
	}
	GetMetaObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetMetaObj",
		Fields:      metaGetWeaviates,
		Description: descriptions.NetworkGetMetaObjDesc,
	}
	AggregateObject := graphql.ObjectConfig{
		Name:        "WeaviateNetworkAggregateObj",
		Fields:      aggregateWeaviates,
		Description: descriptions.NetworkAggregateObjDesc,
	}

	return graphql.NewObject(GetObject), graphql.NewObject(GetMetaObject), graphql.NewObject(AggregateObject)
}

func passThroughFiltersAndResolvers(p graphql.ResolveParams) (interface{}, error) {
	resolver, ok := p.Source.(map[string]interface{})["NetworkResolver"].(network_get.Resolver)
	if !ok {
		return nil, fmt.Errorf("source does not contain a NetworkResolver, but \n%#v", p.Source)
	}

	return network_get.FiltersAndResolver{
		Resolver: resolver,
	}, nil
}
