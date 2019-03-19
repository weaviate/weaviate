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

// Package network provides the network graphql endpoint for Weaviate
package network

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
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

type schemaDependentObjects struct {
	get       *graphql.Object
	getMeta   *graphql.Object
	aggregate *graphql.Object
}

// Build the network queries from the database schema.
func Build(peers peers.Peers, config config.Environment) (*graphql.Field, error) {

	filterContainer := &utils.FilterContainer{}

	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterContainer.NetworkFilterOptions = make(map[string]*graphql.InputObject)

	if len(peers) == 0 {
		// Don't error, but also don't register the Network  field if we don't
		// have any peers. This build function will be called again if the
		// peers change, so next time it might advance past here.
		return nil, nil
	}

	schemaObjects, err := buildSchemaDependentObjects(peers)
	if err != nil {
		return nil, fmt.Errorf("could not build schema-dependent objects: %s", err)
	}

	if schemaObjects == nil {
		// if we don't have any peers with schemas, we effectively don't have
		// a Network Field.
		// We should not error though, because local queries are still possible.
		return nil, nil
	}

	genGlobalNetworkFilterElements(filterContainer)

	networkFetchObject := network_fetch.FieldsObj(filterContainer)

	networkIntrospectObject := network_introspect.FieldsObj(filterContainer)

	graphQLNetworkFieldContents := utils.GraphQLNetworkFieldContents{
		NetworkGetObject:        schemaObjects.get,
		NetworkGetMetaObject:    schemaObjects.getMeta,
		NetworkFetchObject:      networkFetchObject,
		NetworkIntrospectObject: networkIntrospectObject,
		NetworkAggregateObject:  schemaObjects.aggregate,
	}
	object := genNetworkFields(&graphQLNetworkFieldContents /*, filterContainer*/)

	networkField := &graphql.Field{
		Type:        object,
		Description: descriptions.WeaviateNetwork,
		Args: graphql.FieldConfigArgument{
			"networkTimeout": &graphql.ArgumentConfig{
				Description: descriptions.NetworkTimeout,
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
			Name:        descriptions.NetworkGet,
			Type:        graphQLNetworkFieldContents.NetworkGetObject,
			Description: descriptions.NetworkGet,
			Resolve:     passThroughGetFiltersAndResolvers,
		},

		"GetMeta": &graphql.Field{
			Name:        "WeaviateNetworkGetMeta",
			Type:        graphQLNetworkFieldContents.NetworkGetMetaObject,
			Description: descriptions.NetworkGetMeta,
			Resolve:     passThroughGetMetaResolvers,
		},

		"Fetch": &graphql.Field{
			Name:        "WeaviateNetworkFetch",
			Type:        graphQLNetworkFieldContents.NetworkFetchObject,
			Description: descriptions.NetworkFetch,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"Introspect": &graphql.Field{
			Name:        "WeaviateNetworkIntrospection",
			Type:        graphQLNetworkFieldContents.NetworkIntrospectObject,
			Description: descriptions.NetworkIntrospect,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"Aggregate": &graphql.Field{
			Name:        "WeaviateNetworkAggregate",
			Type:        graphQLNetworkFieldContents.NetworkAggregateObject,
			Description: descriptions.NetworkAggregate,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	weaviateNetworkObject := &graphql.ObjectConfig{
		Name:        "WeaviateNetworkObj",
		Fields:      networkGetAndGetMetaFields,
		Description: descriptions.NetworkObj,
	}

	return graphql.NewObject(*weaviateNetworkObject)
}

func buildSchemaDependentObjects(peers peers.Peers) (*schemaDependentObjects, error) {
	if len(peers) == 0 {
		// if we don't have any peers, we must return nil
		// otherwise we'd have an empty Get and GetMeta object, which
		// is not valid GraphQL
		return nil, nil
	}

	getPeers := graphql.Fields{}
	metaGetPeers := graphql.Fields{}
	aggregatePeers := graphql.Fields{}

	for _, peer := range peers {
		get, err := network_get.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			return nil, fmt.Errorf("could not build Get for peer '%s': %s", peer.Name, err)
		}
		getPeers[peer.Name] = get

		getMeta, err := network_getmeta.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			return nil, fmt.Errorf("could not build GetMeta for peer '%s': %s", peer.Name, err)
		}
		metaGetPeers[peer.Name] = getMeta

		aggregate, err := network_aggregate.New(peer.Name, peer.Schema).PeerField()
		if err != nil {
			return nil, fmt.Errorf("could not build Aggregate for peer '%s': %s", peer.Name, err)
		}
		aggregatePeers[peer.Name] = aggregate
	}

	get := graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetObj",
		Fields:      getPeers,
		Description: descriptions.NetworkGetObj,
	})
	getMeta := graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateNetworkGetMetaObj",
		Fields:      metaGetPeers,
		Description: descriptions.NetworkGetMetaObj,
	})
	aggregate := graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateNetworkAggregateObj",
		Fields:      aggregatePeers,
		Description: descriptions.NetworkAggregateObj,
	})

	return &schemaDependentObjects{
		get:       get,
		getMeta:   getMeta,
		aggregate: aggregate,
	}, nil
}

func passThroughGetFiltersAndResolvers(p graphql.ResolveParams) (interface{}, error) {
	resolver, ok := p.Source.(map[string]interface{})["NetworkResolver"].(network_get.Resolver)
	if !ok {
		return nil, fmt.Errorf("source does not contain a NetworkResolver, but \n%#v", p.Source)
	}

	return network_get.FiltersAndResolver{
		Resolver: resolver,
	}, nil
}

func passThroughGetMetaResolvers(p graphql.ResolveParams) (interface{}, error) {
	resolver, ok := p.Source.(map[string]interface{})["NetworkResolver"].(network_getmeta.Resolver)
	if !ok {
		return nil, fmt.Errorf("source does not contain a NetworkResolver, but \n%#v", p.Source)
	}

	return resolver, nil
}
