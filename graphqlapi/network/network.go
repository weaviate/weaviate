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
	"github.com/graphql-go/graphql"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
)

// Build the network queries from the database schema.
func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	
	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are no Actions or Things classes defined yet.")
	}
	
	filterContainer := &utils.FilterContainer{}

	// TODO: placeholder loop, remove this once p2p functionality is up
	weaviateInstances := []string{"WeaviateB", "WeaviateC"}
	weaviateNetworkGetResults := make(map[string]*graphql.Object)
	weaviateNetworkGetMetaResults := make(map[string]*graphql.Object)

	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterContainer.NetworkFilterOptions = make(map[string]*graphql.InputObject)

	// TODO implement function that capitalizes all Weaviate names

	for _, weaviate := range weaviateInstances {

		// This map is used to store all the Thing and Action Objects, so that we can use them in references.
		getNetworkActionsAndThings := make(map[string]*graphql.Object)
		
		getKinds := graphql.Fields{}
		getMetaKinds := graphql.Fields{}

		if len(dbSchema.Actions.Classes) > 0 {
			networkGetActions, networkGetErr := genNetworkActionClassFieldsFromSchema(dbSchema, &getNetworkActionsAndThings, weaviate)
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
			networkGetMetaActions, networkGetMetaErr := genNetworkMetaClassFieldsFromSchema(dbSchema.Actions.Classes, classParentTypeIsAction, weaviate)
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
			networkGetThings, networkGetErr := genNetworkThingClassFieldsFromSchema(dbSchema, &getNetworkActionsAndThings, weaviate)
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
			networkGetMetaThings, networkGetMetaErr := genNetworkMetaClassFieldsFromSchema(dbSchema.Things.Classes, classParentTypeIsAction, weaviate)
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
			Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", weaviate, "Obj"),
			Fields:      getKinds,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkGetWeaviateObjDesc, weaviate),
		})
			
		networkGetMetaObject := graphql.NewObject(graphql.ObjectConfig{
			Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGetMeta", weaviate, "Obj"),
			Fields:      getMetaKinds,
			Description: fmt.Sprintf("%s%s", descriptions.NetworkGetMetaWeaviateObjDesc, weaviate),
		})
		
		weaviateNetworkGetResults[weaviate] = networkGetObject
		weaviateNetworkGetMetaResults[weaviate] = networkGetMetaObject

	}
	// TODO this is a temp function, inserts a temp weaviate obj in between Get and Things/Actions
	networkGetObject, networkGetMetaObject := insertDummyNetworkWeaviateField(weaviateNetworkGetResults, weaviateNetworkGetMetaResults)

	genGlobalNetworkFilterElements(filterContainer)

	networkFetchObj := genFieldsObjForNetworkFetch(filterContainer)

	networkIntrospectObj := genFieldsObjForNetworkIntrospect(filterContainer)

	graphQLNetworkFieldContents := utils.GraphQLNetworkFieldContents{
		networkGetObject,
		networkGetMetaObject,
		networkFetchObj,
		networkIntrospectObj,
	}

	networkGetAndGetMetaObject := genNetworkFields(&graphQLNetworkFieldContents, filterContainer)

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
			return nil, fmt.Errorf("not supported")
		},
	}

	return networkField, nil
}