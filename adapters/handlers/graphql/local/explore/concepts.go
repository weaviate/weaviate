//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package explore

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

type ModulesProvider interface {
	ExploreArguments(schema *models.Schema) map[string]*graphql.ArgumentConfig
	CrossClassExtractSearchParams(arguments map[string]interface{}) map[string]interface{}
}

// Build builds the object containing the Local->Explore Fields, such as Objects
func Build(schema *models.Schema, modulesProvider ModulesProvider) *graphql.Field {
	field := &graphql.Field{
		Name:        "Explore",
		Description: descriptions.LocalExplore,
		Type:        graphql.NewList(exploreObject()),
		Resolve:     newResolver(modulesProvider).resolve,
		Args: graphql.FieldConfigArgument{
			"offset": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Offset,
			},
			"limit": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Limit,
			},

			"nearVector": nearVectorArgument(),
			"nearObject": nearObjectArgument(),
		},
	}

	if modulesProvider != nil {
		for name, argument := range modulesProvider.ExploreArguments(schema) {
			field.Args[name] = argument
		}
	}

	return field
}

func exploreObject() *graphql.Object {
	getLocalExploreFields := graphql.Fields{
		"className": &graphql.Field{
			Name:        "ExploreClassName",
			Description: descriptions.ClassName,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(search.Result)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.ClassName, nil
			},
		},

		"beacon": &graphql.Field{
			Name:        "ExploreBeacon",
			Description: descriptions.Beacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(search.Result)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Beacon, nil
			},
		},

		"certainty": &graphql.Field{
			Name:        "ExploreCertainty",
			Description: descriptions.Certainty,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(search.Result)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Certainty, nil
			},
		},

		"distance": &graphql.Field{
			Name:        "ExploreDistance",
			Description: descriptions.Distance,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(search.Result)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Dist, nil
			},
		},
	}

	getLocalExploreFieldsObject := graphql.ObjectConfig{
		Name:        "ExploreObj",
		Fields:      getLocalExploreFields,
		Description: descriptions.LocalExplore,
	}

	return graphql.NewObject(getLocalExploreFieldsObject)
}

func nearVectorArgument() *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		// Description: descriptions.GetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   "ExploreNearVectorInpObj",
				Fields: nearVectorFields(),
			},
		),
	}
}

func nearVectorFields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"vector": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.Float)),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
	}
}

func nearObjectArgument() *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   "ExploreNearObjectInpObj",
				Fields: nearObjectFields(),
			},
		),
	}
}

func nearObjectFields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"id": &graphql.InputObjectFieldConfig{
			Description: descriptions.ID,
			Type:        graphql.String,
		},
		"beacon": &graphql.InputObjectFieldConfig{
			Description: descriptions.Beacon,
			Type:        graphql.String,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
	}
}
