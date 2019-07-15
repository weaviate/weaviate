/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package explore

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Build builds the object containing the Local->Explore Fields, such as Things/Actions
func Build() *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateLocalExplore",
		Description: descriptions.LocalExplore,
		Type:        graphql.NewList(exploreObject()),
		Resolve:     resolve,
		Args: graphql.FieldConfigArgument{
			"concepts": &graphql.ArgumentConfig{
				Description: descriptions.Keywords,
				Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
			"limit": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Limit,
			},
			"certainty": &graphql.ArgumentConfig{
				Type:        graphql.Float,
				Description: descriptions.Certainty,
			},
			"moveTo": &graphql.ArgumentConfig{
				Description: descriptions.VectorMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveTo",
						Fields: movementInp(),
					}),
			},
			"moveAwayFrom": &graphql.ArgumentConfig{
				Description: descriptions.VectorMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveAwayFrom",
						Fields: movementInp(),
					}),
			},
		},
	}
}

func exploreObject() *graphql.Object {
	getLocalExploreFields := graphql.Fields{
		"className": &graphql.Field{
			Name:        "WeaviateLocalExploreClassName",
			Description: descriptions.ClassName,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.ClassName, nil
			},
		},

		"beacon": &graphql.Field{
			Name:        "WeaviateLocalExploreBeacon",
			Description: descriptions.Beacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Beacon, nil
			},
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateLocalExploreBeacon",
			Description: descriptions.Distance,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Certainty, nil
			},
		},
	}

	getLocalExploreFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalExploreObj",
		Fields:      getLocalExploreFields,
		Description: descriptions.LocalExplore,
	}

	return graphql.NewObject(getLocalExploreFieldsObject)
}

func movementInp() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			Description: descriptions.Keywords,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}
