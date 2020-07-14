//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package explore

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/search"
)

// Build builds the object containing the Local->Explore Fields, such as Things/Actions
func Build() *graphql.Field {
	return &graphql.Field{
		Name:        "Explore",
		Description: descriptions.LocalExplore,
		Type:        graphql.NewList(exploreObject()),
		Resolve:     resolve,
		Args: graphql.FieldConfigArgument{
			"network": &graphql.ArgumentConfig{
				Description: descriptions.Network,
				Type:        graphql.Boolean,
			},
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
						Name:   "ExploreMoveTo",
						Fields: movementInp(),
					}),
			},
			"moveAwayFrom": &graphql.ArgumentConfig{
				Description: descriptions.VectorMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "ExploreMoveAwayFrom",
						Fields: movementInp(),
					}),
			},
		},
	}
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
			Name:        "ExploreBeacon",
			Description: descriptions.Distance,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(search.Result)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Certainty, nil
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
