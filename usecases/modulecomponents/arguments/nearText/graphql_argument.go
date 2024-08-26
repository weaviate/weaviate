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

package nearText

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func (g *GraphQLArgumentsProvider) getNearTextArgumentFn(classname string) *graphql.ArgumentConfig {
	return g.nearTextArgument("GetObjects", classname, true)
}

func (g *GraphQLArgumentsProvider) aggregateNearTextArgumentFn(classname string) *graphql.ArgumentConfig {
	return g.nearTextArgument("Aggregate", classname, false)
}

func (g *GraphQLArgumentsProvider) exploreNearTextArgumentFn() *graphql.ArgumentConfig {
	return g.nearTextArgument("Explore", "", false)
}

func (g *GraphQLArgumentsProvider) nearTextArgument(prefix, className string, addTarget bool) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearTextInpObj", prefixName),
				Fields:      g.nearTextFields(prefixName, addTarget),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func (g *GraphQLArgumentsProvider) nearTextFields(prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	nearTextFields := graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"moveTo": &graphql.InputObjectFieldConfig{
			Description: descriptions.VectorMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveTo", prefix),
					Fields: g.movementInp(fmt.Sprintf("%sMoveTo", prefix)),
				}),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
		"moveAwayFrom": &graphql.InputObjectFieldConfig{
			Description: descriptions.VectorMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveAwayFrom", prefix),
					Fields: g.movementInp(fmt.Sprintf("%sMoveAwayFrom", prefix)),
				}),
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
	if g.nearTextTransformer != nil {
		nearTextFields["autocorrect"] = &graphql.InputObjectFieldConfig{
			Description: "Autocorrect input text values",
			Type:        graphql.Boolean,
		}
	}
	nearTextFields = common_filters.AddTargetArgument(nearTextFields, prefix+"nearText", addTarget)
	return nearTextFields
}

func (g *GraphQLArgumentsProvider) movementInp(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			Description: descriptions.Keywords,
			Type:        graphql.NewList(graphql.String),
		},
		"objects": &graphql.InputObjectFieldConfig{
			Description: "objects",
			Type:        graphql.NewList(g.objectsInpObj(prefix)),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}

func (g *GraphQLArgumentsProvider) objectsInpObj(prefix string) *graphql.InputObject {
	return graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: fmt.Sprintf("%sMovementObjectsInpObj", prefix),
			Fields: graphql.InputObjectConfigFieldMap{
				"id": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: "id of an object",
				},
				"beacon": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: descriptions.Beacon,
				},
			},
			Description: "Movement Object",
		},
	)
}
