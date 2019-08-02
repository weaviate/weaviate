//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func exploreArgument(kindName, className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("Get%ss%s", kindName, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.LocalGetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sExploreInpObj", prefix),
				Fields:      exploreFields(prefix),
				Description: descriptions.LocalGetWhereInpObj,
			},
		),
	}
}

func exploreFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"moveTo": &graphql.InputObjectFieldConfig{
			Description: descriptions.VectorMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveTo", prefix),
					Fields: movementInp(),
				}),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"moveAwayFrom": &graphql.InputObjectFieldConfig{
			Description: descriptions.VectorMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveAwayFrom", prefix),
					Fields: movementInp(),
				}),
		},
	}
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
