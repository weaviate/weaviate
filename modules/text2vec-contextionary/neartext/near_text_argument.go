//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package neartext

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func nearTextArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Txt2VecC11y%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearTextInpObj", prefixName),
				Fields:      nearTextFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearTextFields(prefix string) graphql.InputObjectConfigFieldMap {
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
					Fields: movementInp(fmt.Sprintf("%sMoveTo", prefix)),
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
					Fields: movementInp(fmt.Sprintf("%sMoveAwayFrom", prefix)),
				}),
		},
	}
}

func movementInp(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			Description: descriptions.Keywords,
			Type:        graphql.NewList(graphql.String),
		},
		"objects": &graphql.InputObjectFieldConfig{
			Description: "objects",
			Type:        graphql.NewList(objectsInpObj(prefix)),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}

func objectsInpObj(prefix string) *graphql.InputObject {
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
