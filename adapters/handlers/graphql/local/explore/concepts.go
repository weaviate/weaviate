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
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// TODO: This is module specific logic, for now we are simply deciding to show
// the nearText option if there is at least one class which has the
// text2vec-contextionary vectorizer module active. This logic does not belong
// here and should be removed with actual modularization
func shouldShowNearText(schema *models.Schema) bool {
	for _, c := range schema.Classes {
		if c.Vectorizer == config.VectorizerModuleText2VecContextionary {
			return true
		}
	}

	return false
}

// Build builds the object containing the Local->Explore Fields, such as Objects
func Build(schema *models.Schema) *graphql.Field {
	field := &graphql.Field{
		Name:        "Explore",
		Description: descriptions.LocalExplore,
		Type:        graphql.NewList(exploreObject()),
		Resolve:     resolve,
		Args: graphql.FieldConfigArgument{
			"limit": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Limit,
			},

			"nearVector": nearVectorArgument(),
			"nearObject": nearObjectArgument(),
		},
	}

	// TODO: this is module-specific and should be added dynamically
	if shouldShowNearText(schema) {
		field.Args["nearText"] = nearTextArgument()
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

// TODO: This is module specific and must be provided by the
// text2vec-contextionary module
func nearTextArgument() *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "ExploreNearTextInpObj",
				Fields:      nearTextFields(),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

// TODO: This is module specific and must be provided by the
// text2vec-contextionary module
func nearTextFields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"moveTo": &graphql.InputObjectFieldConfig{
			Description: descriptions.VectorMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   "ExploreNearTextMoveTo",
					Fields: movementInp("ExploreNearTextMoveTo"),
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
					Name:   "ExploreNearTextMoveAwayFrom",
					Fields: movementInp("ExploreNearTextMoveAwayFrom"),
				}),
		},
	}
}

// TODO: This is module specific and must be provided by the
// text2vec-contextionary module
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
	}
}
