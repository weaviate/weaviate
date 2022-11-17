//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package get

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/tailor-inc/graphql"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/models"
)

func nearVectorArgument(className string) *graphql.ArgumentConfig {
	return common_filters.NearVectorArgument("GetObjects", className)
}

func nearObjectArgument(className string) *graphql.ArgumentConfig {
	return common_filters.NearObjectArgument("GetObjects", className)
}

func bm25Argument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sBm25InpObj", prefix),
				Fields: bm25Fields(prefix),
			},
		),
	}
}

func bm25Fields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			// Description: descriptions.ID,
			Type: graphql.String,
		},
		"properties": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Beacon,
			Type: graphql.NewList(graphql.String),
		},
	}
}

func hybridArgument(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", class.Class)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: fmt.Sprintf("%shybridInpObj", prefix),
				Fields:
				//hybridFields(prefix),
				hybridOperands(classObject, class, modulesProvider),
				Description: "hello",
			},
		),
	}
}

func hybridOperands(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider) graphql.InputObjectConfigFieldMap {

	ss := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   class.Class + "SubSearch",
		Fields: hybridSubSearch(classObject, class, modulesProvider),
	})

	return graphql.InputObjectConfigFieldMap{
		"operands": &graphql.InputObjectFieldConfig{
			Description: "Subsearch list",

			Type: graphql.NewList(ss),
		},
	}
}

func hybridSubSearch(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider) graphql.InputObjectConfigFieldMap {
	prefixName := class.Class + "SubSearch"


	return graphql.InputObjectConfigFieldMap{
		"weight": &graphql.InputObjectFieldConfig{
			// Description: descriptions.ID,
			Type: graphql.Float,
		},
		"sparseSearch": &graphql.InputObjectFieldConfig{
			Description: "Sparse Search",
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sBM25InpObj", prefixName),
					Fields:      bm25Fields(prefixName),
					Description: "BM25f search",
				},
			),
		},

		"nearText": &graphql.InputObjectFieldConfig{
			Description: "nearText element",

			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sNearTextInpObj", prefixName),
					Fields:      nearTextFields(prefixName),
					Description: descriptions.GetWhereInpObj,
				},
			),
		},
	}

}

func hybridFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			// Description: descriptions.ID,
			Type: graphql.String,
		},
		"alpha": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Beacon,
			Type: graphql.Float,
		},
		"vector": &graphql.InputObjectFieldConfig{
			Description: descriptions.Vector,
			Type:        graphql.NewList(graphql.Float),
		},
	}
}

func nearTextFields(prefix string) graphql.InputObjectConfigFieldMap {
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
					Fields: movementInp(fmt.Sprintf("%sMoveTo", prefix)),
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
					Fields: movementInp(fmt.Sprintf("%sMoveAwayFrom", prefix)),
				}),
		},
	}
	return nearTextFields
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
