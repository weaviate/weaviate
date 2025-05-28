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

package aggregate

import (
	"fmt"
	"os"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func hybridArgument(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider,
) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("AggregateObjects%s", class.Class)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sHybridInpObj", prefix),
				Fields:      hybridOperands(classObject, class, modulesProvider),
				Description: "Hybrid search",
			},
		),
	}
}

func hybridOperands(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider,
) graphql.InputObjectConfigFieldMap {
	ss := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   class.Class + "HybridSubSearch",
		Fields: hybridSubSearch(classObject, class, modulesProvider),
	})
	prefixName := class.Class + "HybridSubSearch"
	searchesPrefixName := prefixName + "Searches"
	fieldMap := graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			Description: "Query string",
			Type:        graphql.String,
		},
		"alpha": &graphql.InputObjectFieldConfig{
			Description: "Search weight",
			Type:        graphql.Float,
		},
		"maxVectorDistance": &graphql.InputObjectFieldConfig{
			Description: "Removes all results that have a vector distance larger than the given value",
			Type:        graphql.Float,
		},
		"vector": &graphql.InputObjectFieldConfig{
			Description: "Vector search",
			Type:        common_filters.Vector(prefixName),
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "Properties to search",
			Type:        graphql.NewList(graphql.String),
		},
		"bm25SearchOperator": common_filters.GenerateBM25SearchOperatorFields(prefixName),
		"searches": &graphql.InputObjectFieldConfig{
			Description: "Subsearch list",
			Type: graphql.NewList(graphql.NewInputObject(
				graphql.InputObjectConfig{
					Description: "Subsearch list",
					Name:        fmt.Sprintf("%sSearchesInpObj", searchesPrefixName),
					Fields: (func() graphql.InputObjectConfigFieldMap {
						subSearchFields := make(graphql.InputObjectConfigFieldMap)
						fieldMap := graphql.InputObjectConfigFieldMap{
							"nearText": &graphql.InputObjectFieldConfig{
								Description: "nearText element",

								Type: graphql.NewInputObject(
									graphql.InputObjectConfig{
										Name:        fmt.Sprintf("%sNearTextInpObj", searchesPrefixName),
										Fields:      nearTextFields(searchesPrefixName),
										Description: "Near text search",
									},
								),
							},
							"nearVector": &graphql.InputObjectFieldConfig{
								Description: "nearVector element",
								Type: graphql.NewInputObject(
									graphql.InputObjectConfig{
										Name:        fmt.Sprintf("%sNearVectorInpObj", searchesPrefixName),
										Description: "Near vector search",
										Fields:      common_filters.NearVectorFields(searchesPrefixName, false),
									},
								),
							},
						}
						for key, fieldConfig := range fieldMap {
							subSearchFields[key] = fieldConfig
						}
						return subSearchFields
					})(),
				},
			)),
		},
	}

	if os.Getenv("ENABLE_EXPERIMENTAL_HYBRID_OPERANDS") != "" {
		fieldMap["operands"] = &graphql.InputObjectFieldConfig{
			Description: "Subsearch list",
			Type:        graphql.NewList(ss),
		}
	}

	return fieldMap
}

func hybridSubSearch(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider,
) graphql.InputObjectConfigFieldMap {
	prefixName := class.Class + "SubSearch"

	return graphql.InputObjectConfigFieldMap{
		"weight": &graphql.InputObjectFieldConfig{
			Description: "weight, 0 to 1",
			Type:        graphql.Float,
		},
		"sparseSearch": &graphql.InputObjectFieldConfig{
			Description: "Sparse Search",
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sHybridAggregateBM25InpObj", prefixName),
					Fields:      bm25Fields(prefixName),
					Description: "BM25f search",
				},
			),
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
