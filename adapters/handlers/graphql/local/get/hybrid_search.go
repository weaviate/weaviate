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

package get

import (
	"fmt"
	"os"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/models"
)

func hybridArgument(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider, fusionEnum *graphql.Enum,
) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", class.Class)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sHybridInpObj", prefix),
				Fields:      hybridOperands(classObject, class, modulesProvider, fusionEnum),
				Description: "Hybrid search",
			},
		),
	}
}

func hybridOperands(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider, fusionEnum *graphql.Enum,
) graphql.InputObjectConfigFieldMap {
	ss := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   class.Class + "SubSearch",
		Fields: hybridSubSearch(classObject, class, modulesProvider),
	})

	prefixName := class.Class + "SubSearch"

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
			Type:        graphql.NewList(graphql.Float),
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "Which properties should be included in the sparse search",
			Type:        graphql.NewList(graphql.String),
		},
		"fusionType": &graphql.InputObjectFieldConfig{
			Description: "Algorithm used for fusing results from vector and keyword search",
			Type:        fusionEnum,
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
		"bm25SearchOperator": common_filters.GenerateBM25SearchOperatorFields(prefixName),

		"searches": &graphql.InputObjectFieldConfig{
			Description: "Subsearch list",
			Type: graphql.NewList(graphql.NewInputObject(
				graphql.InputObjectConfig{
					Description: "Subsearch list",
					Name:        fmt.Sprintf("%sSearchesInpObj", prefixName),
					Fields: (func() graphql.InputObjectConfigFieldMap {
						subSearchFields := make(graphql.InputObjectConfigFieldMap)
						fieldMap := graphql.InputObjectConfigFieldMap{
							"nearText": &graphql.InputObjectFieldConfig{
								Description: "nearText element",

								Type: graphql.NewInputObject(
									graphql.InputObjectConfig{
										Name:        fmt.Sprintf("%sNearTextInpObj", prefixName),
										Fields:      nearTextFields(prefixName),
										Description: "Near text search",
									},
								),
							},
							"nearVector": &graphql.InputObjectFieldConfig{
								Description: "nearVector element",
								Type: graphql.NewInputObject(
									graphql.InputObjectConfig{
										Name:        fmt.Sprintf("%sNearVectorInpObj", prefixName),
										Description: "Near vector search",
										Fields:      common_filters.NearVectorFields(prefixName, true),
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
	fieldMap = common_filters.AddTargetArgument(fieldMap, prefixName+"hybrid", true)

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
					Name:        fmt.Sprintf("%sHybridGetBM25InpObj", prefixName),
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
