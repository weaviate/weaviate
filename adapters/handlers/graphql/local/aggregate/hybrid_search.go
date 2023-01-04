package aggregate

import (
	"fmt"
	"os"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/tailor-inc/graphql"
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
		Name:   class.Class + "SubSearch",
		Fields: hybridSubSearch(classObject, class, modulesProvider),
	})
	fieldMap := graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			Description: "Query string",
			Type:        graphql.String,
		},
		"alpha": &graphql.InputObjectFieldConfig{
			Description: "Search weight",
			Type:        graphql.Float,
		},
		"vector": &graphql.InputObjectFieldConfig{
			Description: "Vector search",
			Type:        graphql.NewList(graphql.Float),
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
					Name:        fmt.Sprintf("%sBM25InpObj", prefixName),
					Fields:      bm25Fields(prefixName),
					Description: "BM25f search",
				},
			),
		},

		//"nearText": &graphql.InputObjectFieldConfig{
		//	Description: "nearText element",
		//
		//	Type: graphql.NewInputObject(
		//		graphql.InputObjectConfig{
		//			Name:        fmt.Sprintf("%sNearTextInpObj", prefixName),
		//			Fields:      nearTextFields(prefixName),
		//			Description: descriptions.GetWhereInpObj,
		//		},
		//	),
		//},
	}
}
