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
				Name:   fmt.Sprintf("%shybridInpObj", prefix),
				Fields: 
					//hybridFields(prefix),
					hybridOperands(classObject, class, modulesProvider),
				Description: "hello",
			},
		),
	}
}

func hybridOperands (classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider) graphql.InputObjectConfigFieldMap {
		
		/*ss :=graphql.NewInputObject(graphql.InputObjectConfig{
			Name: class.Class+ "SubSearch",
			Fields: hybridFields("SubSearch"),
		})
		*/
		return graphql.InputObjectConfigFieldMap{
			"operands":&graphql.InputObjectFieldConfig{
			Description: "Subsearch list",
			
					Type: graphql.NewList(graphql.String),
			
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
