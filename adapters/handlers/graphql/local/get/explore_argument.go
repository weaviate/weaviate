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

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func nearVectorArgument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.GetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearVectorInpObj", prefix),
				Fields: nearVectorFields(prefix),
			},
		),
	}
}

func nearVectorFields(prefix string) graphql.InputObjectConfigFieldMap {
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

func nearObjectArgument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearObjectInpObj", prefix),
				Fields: nearObjectFields(prefix),
			},
		),
	}
}

func nearObjectFields(prefix string) graphql.InputObjectConfigFieldMap {
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
