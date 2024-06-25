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

package common_filters

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func NearVectorArgument(argumentPrefix, className string, addTarget bool) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("%s%s", argumentPrefix, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.GetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearVectorInpObj", prefix),
				Fields: NearVectorFields(prefix, addTarget),
			},
		),
	}
}

func NearVectorFields(prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	fieldMap := graphql.InputObjectConfigFieldMap{
		"vector": &graphql.InputObjectFieldConfig{
			Description: descriptions.Vector,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.Float)),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
	fieldMap = AddTargetArgument(fieldMap, prefix+"nearVector", addTarget)
	return fieldMap
}

func NearObjectArgument(argumentPrefix, className string, addTarget bool) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("%s%s", argumentPrefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearObjectInpObj", prefix),
				Fields: nearObjectFields(prefix, addTarget),
			},
		),
	}
}

func nearObjectFields(prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	fieldMap := graphql.InputObjectConfigFieldMap{
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
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
	fieldMap = AddTargetArgument(fieldMap, prefix+"nearObject", addTarget)
	return fieldMap
}
