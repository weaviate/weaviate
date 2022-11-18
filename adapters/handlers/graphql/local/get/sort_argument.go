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

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/tailor-inc/graphql"
)

func sortArgument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewList(
			graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sSortInpObj", prefix),
					Fields:      sortFields(prefix),
					Description: descriptions.GetWhereInpObj,
				},
			),
		),
	}
}

func sortFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"path": &graphql.InputObjectFieldConfig{
			Description: descriptions.SortPath,
			Type:        graphql.NewList(graphql.String),
		},
		"order": &graphql.InputObjectFieldConfig{
			Description: descriptions.SortOrder,
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: fmt.Sprintf("%sSortInpObjTypeEnum", prefix),
				Values: graphql.EnumValueConfigMap{
					"asc":  &graphql.EnumValueConfig{},
					"desc": &graphql.EnumValueConfig{},
				},
			}),
		},
	}
}
