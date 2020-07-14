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

package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func groupArgument(kindName, className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("Get%ss%s", kindName, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.GetGroup,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sGroupInpObj", prefix),
				Fields:      groupFields(prefix),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func groupFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"type": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: fmt.Sprintf("%sGroupInpObjTypeEnum", prefix),
				Values: graphql.EnumValueConfigMap{
					"closest": &graphql.EnumValueConfig{},
					"merge":   &graphql.EnumValueConfig{},
				},
			}),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}
