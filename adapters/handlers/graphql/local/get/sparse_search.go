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

	"github.com/tailor-inc/graphql"
)

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
			Description: "The query to search for",
			Type:        graphql.String,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "The properties to search in",
			Type:        graphql.NewList(graphql.String),
		},
	}
}
