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

package parameters

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func input(prefix string) *graphql.InputObjectFieldConfig {
	return &graphql.InputObjectFieldConfig{
		Description: fmt.Sprintf("%s settings", Name),
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name: fmt.Sprintf("%s%sInputObject", prefix, Name),
			Fields: graphql.InputObjectConfigFieldMap{
				"service": &graphql.InputObjectFieldConfig{
					Description: "service",
					Type:        graphql.String,
				},
				"region": &graphql.InputObjectFieldConfig{
					Description: "region",
					Type:        graphql.String,
				},
				"endpoint": &graphql.InputObjectFieldConfig{
					Description: "endpoint",
					Type:        graphql.String,
				},
				"targetModel": &graphql.InputObjectFieldConfig{
					Description: "targetModel",
					Type:        graphql.String,
				},
				"targetVariant": &graphql.InputObjectFieldConfig{
					Description: "targetVariant",
					Type:        graphql.String,
				},
				"model": &graphql.InputObjectFieldConfig{
					Description: "model",
					Type:        graphql.String,
				},
				"temperature": &graphql.InputObjectFieldConfig{
					Description: "temperature",
					Type:        graphql.Float,
				},
				"images": &graphql.InputObjectFieldConfig{
					Description: "images",
					Type:        graphql.NewList(graphql.String),
				},
				"imageProperties": &graphql.InputObjectFieldConfig{
					Description: "imageProperties",
					Type:        graphql.NewList(graphql.String),
				},
			},
		}),
		DefaultValue: nil,
	}
}
