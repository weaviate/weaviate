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

package generate

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *GenerateProvider) additionalGenerateField(classname string) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"singleResult": &graphql.ArgumentConfig{
				Description: "Results per object",
				Type: graphql.NewInputObject(graphql.InputObjectConfig{
					Name: fmt.Sprintf("%sIndividualResultsArg", classname),
					Fields: graphql.InputObjectConfigFieldMap{
						"prompt": &graphql.InputObjectFieldConfig{
							Description: "prompt",
							Type:        graphql.String,
						},
					},
				}),
				DefaultValue: nil,
			},
			"groupedResult": &graphql.ArgumentConfig{
				Description: "Grouped results of all objects",
				Type: graphql.NewInputObject(graphql.InputObjectConfig{
					Name: fmt.Sprintf("%sAllResultsArg", classname),
					Fields: graphql.InputObjectConfigFieldMap{
						"task": &graphql.InputObjectFieldConfig{
							Description: "task",
							Type:        graphql.String,
						},
						"properties": &graphql.InputObjectFieldConfig{
							Description:  "Properties used for the generation",
							Type:         graphql.NewList(graphql.String),
							DefaultValue: nil,
						},
					},
				}),
				DefaultValue: nil,
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalGenerate", classname),
			Fields: graphql.Fields{
				"singleResult":  &graphql.Field{Type: graphql.String},
				"groupedResult": &graphql.Field{Type: graphql.String},
				"error":         &graphql.Field{Type: graphql.String},
			},
		}),
	}
}
