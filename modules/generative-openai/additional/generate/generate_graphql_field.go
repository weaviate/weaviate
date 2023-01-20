//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
			"task": &graphql.ArgumentConfig{
				Description:  "Task to be performed",
				Type:         graphql.String,
				DefaultValue: nil,
			},
			"resultLanguage": &graphql.ArgumentConfig{
				Description: "Result language",
				Type: graphql.NewEnum(graphql.EnumConfig{
					Name: fmt.Sprintf("%sAdditionalArgumentResultLanguageGenerate", classname),
					Values: graphql.EnumValueConfigMap{
						"English":    &graphql.EnumValueConfig{},
						"Chinese":    &graphql.EnumValueConfig{},
						"Spanish":    &graphql.EnumValueConfig{},
						"French":     &graphql.EnumValueConfig{},
						"Arabic":     &graphql.EnumValueConfig{},
						"Russian":    &graphql.EnumValueConfig{},
						"Portuguese": &graphql.EnumValueConfig{},
						"Japanese":   &graphql.EnumValueConfig{},
						"German":     &graphql.EnumValueConfig{},
						"Dutch":      &graphql.EnumValueConfig{},
						"auto":       &graphql.EnumValueConfig{},
					},
				}),
				DefaultValue: nil,
			},
			"properties": &graphql.ArgumentConfig{
				Description:  "Properties used for the generation",
				Type:         graphql.NewList(graphql.String),
				DefaultValue: nil,
			},
			"onSet": &graphql.ArgumentConfig{
				Description: "Generate all over the results",
				Type: graphql.NewEnum(graphql.EnumConfig{
					Name: fmt.Sprintf("%sAdditionalArgumentOnSetGenerate", classname),
					Values: graphql.EnumValueConfigMap{
						"allResults":        &graphql.EnumValueConfig{},
						"individualResults": &graphql.EnumValueConfig{},
					},
				}),
				DefaultValue: nil,
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalGenerate", classname),
			Fields: graphql.Fields{
				"result": &graphql.Field{Type: graphql.String},
			},
		}),
	}
}
