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

func (p *GenerateProvider) additionalGenerateField(className string) *graphql.Field {
	generate := &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"singleResult": &graphql.ArgumentConfig{
				Description: "Results per object",
				Type: graphql.NewInputObject(graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sIndividualResultsArg", className),
					Fields: p.singleResultArguments(className),
				}),
				DefaultValue: nil,
			},
			"groupedResult": &graphql.ArgumentConfig{
				Description: "Grouped results of all objects",
				Type: graphql.NewInputObject(graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sAllResultsArg", className),
					Fields: p.groupedResultArguments(className),
				}),
				DefaultValue: nil,
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:   fmt.Sprintf("%sAdditionalGenerate", className),
			Fields: p.fields(className),
		}),
	}
	return generate
}

func (p *GenerateProvider) singleResultArguments(className string) graphql.InputObjectConfigFieldMap {
	argumentFields := graphql.InputObjectConfigFieldMap{
		"prompt": &graphql.InputObjectFieldConfig{
			Description: "prompt",
			Type:        graphql.String,
		},
		"debug": &graphql.InputObjectFieldConfig{
			Description: "debug",
			Type:        graphql.Boolean,
		},
	}
	p.inputArguments(argumentFields, fmt.Sprintf("%sSingleResult", className))
	return argumentFields
}

func (p *GenerateProvider) groupedResultArguments(className string) graphql.InputObjectConfigFieldMap {
	argumentFields := graphql.InputObjectConfigFieldMap{
		"task": &graphql.InputObjectFieldConfig{
			Description: "task",
			Type:        graphql.String,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description:  "Properties used for the generation",
			Type:         graphql.NewList(graphql.String),
			DefaultValue: nil,
		},
		"debug": &graphql.InputObjectFieldConfig{
			Description: "debug",
			Type:        graphql.Boolean,
		},
	}
	p.inputArguments(argumentFields, fmt.Sprintf("%sGroupedResult", className))
	return argumentFields
}

func (p *GenerateProvider) inputArguments(argumentFields graphql.InputObjectConfigFieldMap, prefix string) {
	// Dynamic RAG syntax generative module specific request parameters
	for name, generativeParameters := range p.additionalGenerativeParameters {
		if generativeParameters.RequestParamsFunction != nil {
			argumentFields[name] = generativeParameters.RequestParamsFunction(prefix)
		}
	}
}

func (p *GenerateProvider) fields(className string) graphql.Fields {
	fields := graphql.Fields{
		"singleResult":  &graphql.Field{Type: graphql.String},
		"groupedResult": &graphql.Field{Type: graphql.String},
		"error":         &graphql.Field{Type: graphql.String},
		"debug": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sDebugFields", className),
			Fields: graphql.Fields{
				"prompt": &graphql.Field{Type: graphql.String},
			},
		})},
	}
	// Dynamic RAG syntax generative module specific response parameters
	for name, generativeParameters := range p.additionalGenerativeParameters {
		if generativeParameters.ResponseParamsFunction != nil {
			fields[name] = generativeParameters.ResponseParamsFunction(className)
		}
	}
	return fields
}
