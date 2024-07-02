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
	"log"

	"github.com/tailor-inc/graphql/language/ast"
)

func (p *GenerateProvider) parseGenerateArguments(args []*ast.Argument) *Params {
	out := &Params{Options: make(map[string]interface{})}

	for _, arg := range args {
		switch arg.Name.Value {
		case "singleResult":
			obj := arg.Value.(*ast.ObjectValue).Fields
			for _, field := range obj {
				switch field.Name.Value {
				case "prompt":
					out.Prompt = &field.Value.(*ast.StringValue).Value
				case "debug":
					out.Debug = field.Value.(*ast.BooleanValue).Value
				default:
					if p.isDynamicRAGSyntaxEnabled {
						if value := p.extractGenerativeParameter(field); value != nil {
							out.Options[field.Name.Value] = value
						}
					}
				}
			}
		case "groupedResult":
			obj := arg.Value.(*ast.ObjectValue).Fields
			for _, field := range obj {
				switch field.Name.Value {
				case "task":
					out.Task = &field.Value.(*ast.StringValue).Value
				case "properties":
					inp := field.Value.GetValue().([]ast.Value)
					out.Properties = make([]string, len(inp))

					for i, value := range inp {
						out.Properties[i] = value.(*ast.StringValue).Value
					}
				case "debug":
					out.Debug = field.Value.(*ast.BooleanValue).Value
				default:
					if p.isDynamicRAGSyntaxEnabled {
						if value := p.extractGenerativeParameter(field); value != nil {
							out.Options[field.Name.Value] = value
						}
					}
				}
			}

		default:
			// ignore what we don't recognize
			log.Printf("Igonore not recognized value: %v", arg.Name.Value)
		}
	}

	return out
}

func (p *GenerateProvider) extractGenerativeParameter(field *ast.ObjectField) interface{} {
	if len(p.additionalGenerativeParameters) > 0 {
		if generative, ok := p.additionalGenerativeParameters[field.Name.Value]; ok {
			if extractFn := generative.ExtractRequestParamsFunction; extractFn != nil {
				return extractFn(field)
			}
		}
	}
	return nil
}
