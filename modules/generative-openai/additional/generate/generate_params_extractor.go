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
	"log"

	"github.com/tailor-inc/graphql/language/ast"
)

func (p *GenerateProvider) parseGenerateArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
		case "task":
			out.Task = arg.Value.(*ast.StringValue).Value
		case "resultLanguage":
			out.ResultLanguage = arg.Value.(*ast.EnumValue).Value
		case "onSet":
			out.OnSet = arg.Value.(*ast.EnumValue).Value
		case "properties":
			inp := arg.Value.GetValue().([]ast.Value)
			out.Properties = make([]string, len(inp))

			for i, value := range inp {
				out.Properties[i] = value.(*ast.StringValue).Value
			}

		default:
			// ignore what we don't recognize
			log.Printf("Igonore not recognized value: %v", arg.Name.Value)
		}
	}

	return out
}
