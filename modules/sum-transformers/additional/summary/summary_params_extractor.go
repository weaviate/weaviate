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

package summary

import (
	"log"

	"github.com/tailor-inc/graphql/language/ast"
)

func (p *SummaryProvider) parseSummaryArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
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
