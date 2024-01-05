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

package tokens

import (
	"strconv"

	"github.com/tailor-inc/graphql/language/ast"
)

func (p *TokenProvider) parseTokenArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
		case "limit":
			asInt, _ := strconv.Atoi(arg.Value.GetValue().(string))
			out.Limit = ptInt(asInt)
		case "certainty":
			asFloat, _ := strconv.ParseFloat(arg.Value.GetValue().(string), 64)
			out.Certainty = &asFloat
		case "distance":
			asFloat, _ := strconv.ParseFloat(arg.Value.GetValue().(string), 64)
			out.Distance = &asFloat
		case "properties":
			inp := arg.Value.GetValue().([]ast.Value)
			out.Properties = make([]string, len(inp))

			for i, value := range inp {
				out.Properties[i] = value.(*ast.StringValue).Value
			}

		default:
			// ignore what we don't recognize
		}
	}

	return out
}

func ptInt(in int) *int {
	return &in
}
