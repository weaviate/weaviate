//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package tokens

import (
	"strconv"

	"github.com/graphql-go/graphql/language/ast"
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