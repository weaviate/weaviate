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

package projector

import (
	"strconv"

	"github.com/tailor-inc/graphql/language/ast"
)

func parseFeatureProjectionArguments(args []*ast.Argument) *Params {
	out := &Params{Enabled: true}

	for _, arg := range args {
		switch arg.Name.Value {
		case "dimensions":
			asInt, _ := strconv.Atoi(arg.Value.GetValue().(string))
			out.Dimensions = ptInt(asInt)
		case "iterations":
			asInt, _ := strconv.Atoi(arg.Value.GetValue().(string))
			out.Iterations = ptInt(asInt)
		case "learningRate":
			asInt, _ := strconv.Atoi(arg.Value.GetValue().(string))
			out.LearningRate = ptInt(asInt)
		case "perplexity":
			asInt, _ := strconv.Atoi(arg.Value.GetValue().(string))
			out.Perplexity = ptInt(asInt)
		case "algorithm":
			out.Algorithm = ptString(arg.Value.GetValue().(string))

		default:
			// ignore what we don't recognize
		}
	}

	return out
}

func ptString(in string) *string {
	return &in
}

func ptInt(in int) *int {
	return &in
}
