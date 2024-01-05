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

package rank

import (
	"github.com/tailor-inc/graphql/language/ast"
)

func (p *ReRankerProvider) parseReRankerArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
		case "query":
			out.Query = &arg.Value.(*ast.StringValue).Value
		case "property":
			out.Property = &arg.Value.(*ast.StringValue).Value
		}
	}

	return out
}
