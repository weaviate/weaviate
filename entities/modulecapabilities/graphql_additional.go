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

package modulecapabilities

import (
	"context"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/search"
)

type ExtractAdditionalFn = func(param []*ast.Argument) interface{}

type AdditionalPropertyWithSearchVector interface {
	SetSearchVector(vector []float32)
}

type AdditionalPropertyFn = func(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error)

type GraphQLAdditionalProperties interface {
	GetAdditionalFields(classname string) map[string]*graphql.Field
	ExtractAdditionalFunctions() map[string]ExtractAdditionalFn
	AdditionalPropetiesFunctions() map[string]AdditionalPropertyFn
}
