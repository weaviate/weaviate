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

// AdditionalSearch defines on which type of query a given
// additional logic can be performed
type AdditionalSearch struct {
	ObjectGet   AdditionalPropertyFn
	ObjectList  AdditionalPropertyFn
	ExploreGet  AdditionalPropertyFn
	ExploreList AdditionalPropertyFn
}

// DefaultValueFn this a default value used for rest queries
type DefaultValueFn = func() interface{}

// ExtractAdditionalFn extracts parameters from graphql queries
type ExtractAdditionalFn = func(param []*ast.Argument) interface{}

// AdditionalPropertyWithSearchVector defines additional property params
// with the ability to pass search vector
type AdditionalPropertyWithSearchVector interface {
	SetSearchVector(vector []float32)
}

// AdditionalPropertyFn defines interface for additional property
// functions performing given logic
type AdditionalPropertyFn = func(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error)

// GraphQLAdditionalProperties groups whole interface methods needed
// for adding the capability of additional properties
type GraphQLAdditionalProperties interface {
	GetAdditionalFields(classname string) map[string]*graphql.Field
	ExtractAdditionalFunctions() map[string]ExtractAdditionalFn
	AdditionalPropertiesDefaultValues() map[string]DefaultValueFn
	RestApiAdditionalProperties() map[string][]string
	GraphQLAdditionalProperties() map[string][]string
	SearchAdditionalFunctions() map[string]AdditionalSearch
}
