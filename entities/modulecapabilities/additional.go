//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
)

// GraphQLFieldFn generates graphql field based on classname
type GraphQLFieldFn = func(classname string) *graphql.Field

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
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error)

// AdditionalSearch defines on which type of query a given
// additional logic can be performed
type AdditionalSearch struct {
	ObjectGet   AdditionalPropertyFn
	ObjectList  AdditionalPropertyFn
	ExploreGet  AdditionalPropertyFn
	ExploreList AdditionalPropertyFn
}

// AdditionalProperty defines all the needed settings / methods
// to be set in order to add the additional property to Weaviate
type AdditionalProperty struct {
	RestNames              []string
	DefaultValue           interface{}
	GraphQLNames           []string
	GraphQLFieldFunction   GraphQLFieldFn
	GraphQLExtractFunction ExtractAdditionalFn
	SearchFunctions        AdditionalSearch
}

// AdditionalProperties groups whole interface methods needed
// for adding the capability of additional properties
type AdditionalProperties interface {
	AdditionalProperties() map[string]AdditionalProperty
}
