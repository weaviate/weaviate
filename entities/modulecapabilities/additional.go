//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modulecapabilities

import (
	"context"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

// GraphQLFieldFn generates graphql field based on classname
type GraphQLFieldFn = func(classname string) *graphql.Field

// ExtractAdditionalFn extracts parameters from graphql queries
type ExtractAdditionalFn = func(param []*ast.Argument, class *models.Class) any

// AdditionalPropertyWithSearchVector defines additional property params
// with the ability to pass search vector
type AdditionalPropertyWithSearchVector[T dto.Embedding] interface {
	SetSearchVector(vector T)
}

// AdditionalPropertyFn defines interface for additional property
// functions performing given logic
type AdditionalPropertyFn = func(ctx context.Context,
	in []search.Result, params any, limit *int,
	argumentModuleParams map[string]any, cfg moduletools.ClassConfig) ([]search.Result, error)

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
	DefaultValue           any
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

// DecisionLimits are the limits of a decisions module's provider for the
// annotate additional property. The shared validation applies them before the
// search runs, so a question the provider would reject costs no search and
// no provider call. A count of 0 means the Weaviate ceiling applies.
type DecisionLimits struct {
	// MaxDocuments bounds the documents a query judges: its limit. The
	// offset does not count, the module only sees the page.
	MaxDocuments int
	// MaxOptions bounds the options of a choice, MaxLevels the levels of a
	// score.
	MaxOptions int
	MaxLevels  int
	// DataTypes are the property types the module judges.
	DataTypes []schema.DataType
}

// DecisionLimitsProvider is implemented by a decisions module. The limits
// may depend on the class settings of the module; a class setting the module
// cannot work with is the error, so that the request fails before the
// search.
type DecisionLimitsProvider interface {
	DecisionLimits(cfg moduletools.ClassConfig) (DecisionLimits, error)
}
