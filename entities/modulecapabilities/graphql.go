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

package modulecapabilities

import (
	"github.com/tailor-inc/graphql"
)

// GetArgumentsFn generates get graphql config for a given classname
type GetArgumentsFn = func(classname string) *graphql.ArgumentConfig

// AggregateArgumentsFn generates aggregate graphql config for a given classname
type AggregateArgumentsFn = func(classname string) *graphql.ArgumentConfig

// ExploreArgumentsFn generates explore graphql config
type ExploreArgumentsFn = func() *graphql.ArgumentConfig

// ExtractFn extracts graphql params to given struct implementation
type ExtractFn = func(param map[string]interface{}) interface{}

// NearParam defines params with certainty information
type NearParam interface {
	GetCertainty() float64
	GetDistance() float64
	SimilarityMetricProvided() bool
}

// ValidateFn validates a given module param
type ValidateFn = func(param interface{}) error

// GraphQLArgument defines all the needed settings / methods
// to add a module specific graphql argument
type GraphQLArgument struct {
	GetArgumentsFunction       GetArgumentsFn
	AggregateArgumentsFunction AggregateArgumentsFn
	ExploreArgumentsFunction   ExploreArgumentsFn
	ExtractFunction            ExtractFn
	ValidateFunction           ValidateFn
}

// GraphQLArguments defines the capabilities of modules to add their
// arguments to graphql API
type GraphQLArguments interface {
	Arguments() map[string]GraphQLArgument
}
