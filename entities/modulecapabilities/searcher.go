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
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/moduletools"
)

// FindVectorFn method for getting a vector of given object by its ID
// type FindVectorFn = func(ctx context.Context, className string, id strfmt.UUID, tenant, targetVector string) ([]float32, string, error)
type FindVectorFn[T dto.Embedding] interface {
	FindVector(ctx context.Context,
		className string, id strfmt.UUID, tenant, targetVector string) (T, string, error)
}

// VectorForParams defines method for passing a raw searcher content to the module
// and exchanging it for a vector. Warning: Argument "cfg"
// (moduletools.ClassConfig) is not guaranteed to be non-nil. Implementations
// have to provide a nil check before using it. It is generally present on
// class-based action, but is not present on Cross-Class requests, such as
// Explore {}
type VectorForParams[T dto.Embedding] interface {
	VectorForParams(ctx context.Context, params interface{},
		className string, findVectorFn FindVectorFn[T], cfg moduletools.ClassConfig) (T, error)
}

// Searcher defines all methods for all searchers
// for getting a vector from a given raw searcher content
type Searcher[T dto.Embedding] interface {
	VectorSearches() map[string]VectorForParams[T]
}

// DependencySearcher defines all of the available searches loaded as a dependency
// for this time it's limited to modules providing []float32 embeddings
type DependencySearcher[T dto.Embedding] interface {
	VectorSearches() map[string]map[string]VectorForParams[T]
}
