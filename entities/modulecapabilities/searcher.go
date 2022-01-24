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

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/moduletools"
)

// FindVectorFn method for getting a vector of given object by it's ID
type FindVectorFn = func(ctx context.Context, id strfmt.UUID) ([]float32, error)

// VectorForParams defines method for passing a raw searcher content to the module
// and exchanging it for a vector. Warning: Argument "cfg"
// (moduletools.ClassConfig) is not guaranteed to be non-nil. Implementations
// have to provide a nil check before using it. It is generally present on
// class-based action, but is not present on Cross-Class requests, such as
// Explore {}
type VectorForParams = func(ctx context.Context, params interface{}, findVectorFn FindVectorFn,
	cfg moduletools.ClassConfig) ([]float32, error)

// Searcher defines all methods for all searchers
// for getting a vector from a given raw searcher content
type Searcher interface {
	VectorSearches() map[string]VectorForParams
}
