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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
)

type Vectorizer interface {
	// VectorizeObject takes an object and returns a vector and - if applicable - any meta
	// information as part of _additional properties
	VectorizeObject(ctx context.Context, obj *models.Object,
		cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	// VectorizableProperties returns which properties the vectorizer looks at.
	// If the vectorizer is capable of vectorizing all text properties, the first bool is true.
	// Any additional "media"-properties are explicitly mentioned in the []string return
	VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error)
	VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error)
}

type FindObjectFn = func(ctx context.Context, class string, id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties, tenant string) (*search.Result, error)

// ReferenceVectorizer is implemented by ref2vec modules, which calculate a target
// object's vector based only on the vectors of its references. If the object has
// no references, the object will have a nil vector
type ReferenceVectorizer interface {
	// VectorizeObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector, which is calculated by the module
	VectorizeObject(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig, findObjectFn FindObjectFn) ([]float32, error)
}

type InputVectorizer interface {
	VectorizeInput(ctx context.Context, input string,
		cfg moduletools.ClassConfig) ([]float32, error)
}
