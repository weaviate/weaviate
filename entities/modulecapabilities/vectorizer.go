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
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
)

type Vectorizer interface {
	// VectorizeObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector and - if applicable - any meta
	// information as part of _additional properties
	VectorizeObject(ctx context.Context, obj *models.Object, objDiff *moduletools.ObjectDiff,
		cfg moduletools.ClassConfig) error
}

type FindObjectFn = func(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	adds additional.Properties) (*search.Result, error)

// ReferenceVectorizer is implemented by ref2vec modules, which calculate a target
// object's vector based only on the vectors of its references. If the object has
// no references, the object will have a nil vector
type ReferenceVectorizer interface {
	// VectorizeObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector, which is calculated by the module
	VectorizeObject(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig, findObjectFn FindObjectFn) error
}
