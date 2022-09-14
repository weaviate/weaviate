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

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type Vectorizer interface {
	// VectorizeObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector and - if applicable - any meta
	// information as part of _additional properties
	VectorizeObject(ctx context.Context, obj *models.Object,
		cfg moduletools.ClassConfig) error
}

// ReferenceVectorizer is implemented by ref2vec modules, which calculate a target
// object's vector based only on the vectors of its references. If the object has
// no references, the object will have a nil vector
type ReferenceVectorizer interface {
	// VectorizeObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector, which is calculated by the module
	// with the reference vectors refVectors
	VectorizeObject(ctx context.Context, obj *models.Object,
		cfg moduletools.ClassConfig, refVectors ...[]float32) error
	// TargetReferenceProperties takes in a map of a given class' module config, and
	// returns a list of properties which were configured by the class to be used for
	// calculating an object's vector based only on the vectors of its references
	TargetReferenceProperties(allProps map[string]interface{}) []string
}
