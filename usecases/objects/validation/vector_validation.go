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

package validation

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

func (v *Validator) vector(ctx context.Context, class *models.Class,
	incomingObject *models.Object,
) error {
	// In case the schema has a legacy vector index, we need to check if there is a default named vector which
	// is used to transport legacy vector via the named vector API.
	if defaultVec, ok := incomingObject.Vectors[modelsext.DefaultNamedVectorName]; ok && modelsext.ClassHasLegacyVectorIndex(class) {
		vec, ok := defaultVec.([]float32)
		if !ok {
			return fmt.Errorf("vector %s has to be a float32 array", modelsext.DefaultNamedVectorName)
		}
		incomingObject.Vector = vec
		delete(incomingObject.Vectors, modelsext.DefaultNamedVectorName)
	}

	if !modelsext.ClassHasLegacyVectorIndex(class) && len(incomingObject.Vector) > 0 {
		// if there is only one named vector we can assume that the single vector
		if len(class.VectorConfig) == 1 {
			namedVectorName := ""
			for key := range class.VectorConfig {
				namedVectorName = key
			}
			var vector []float32
			if len(incomingObject.Vector) > 0 {
				vector = make([]float32, len(incomingObject.Vector))
				copy(vector, incomingObject.Vector)
			}
			incomingObject.Vectors = map[string]models.Vector{namedVectorName: models.Vector(vector)}
			incomingObject.Vector = nil
			return nil
		}

		return fmt.Errorf("collection %v configuration does not have single vector index", class.Class)
	}

	var incomingTargetVectors []string
	for name := range incomingObject.Vectors {
		_, ok := class.VectorConfig[name]
		if !ok {
			return fmt.Errorf("collection %v does not have configuration for vector %s", class.Class, name)
		}

		incomingTargetVectors = append(incomingTargetVectors, name)
	}

	if len(class.VectorConfig) == 0 && len(incomingTargetVectors) > 0 {
		return fmt.Errorf("collection %v is configured without multiple named vectors, but received named vectors: %v", class.Class, incomingTargetVectors)
	}

	return nil
}
