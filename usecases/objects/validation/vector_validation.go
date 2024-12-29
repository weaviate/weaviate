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
)

func (v *Validator) vector(ctx context.Context, class *models.Class,
	incomingObject *models.Object,
) error {
	if len(class.VectorConfig) > 1 && len(incomingObject.Vector) > 0 {
		return fmt.Errorf("collection %v is configured with multiple named vectors %v, but received a single vector", class.Class, class.VectorConfig)
	}

	if class.VectorIndexConfig != nil && len(incomingObject.Vectors) > 0 {
		return fmt.Errorf("collection %v is configured without multiple named vectors, but received named vectors: %v", class.Class, incomingObject.Vectors)
	}

	// if there is only one named vector we can assume that the single vector
	if len(class.VectorConfig) == 1 && len(incomingObject.Vector) > 0 {
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
	}

	return nil
}
