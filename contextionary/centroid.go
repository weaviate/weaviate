/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package contextionary

import (
	"fmt"
)

func ComputeCentroid(vectors []Vector) (*Vector, error) {
	var weights []float32 = make([]float32, len(vectors))

	for i := 0; i < len(vectors); i++ {
		weights[i] = 1.0
	}

	return ComputeWeightedCentroid(vectors, weights)
}

func ComputeWeightedCentroid(vectors []Vector, weights []float32) (*Vector, error) {

	if len(vectors) == 0 {
		return nil, fmt.Errorf("Can not compute centroid of empty slice")
	} else if len(vectors) != len(weights) {
		return nil, fmt.Errorf("Can not compute weighted centroid if len(vectors) != len(weights)")
	} else if len(vectors) == 1 {
		return &vectors[0], nil
	} else {
		vector_len := vectors[0].Len()

		var new_vector []float32 = make([]float32, vector_len)
		var weight_sum float32 = 0.0

		for vector_i, v := range vectors {
			if v.Len() != vector_len {
				return nil, fmt.Errorf("Vectors have different lengths")
			}

			weight_sum += weights[vector_i]

			for i := 0; i < vector_len; i++ {
				new_vector[i] += v.vector[i] * weights[vector_i]
			}
		}

		for i := 0; i < vector_len; i++ {
			new_vector[i] /= weight_sum
		}

		result := NewVector(new_vector)
		return &result, nil
	}
}
