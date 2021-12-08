//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

// CombineVectors combines all of the vector into sum of their parts
func CombineVectors(vectors [][]float32) []float32 {
	return CombineVectorsWithWeights(vectors, nil)
}

func CombineVectorsWithWeights(vectors [][]float32, weights []float32) []float32 {
	maxVectorLength := 0
	for i := range vectors {
		if len(vectors[i]) > maxVectorLength {
			maxVectorLength = len(vectors[i])
		}
	}
	sums := make([]float32, maxVectorLength)
	dividers := make([]float32, maxVectorLength)
	for indx, vector := range vectors {
		for i := 0; i < len(vector); i++ {
			if weights != nil {
				// apply weight to vector value
				sums[i] += vector[i] * weights[indx]
			} else {
				sums[i] += vector[i]
			}
			dividers[i]++
		}
	}
	combinedVector := make([]float32, len(sums))
	for i := 0; i < len(sums); i++ {
		combinedVector[i] = sums[i] / dividers[i]
	}

	return combinedVector
}
