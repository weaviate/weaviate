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

package common

func VectorsEqual(vecA, vecB []float32) bool {
	return vectorsEqual(vecA, vecB, func(valueA, valueB float32) bool {
		return valueA == valueB
	})
}

func MultiVectorsEqual(vecA, vecB [][]float32) bool {
	return vectorsEqual(vecA, vecB, VectorsEqual)
}

// vectorsEqual verifies whether provided vectors are the same
// It considers nil vector as equal to vector of len = 0.
func vectorsEqual[T []C, C float32 | []float32](vecA, vecB T, valuesEqual func(valueA, valueB C) bool) bool {
	if lena, lenb := len(vecA), len(vecB); lena != lenb {
		return false
	} else if lena == 0 {
		return true
	}

	for i := range vecA {
		if !valuesEqual(vecA[i], vecB[i]) {
			return false
		}
	}
	return true
}

func CalculateOptimalSegments(dims int) int {
	if dims >= 2048 && dims%8 == 0 {
		return dims / 8
	} else if dims >= 768 && dims%6 == 0 {
		return dims / 6
	} else if dims >= 256 && dims%4 == 0 {
		return dims / 4
	} else if dims%2 == 0 {
		return dims / 2
	}
	return dims
}
