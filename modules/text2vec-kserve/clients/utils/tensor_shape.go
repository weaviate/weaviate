//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package utils

func TensorShapeEqual[T int32 | int64](s1 []T, s2 []T) bool {
	if len(s1) != 2 {
		return false
	}
	batchDim, outputDim := s1[0], s1[1]
	expectedBatchDim, expectedOutputDim := s2[0], s2[1]
	if batchDim != expectedBatchDim {
		return false
	}
	return outputDim == expectedOutputDim
}
