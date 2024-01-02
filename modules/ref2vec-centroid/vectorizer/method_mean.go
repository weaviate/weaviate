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

package vectorizer

import "fmt"

func calculateMean(refVecs ...[]float32) ([]float32, error) {
	if len(refVecs) == 0 || len(refVecs[0]) == 0 {
		return nil, nil
	}

	targetVecLen := len(refVecs[0])
	meanVec := make([]float32, targetVecLen)

	// TODO: is there a more efficient way of doing this?
	for _, vec := range refVecs {
		if len(vec) != targetVecLen {
			return nil, fmt.Errorf("calculate mean: found vectors of different length: %d and %d",
				targetVecLen, len(vec))
		}

		for i, val := range vec {
			meanVec[i] += val
		}
	}

	for i := range meanVec {
		meanVec[i] /= float32(len(refVecs))
	}

	return meanVec, nil
}
