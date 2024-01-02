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

package autocut

func Autocut(yValues []float32, cutOff int) int {
	if len(yValues) <= 1 {
		return len(yValues)
	}

	diff := make([]float32, len(yValues))
	step := 1. / (float32(len(yValues)) - 1.)

	for i := range yValues {
		xValue := 0. + float32(i)*step
		yValueNorm := (yValues[i] - yValues[0]) / (yValues[len(yValues)-1] - yValues[0])
		diff[i] = yValueNorm - xValue
	}

	extremaCount := 0
	for i := range diff {
		if i == 0 {
			continue // we want the index _before_ the extrema
		}

		if i == len(diff)-1 && len(diff) > 1 { // for last element there is no "next" point
			if diff[i] > diff[i-1] && diff[i] > diff[i-2] {
				extremaCount += 1
				if extremaCount >= cutOff {
					return i
				}
			}
		} else {
			if diff[i] > diff[i-1] && diff[i] > diff[i+1] {
				extremaCount += 1
				if extremaCount >= cutOff {
					return i
				}
			}
		}
	}
	return len(yValues)
}
