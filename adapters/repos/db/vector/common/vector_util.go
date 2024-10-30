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
	if len(vecA) != len(vecB) {
		return false
	}
	if vecA == nil && vecB != nil {
		return false
	}
	if vecA != nil && vecB == nil {
		return false
	}
	for i := range vecA {
		if vecA[i] != vecB[i] {
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
