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
