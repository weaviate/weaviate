//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test_suits

import (
	"math/rand"
	"time"
)

func generateRandomVector(dimensionality int) []float32 {
	if dimensionality <= 0 {
		return nil
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	slice := make([]float32, dimensionality)
	for i := range slice {
		slice[i] = r.Float32()
	}
	return slice
}
