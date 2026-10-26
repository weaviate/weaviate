//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !arm64 && !amd64

package compression

// FastWalshHadamardTransform64 performs a normalized fast Walsh-Hadamard
// transform on the first 64 elements of x.
func FastWalshHadamardTransform64(x []float32) {
	fastWalshHadamardTransform64Go(x)
}

// FastWalshHadamardTransform256 performs a normalized fast Walsh-Hadamard
// transform on the first 256 elements of x.
func FastWalshHadamardTransform256(x []float32) {
	fastWalshHadamardTransform256Go(x)
}
