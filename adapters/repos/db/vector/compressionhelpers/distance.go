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

package compressionhelpers

import "math/bits"

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var hammingBitwiseImpl func(a, b []uint64) float32 = func(a, b []uint64) float32 {
	total := float32(0)
	for segment := range a {
		total += float32(bits.OnesCount64(a[segment] ^ b[segment]))
	}
	return total
}
