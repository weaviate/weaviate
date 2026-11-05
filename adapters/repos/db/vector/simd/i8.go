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

package simd

// DotProductInt8 returns sum(int32(a[i]) * int32(b[i])) over min(len(a),
// len(b)) elements, accumulated in int32 with two's-complement wraparound; 0
// when either slice is empty.
func DotProductInt8(a, b []int8) int32 {
	n := min(len(a), len(b))
	if n == 0 {
		return 0
	}
	return dotInt8Impl(a[:n], b[:n])
}
