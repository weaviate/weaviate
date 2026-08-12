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

package compressionhelpers

import "math"

// float32ToBF16 converts to bfloat16 (top 16 bits of the float32 layout)
// with round-to-nearest-even and NaN canonicalized to 0x7FC0.
//
// TODO(rq1-centered): this duplicates float32ToBFloat16 from the rq4c
// centering branch (trengrj/4bit-centering, commit 3d208f5300) with
// identical semantics, kept separate only while the two branches are
// unmerged. When they meet, one helper must win, and it will be the rq4c
// one — delete this file and switch callers.
func float32ToBF16(x float32) uint16 {
	if x != x {
		return 0x7FC0
	}
	bits := math.Float32bits(x)
	return uint16((bits + 0x7FFF + ((bits >> 16) & 1)) >> 16)
}

// bf16ToFloat32 is the exact inverse embedding: bfloat16 bits are the top
// 16 bits of a float32.
func bf16ToFloat32(b uint16) float32 {
	return math.Float32frombits(uint32(b) << 16)
}
