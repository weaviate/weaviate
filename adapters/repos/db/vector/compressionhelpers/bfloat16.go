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

// float32ToBFloat16 converts to bfloat16 (top 16 bits of the float32
// layout) with round-to-nearest-even and NaN canonicalized to 0x7FC0.
//
// History: the rq4c centering branch introduced this helper for its 12-byte
// packed header and we adopted it; the outlier-detection rework
// (95a78e4394) replaced that header with int8/position fields and deleted
// the helper, so the centered rq1 header is now its only user and the
// definition lives here. Semantics are unchanged from the rq4c original
// (commit 3d208f5300).
func float32ToBFloat16(x float32) uint16 {
	if x != x {
		return 0x7FC0
	}
	bits := math.Float32bits(x)
	return uint16((bits + 0x7FFF + ((bits >> 16) & 1)) >> 16)
}

// bfloat16ToFloat32 is the exact inverse embedding: bfloat16 bits are the
// top 16 bits of a float32.
func bfloat16ToFloat32(b uint16) float32 {
	return math.Float32frombits(uint32(b) << 16)
}
