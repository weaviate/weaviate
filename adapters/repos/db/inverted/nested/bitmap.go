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

package nested

import "github.com/weaviate/sroar"

const (
	maskRoots  = 0x0003_FFFF_FFFF_FFFF // zeroed 63-50 bits
	maskLeaves = 0xFFFC_000F_FFFF_FFFF // zeroed 49-36 bits

	MaxRoots         = 1 << 14
	MaxLeavesPerRoot = 1 << 14
)

func MaskLeafPositions(bm *sroar.Bitmap) *sroar.Bitmap {
	return bm.Masked(maskLeaves)
}

func MaskAllPositions(bm *sroar.Bitmap) *sroar.Bitmap {
	return bm.Masked(maskRoots & maskLeaves)
}
