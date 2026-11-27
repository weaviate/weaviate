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

//go:build !arm64

package kernelproto

func fused(q *Query, x []uint64) int {
	return FusedGo(q, x)
}

func scanBlock(lut, nibbles []uint8, s *[BlockCodes]uint16) {
	scanBlockGo(lut, nibbles, s)
}
