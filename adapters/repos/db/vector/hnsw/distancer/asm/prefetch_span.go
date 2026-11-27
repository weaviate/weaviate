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

package asm

// prefetchStride is the distance between two hints of PrefetchN.
const prefetchStride = 64

// PrefetchSpan returns the size to hand PrefetchN so that its hints reach the
// last of the n bytes that start at addr.
//
// PrefetchN hints one cache line per 64 bytes of the size it is given,
// counting from addr. A range that does not start on a line boundary can end
// one line further than its size alone says: 104 bytes that start 32 bytes
// into a line end in the third one, and PrefetchN(addr, 104) hints two. The
// line left out is a cache miss its reader then waits for.
func PrefetchSpan(addr uintptr, n int) int {
	return int(addr%prefetchStride) + n
}
