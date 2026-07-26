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

package roaringset

// CloneBufSize mirrors CloneToBuf's growth headroom for callers that size a
// clone from a bound wider than its source and so cannot call CloneToBuf: a
// raw Get with that size silently drops the headroom. It lives beside its
// callers rather than with the pool because it has no other use.
func CloneBufSize(lenInBytes int) int {
	return withGrowthHeadroom(lenInBytes, bitmapCloneGrowthFactor)
}
