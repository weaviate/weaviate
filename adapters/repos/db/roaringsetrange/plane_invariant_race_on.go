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

//go:build race

package roaringsetrange

import "fmt"

// assertPlaneIsSubsetOfPlaneZero guards the invariant the seeded cascade
// depends on: if a plane escapes plane 0, results are silently wrong with no
// panic or log. The check is a whole-shard AndNot, so it is compiled in only
// under the race build tag, where that overhead is already accepted.
func assertPlaneIsSubsetOfPlaneZero(bitmaps rangeBitmaps, plane int) {
	outside := bitmaps[plane].Clone()
	outside.AndNot(bitmaps[0])
	if !outside.IsEmpty() {
		panic(fmt.Sprintf("roaringsetrange: plane %d is not a subset of plane 0, "+
			"%d docs outside it; the range cascade cannot be seeded from it",
			plane, outside.GetCardinality()))
	}
}
