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

// assertPlaneIsSubsetOfPlaneZero checks the property the seeded range cascade
// rests on. A plane escaping plane 0 makes the cascade return a wrong
// allow-list: no panic, no log, no metric, just missing or extra objects. The
// check costs a whole-shard AndNot, so it is compiled into race builds only —
// which is every test binary CI runs, unit and acceptance alike.
func assertPlaneIsSubsetOfPlaneZero(bitmaps rangeBitmaps, plane int) {
	outside := bitmaps[plane].Clone()
	outside.AndNot(bitmaps[0])
	if !outside.IsEmpty() {
		panic(fmt.Sprintf("roaringsetrange: plane %d is not a subset of plane 0, "+
			"%d docs outside it; the range cascade cannot be seeded from it",
			plane, outside.GetCardinality()))
	}
}
