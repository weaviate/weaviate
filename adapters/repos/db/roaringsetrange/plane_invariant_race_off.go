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

//go:build !race

package roaringsetrange

// No-op: too costly for a release query path. See plane_invariant_race_on.go.
// The pointer is for the race build, which shares this signature and does run a
// body, where by value it would copy the whole plane array on every seeded
// cascade. Here the body is empty, so both forms compile to the same code.
func assertPlaneIsSubsetOfPlaneZero(bitmaps *rangeBitmaps, plane int) {}
