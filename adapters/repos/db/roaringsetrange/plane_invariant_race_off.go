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
// bitmaps is a pointer so that the call stays free even if this body grows;
// by value it copies the whole plane array on every seeded cascade.
func assertPlaneIsSubsetOfPlaneZero(bitmaps *rangeBitmaps, plane int) {}
