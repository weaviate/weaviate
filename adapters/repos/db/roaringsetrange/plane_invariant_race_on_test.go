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

import (
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestPlaneInvariantGuardFires: a plane holding a doc absent from plane 0 must panic.
func TestPlaneInvariantGuardFires(t *testing.T) {
	seg := newCascadeFixture(t, 3)
	seg.bitmaps[1].Set(1 << 30)

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	require.PanicsWithValue(t,
		"roaringsetrange: plane 1 is not a subset of plane 0, 1 docs outside it; "+
			"the range cascade cannot be seeded from it",
		func() { reader.mergeGreaterThanEqual(1, 1) })
}

// Guards against cascadeSeed moving back below the cache probe: a hit never
// runs the cascade, so the invariant it checks must still fire for cached
// predicates.
func TestPlaneInvariantGuardFiresOnACacheHit(t *testing.T) {
	seg := newCascadeFixture(t, 3)
	value := cascadeEncodeInt64(101)

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	// first sight records the key, second admits and stores it
	for i := 0; i < 2; i++ {
		_, releaseBm := reader.mergeGreaterThanEqual(value, 1)
		releaseBm()
	}
	require.NotZero(t, cachedEntries(seg), "nothing admitted, a hit is not reachable")

	seg.bitmaps[bits.TrailingZeros64(value)+1].Set(1 << 30)

	require.Panics(t, func() {
		_, releaseBm := reader.mergeGreaterThanEqual(value, 1)
		releaseBm()
	}, "the guard stopped firing once the predicate was cached")
}

// Same as TestPlaneInvariantGuardFiresOnACacheHit, for the two-seed
// mergeBetween entry point.
func TestPlaneInvariantGuardFiresOnABetweenCacheHit(t *testing.T) {
	seg := newCascadeFixture(t, 3)
	value := cascadeEncodeInt64(101)

	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	for i := 0; i < 2; i++ {
		_, releaseBm := reader.mergeBetween(value, value+1, 1)
		releaseBm()
	}
	require.NotZero(t, cachedEntries(seg), "nothing admitted, a hit is not reachable")

	seg.bitmaps[bits.TrailingZeros64(value)+1].Set(1 << 30)

	require.Panics(t, func() {
		_, releaseBm := reader.mergeBetween(value, value+1, 1)
		releaseBm()
	}, "the guard stopped firing once the predicate was cached")
}
