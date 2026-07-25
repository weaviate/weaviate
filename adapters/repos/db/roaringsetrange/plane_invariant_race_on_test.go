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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestPlaneInvariantGuardFires proves the guard is not vacuous by handing the
// cascade the corruption it exists to catch: a plane holding a doc that plane 0
// does not.
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
