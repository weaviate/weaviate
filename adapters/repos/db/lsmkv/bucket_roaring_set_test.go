//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRoaringSetWritePathRefCount ensures that all write paths of the
// RoaringSet type correctly use and release refcounts on the active memtable
// and therefore do not block a flushlock for the entire duration of the wrige.
func TestRoaringSetWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableRoaringSet(nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}

	// add one
	err := b.RoaringSetAddOne([]byte("key1"), 1)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add list
	err = b.RoaringSetAddList([]byte("key1"), []uint64{2, 3, 4})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add bitmap
	err = b.RoaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{5, 6, 7}))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// remove one
	err = b.RoaringSetRemoveOne([]byte("key1"), 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, releaseBufPol, err := b.RoaringSetGet([]byte("key1"))
	defer releaseBufPol()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, v.ToArray())
}
