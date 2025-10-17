//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategySetCollection,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableSet(t, nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}
	assertWriterRefs()

	// add
	err := b.SetAdd([]byte("key1"), [][]byte{[]byte("v1"), []byte("v2")})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// delete one
	err = b.SetDeleteSingle([]byte("key1"), []byte("v1"))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, err := b.SetList([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("v2")}, v)
}
