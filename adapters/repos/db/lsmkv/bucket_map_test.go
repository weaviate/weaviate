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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyMapCollection,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableMap(nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}
	assertWriterRefs()

	// add one
	err := b.MapSet([]byte("key1"), MapPair{Key: []byte("k1"), Value: []byte("v1")})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add many
	err = b.MapSetMulti([]byte("key1"), []MapPair{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// delete one
	err = b.MapDeleteKey([]byte("key1"), []byte("k2"))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, err := b.MapList(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}, v)
}
