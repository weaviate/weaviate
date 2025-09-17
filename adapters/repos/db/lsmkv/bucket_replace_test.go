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
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucketReplaceWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy:       StrategyReplace,
		disk:           &SegmentGroup{segments: []Segment{}},
		active:         newTestMemtableReplace(nil),
		keepTombstones: true,
	}

	expectedRefs := 0

	err := b.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	expectedRefs++
	err = b.Put([]byte("key2"), []byte("value2"))
	require.NoError(t, err)
	expectedRefs++

	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}
	assertWriterRefs()

	// regular delete
	err = b.Delete([]byte("key1"))
	require.NoError(t, err)
	expectedRefs++

	// delete with timetsamp
	err = b.DeleteWith([]byte("key2"), time.Now())
	require.NoError(t, err)
	expectedRefs++

	assertWriterRefs()
}
