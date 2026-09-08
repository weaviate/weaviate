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

package lsmkv

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestFlushRoaringSetKeysDoNotAliasNodeBuffers pins the backing array, not just
// the bytes: a key copied out of the node's serialization would release the
// segment body and still pass a bytes-only assertion, and copying every key per
// flush is the cost this avoids.
func TestFlushRoaringSetKeysDoNotAliasNodeBuffers(t *testing.T) {
	m := newRoaringSetFlushFixture(t, goldenFixtureShapes(), false)

	keys, err := m.flushDataRoaringSet(discardingSegmentFile())
	require.NoError(t, err)

	// Pairing by index assumes the flush walks in FlattenInOrder order, which is
	// what makes a walk-order change report as a mismatch rather than as a
	// pointer surprise.
	flat := m.roaringSet.FlattenInOrder()
	require.Len(t, keys, len(flat))

	for i, node := range flat {
		require.Equal(t, node.Key, keys[i].Key,
			"key %d does not hold its node's key bytes", i)
		require.Same(t, unsafe.SliceData(node.Key), unsafe.SliceData(keys[i].Key),
			"key %d points into the node's serialization, holding the segment body", i)
	}
}
