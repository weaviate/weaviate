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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
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

// TestFlushRoaringSetRefusesUnreadableIndex covers the two states where the
// flush would write a segment whose index no reader can resolve against its
// body. Neither is reachable today, so both rest on caller discipline alone.
func TestFlushRoaringSetRefusesUnreadableIndex(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(m *Memtable, f *segmentindex.SegmentFile)
	}{
		{
			// A non-zero count writes an offset table the header says is absent,
			// so the reader resolves that table as the primary tree.
			name:    "secondary indexes the header does not declare",
			prepare: func(m *Memtable, _ *segmentindex.SegmentFile) { m.secondaryIndices = 1 },
		},
		{
			// BodyWriter marks the file written, after which WriteHeader reports
			// zero bytes and the nodes would start before HeaderSize.
			name:    "header already written, so the nodes start too early",
			prepare: func(_ *Memtable, f *segmentindex.SegmentFile) { f.BodyWriter() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newRoaringSetFlushFixture(t, goldenFixtureShapes(), false)
			f := discardingSegmentFile()
			tt.prepare(m, f)

			keys, err := m.flushDataRoaringSet(f)
			require.Error(t, err)
			require.Nil(t, keys)
		})
	}
}
