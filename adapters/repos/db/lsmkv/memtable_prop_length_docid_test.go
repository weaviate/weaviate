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

	"github.com/stretchr/testify/require"
)

// TestAppendMapSortedPropLengthDocIDMatchesSetTombstone pins that
// appendMapSorted and SetTombstone agree on how a MapPair.Key encodes a
// docID for StrategyInverted. Every other docID key in the package (bucket.go
// MapDeleteKey, commitlogger_parser_collection.go WAL replay,
// segment_serialization_inverted.go on-disk encoding) uses BigEndian, since
// StrategyInverted requires byte-wise-sortable keys.
//
// SetTombstone's propLengthExists.Remove(docId) exists to re-arm the
// propLengthExists.Set(docId) dedup gate in appendMapSorted, so that a doc
// re-indexed after being tombstoned within the same still-active memtable
// (delete immediately followed by re-add, e.g. an object update) has its new
// prop length counted again rather than silently dropped as a "duplicate."
// If appendMapSorted decodes the docID with a different endianness than
// SetTombstone, Remove operates on a different bitmap position than the one
// Set used, so the re-arm never happens: the update's new prop length is
// dropped from GetPropLengths' running sum/count instead of replacing the
// stale one.
func TestAppendMapSortedPropLengthDocIDMatchesSetTombstone(t *testing.T) {
	m := newTestMemtableInverted(nil)
	rowKey := []byte("key1")
	docID := uint64(42)

	require.NoError(t, m.appendMapSorted(rowKey, NewMapPairFromDocIdAndTf(docID, 3, 7, false)))
	sum, count := m.GetPropLengths()
	require.Equal(t, uint64(7), sum)
	require.Equal(t, uint64(1), count)

	require.NoError(t, m.SetTombstone(docID))

	// Re-add the same docID with a different prop length, simulating an
	// update applied within the same live memtable.
	require.NoError(t, m.appendMapSorted(rowKey, NewMapPairFromDocIdAndTf(docID, 3, 20, false)))

	sum, count = m.GetPropLengths()
	require.Equal(t, uint64(27), sum, "the re-add after tombstone must be counted, not silently dropped")
	require.Equal(t, uint64(2), count, "the re-add after tombstone must be counted, not silently dropped")
}
