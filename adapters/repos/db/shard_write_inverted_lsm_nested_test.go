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

package db

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// TestNestedMetaEntries_AnchorsRoundtrip exercises the build-time wiring of
// NestedProperty.Anchors through nestedMetaEntries: each AnchorEntry must
// produce one RoaringSetBatchEntry whose key matches AnchorKey(path) and
// whose values are positions OR'd with the docID. _idx, _exists, and
// _anchor entries coexist in the returned batch — verified by counting and
// by ensuring each family's entries are addressable by family key.
func TestNestedMetaEntries_AnchorsRoundtrip(t *testing.T) {
	const docID = uint64(42)
	// A mixed input: 1 idx, 2 exists, 3 anchors across two paths.
	np := inverted.NestedProperty{
		Name: "addresses",
		Idx: []inverted.NestedMeta{
			{Path: "addresses", Index: 0, Positions: []uint64{nested.Encode(1, 4, 0)}},
		},
		Exists: []inverted.NestedMeta{
			{Path: "", Index: -1, Positions: []uint64{nested.Encode(1, 1, 0), nested.Encode(1, 4, 0)}},
			{Path: "addresses", Index: -1, Positions: []uint64{nested.Encode(1, 4, 0)}},
		},
		Anchors: []inverted.NestedMeta{
			{Path: "", Index: -1, Positions: []uint64{nested.Encode(1, 1, 0)}},
			{Path: "addresses", Index: -1, Positions: []uint64{nested.Encode(1, 4, 0)}},
			{Path: "addresses.numbers", Index: -1, Positions: []uint64{nested.Encode(1, 5, 0)}},
		},
	}

	entries := nestedMetaEntries(np, docID)
	require.Len(t, entries, 6, "1 idx + 2 exists + 3 anchors")

	// Per-anchor: find the batch entry by key prefix, verify Values were
	// OrDocID'd with docID (= the position bits have docID=42, not 0).
	anchorCases := []struct {
		name string
		path string
		leaf uint16
	}{
		{"root anchor", "", 1},
		{"addresses element anchor", "addresses", 4},
		{"scalar-array element anchor", "addresses.numbers", 5},
	}
	for _, tc := range anchorCases {
		t.Run(tc.name, func(t *testing.T) {
			wantKey := nested.AnchorKey(tc.path)
			wantValues := []uint64{nested.Encode(1, tc.leaf, docID)}

			found := findEntryByKey(entries, wantKey)
			require.NotNil(t, found, "no batch entry for AnchorKey(%q)", tc.path)
			assert.Equal(t, wantValues, found.Values, "AnchorKey(%q) values", tc.path)
		})
	}

	// Cross-family sanity: the anchor keys must NOT collide with the
	// exists keys for the same path. (Already covered by
	// TestKeyFamiliesAreDistinct at the key level — this verifies the
	// distinct keys actually survive into the batch.)
	for _, path := range []string{"", "addresses"} {
		anchorKey := nested.AnchorKey(path)
		existsKey := nested.ExistsKey(path)
		assert.NotEqual(t, anchorKey, existsKey, "AnchorKey(%q) must differ from ExistsKey", path)

		anchorEntry := findEntryByKey(entries, anchorKey)
		existsEntry := findEntryByKey(entries, existsKey)
		require.NotNil(t, anchorEntry)
		require.NotNil(t, existsEntry)
		assert.NotSame(t, &anchorEntry.Values, &existsEntry.Values,
			"anchor and exists entries at path %q share a Values backing array", path)
	}
}

// TestNestedMetaEntries_AnchorsOnly proves the short-circuit guards do not
// drop a write that carries only Anchors. extendNestedMetaIndex and
// deleteNestedMetaIndex both short-circuit on
// len(Idx) == 0 && len(Exists) == 0 && len(Anchors) == 0; the inverse must
// produce a non-empty batch so the writes reach the bucket.
func TestNestedMetaEntries_AnchorsOnly(t *testing.T) {
	const docID = uint64(7)
	np := inverted.NestedProperty{
		Name: "addresses",
		Anchors: []inverted.NestedMeta{
			{Path: "addresses", Index: -1, Positions: []uint64{nested.Encode(1, 4, 0)}},
		},
	}

	entries := nestedMetaEntries(np, docID)
	require.Len(t, entries, 1)
	assert.Equal(t, nested.AnchorKey("addresses"), []byte(entries[0].Key))
	assert.Equal(t, []uint64{nested.Encode(1, 4, docID)}, entries[0].Values)
}

// TestNestedMetaEntries_EmptyAnchors verifies the zero-anchor case stays a
// pass-through: no batch entries beyond the idx/exists ones.
func TestNestedMetaEntries_EmptyAnchors(t *testing.T) {
	np := inverted.NestedProperty{
		Name: "addresses",
		Exists: []inverted.NestedMeta{
			{Path: "addresses", Index: -1, Positions: []uint64{nested.Encode(1, 4, 0)}},
		},
		// Anchors deliberately nil.
	}

	entries := nestedMetaEntries(np, 1)
	require.Len(t, entries, 1, "only the single exists entry should be present")
	assert.Equal(t, nested.ExistsKey("addresses"), []byte(entries[0].Key))
}

func findEntryByKey(entries []lsmkv.RoaringSetBatchEntry, key []byte) *lsmkv.RoaringSetBatchEntry {
	for i := range entries {
		if bytes.Equal(entries[i].Key, key) {
			return &entries[i]
		}
	}
	return nil
}
