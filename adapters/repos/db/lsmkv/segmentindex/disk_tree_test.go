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

package segmentindex

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// A corrupt or truncated on-disk index must never crash the node: every read
// path (Get, Seek/Next, AllKeys) has to return NotFound or an error instead of
// panicking on an out-of-range slice.
func TestDiskTreeCorruptDataNeverPanics(t *testing.T) {
	tree := NewTree(4)
	elements := []struct {
		key        []byte
		start, end uint64
	}{
		{[]byte("foobar"), 17, 18}, // inserted first -> BST root at offset 0
		{[]byte("abc"), 4, 5},
		{[]byte("zzz"), 34, 35},
		{[]byte("aaa"), 1, 2},
		{[]byte("zzzz"), 100, 102},
	}
	for _, e := range elements {
		tree.Insert(e.key, e.start, e.end)
	}
	valid, err := tree.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(valid), TREE_KEY_STORE_OVERHEAD)

	// queries span match, descend-left and descend-right branches plus misses.
	queries := [][]byte{
		[]byte("aaa"), []byte("abc"), []byte("foobar"), []byte("zzz"), []byte("zzzz"),
		[]byte("a"), []byte("m"), []byte("zzzzz"), []byte(""),
	}

	t.Run("every truncation of the buffer", func(t *testing.T) {
		for trunc := 0; trunc <= len(valid); trunc++ {
			dTree := NewDiskTree(valid[:trunc])
			require.NotPanics(t, func() {
				_, _ = dTree.AllKeys()
			}, "AllKeys panicked at truncation=%d", trunc)
			for _, q := range queries {
				require.NotPanics(t, func() {
					_, _ = dTree.Get(q)
				}, "Get panicked at truncation=%d query=%q", trunc, q)
				require.NotPanics(t, func() {
					_, _ = dTree.Seek(q)
				}, "Seek panicked at truncation=%d query=%q", trunc, q)
			}
		}
	})

	t.Run("corrupt keyLen larger than the buffer returns an error", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		binary.LittleEndian.PutUint32(corrupt[0:4], 0xFFFFFFFF)
		dTree := NewDiskTree(corrupt)

		var getErr, allErr error
		require.NotPanics(t, func() {
			_, getErr = dTree.Get([]byte("foobar"))
			_, allErr = dTree.AllKeys()
			_, _ = dTree.Seek([]byte("foobar"))
		})
		require.Error(t, getErr)
		require.Error(t, allErr)
	})

	t.Run("corrupt child pointers do not panic", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		// root node at offset 0: [keyLen:4][key][start:8][end:8][left:8][right:8].
		keyLen := int(binary.LittleEndian.Uint32(corrupt[0:4]))
		childBase := 4 + keyLen + 16                                             // past keyLen + key + start + end
		binary.LittleEndian.PutUint64(corrupt[childBase:], 0xFFFFFFFFFFFFFFFF)   // left child
		binary.LittleEndian.PutUint64(corrupt[childBase+8:], 0xFFFFFFFFFFFFFFF0) // right child
		dTree := NewDiskTree(corrupt)

		require.NotPanics(t, func() {
			_, _ = dTree.Get([]byte("aaa"))   // descends left into the bad pointer
			_, _ = dTree.Get([]byte("zzzzz")) // descends right into the bad pointer
			_, _ = dTree.Seek([]byte("aaa"))
		})
	})
}

// TestDiskTreeValidateRootInBounds pins weaviate/weaviate#12280 shape 4: a
// corrupt but in-bounds IndexStart lands the tree on the wrong bytes, and
// Get()'s own corruption-tolerant walk resolves that to NotFound instead of
// an error - indistinguishable from a legitimate empty result. The root
// node's Start/End must fall within the segment's own data region for any
// legitimately-written segment, so checking just the root at open time
// catches this without walking (or trusting) the rest of the tree.
func TestDiskTreeValidateRootInBounds(t *testing.T) {
	buildTree := func(t *testing.T, start, end uint64) []byte {
		t.Helper()
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), start, end)
		data, err := tree.MarshalBinary()
		require.NoError(t, err)
		return data
	}

	t.Run("root Start/End within the data region: accepted", func(t *testing.T) {
		data := buildTree(t, 20, 30)
		dTree := NewDiskTree(data)
		require.NoError(t, dTree.ValidateRootInBounds(16, 100))
	})

	t.Run("root Start before the data region: rejected", func(t *testing.T) {
		data := buildTree(t, 5, 30)
		dTree := NewDiskTree(data)
		err := dTree.ValidateRootInBounds(16, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside the segment's data region")
	})

	t.Run("root End past the data region: rejected", func(t *testing.T) {
		data := buildTree(t, 20, 500)
		dTree := NewDiskTree(data)
		err := dTree.ValidateRootInBounds(16, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside the segment's data region")
	})

	t.Run("empty tree (legitimate empty segment): accepted", func(t *testing.T) {
		dTree := NewDiskTree(nil)
		require.NoError(t, dTree.ValidateRootInBounds(16, 100))
	})

	t.Run("garbage bytes at the root: rejected, not panicked", func(t *testing.T) {
		// Simulates a corrupt IndexStart landing on data-region bytes rather
		// than real tree-node bytes: a plausible-looking keyLen followed by
		// arbitrary content, which still parses as *some* node (readNodeAt
		// is itself corruption-tolerant), just one whose Start/End are
		// garbage and (almost always) outside the real data region.
		garbage := make([]byte, 64)
		binary.LittleEndian.PutUint32(garbage[0:4], 4) // keyLen=4
		copy(garbage[4:8], []byte("junk"))
		binary.LittleEndian.PutUint64(garbage[8:16], 0xDEADBEEFDEADBEEF)  // start
		binary.LittleEndian.PutUint64(garbage[16:24], 0xFEEDFACEFEEDFACE) // end
		dTree := NewDiskTree(garbage)
		require.NotPanics(t, func() {
			err := dTree.ValidateRootInBounds(16, 100)
			require.Error(t, err)
		})
	})
}
