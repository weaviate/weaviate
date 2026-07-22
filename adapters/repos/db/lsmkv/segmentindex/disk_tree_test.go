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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
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

// TestDiskTreeValidateRootInBounds pins weaviate/weaviate#12280: a corrupt
// but in-bounds IndexStart lands the tree on the wrong bytes, which Get()
// would otherwise resolve to NotFound instead of erroring.
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
		// A plausible keyLen followed by arbitrary bytes still parses as
		// some node (readNodeAt is corruption-tolerant), just one whose
		// Start/End are garbage.
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

// TestDiskTreeGet_CycleGuard pins weaviate/weaviate#12280: a cyclic but
// individually in-bounds child offset must not spin Get's descent forever.
// Bounded by a timeout so a regression fails the test instead of hanging it.
func TestDiskTreeGet_CycleGuard(t *testing.T) {
	tree := NewTree(4)
	tree.Insert([]byte("b"), 20, 21) // root
	tree.Insert([]byte("a"), 10, 11) // left child
	tree.Insert([]byte("d"), 30, 31) // right child
	valid, err := tree.MarshalBinary()
	require.NoError(t, err)

	// Root's left child pointer, redirected to offset 0 (the root itself):
	// an in-bounds, individually-valid offset that forms a self-cycle.
	keyLen := int(binary.LittleEndian.Uint32(valid[0:4]))
	childBase := 4 + keyLen + 16
	corrupt := make([]byte, len(valid))
	copy(corrupt, valid)
	binary.LittleEndian.PutUint64(corrupt[childBase:], 0)

	dTree := NewDiskTree(corrupt)

	done := make(chan struct{})
	var getErr error
	go func() {
		defer close(done)
		_, getErr = dTree.Get([]byte("0")) // "0" < "a" < "b": descends left into the cycle
	}()

	select {
	case <-done:
		require.Error(t, getErr, "a cyclic descent must terminate with an error, not resolve as if the key were simply missing")
		require.Contains(t, getErr.Error(), "exceeded")
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not terminate within 5s on a cyclic child pointer; the iteration cap is not working")
	}
}

// TestDiskTreeSeekNext_CycleGuard pins weaviate/weaviate#12280: a cyclic
// child pointer must not stack-overflow seekAt (Seek/Next).
func TestDiskTreeSeekNext_CycleGuard(t *testing.T) {
	tree := NewTree(4)
	tree.Insert([]byte("b"), 20, 21) // root
	tree.Insert([]byte("a"), 10, 11) // left child
	tree.Insert([]byte("d"), 30, 31) // right child
	valid, err := tree.MarshalBinary()
	require.NoError(t, err)

	// Root's left child pointer, redirected to offset 0 (the root itself):
	// an in-bounds, individually-valid offset that forms a self-cycle.
	keyLen := int(binary.LittleEndian.Uint32(valid[0:4]))
	childBase := 4 + keyLen + 16
	corrupt := make([]byte, len(valid))
	copy(corrupt, valid)
	binary.LittleEndian.PutUint64(corrupt[childBase:], 0)

	dTree := NewDiskTree(corrupt)

	t.Run("Seek", func(t *testing.T) {
		var seekErr error
		require.NotPanics(t, func() {
			_, seekErr = dTree.Seek([]byte("0")) // "0" < "a" < "b": descends left into the cycle
		})
		require.Error(t, seekErr, "a cyclic descent must terminate with an error, not resolve as if the key were simply missing")
		require.Contains(t, seekErr.Error(), "exceeded")
	})

	t.Run("Next", func(t *testing.T) {
		var nextErr error
		require.NotPanics(t, func() {
			_, nextErr = dTree.Next([]byte("0"))
		})
		require.Error(t, nextErr, "a cyclic descent must terminate with an error, not resolve as if the key were simply missing")
		require.Contains(t, nextErr.Error(), "exceeded")
	})
}

// TestDiskTreeReadTimeNodeSanity pins weaviate/weaviate#12280: a node with
// End < Start must return a clean error, not panic downstream via
// make([]byte, End-Start) wrapping to a huge length.
func TestDiskTreeReadTimeNodeSanity(t *testing.T) {
	t.Run("Get: inverted node (End < Start) returns a clean error, not a corrupt Node", func(t *testing.T) {
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 50, 10) // Start > End
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		_, err = dTree.Get([]byte("key-000"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "inverted")
	})

	t.Run("Seek: inverted node (End < Start) returns a clean error via readNode", func(t *testing.T) {
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 50, 10)
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		_, err = dTree.Seek([]byte("key-000"))
		require.Error(t, err)
	})

	t.Run("AllKeys: inverted node aborts the traversal with an error, not a panic downstream", func(t *testing.T) {
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 50, 10)
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		require.NotPanics(t, func() {
			_, err = dTree.AllKeys()
		})
		require.Error(t, err)
	})

	t.Run("Get: node End past a set dataEnd is rejected", func(t *testing.T) {
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 10, 200) // valid ordering, but End is past the data region
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		dTree.SetDataEnd(100)
		_, err = dTree.Get([]byte("key-000"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside the segment's data region")
	})

	t.Run("Get: an ordinary well-formed node is accepted without SetDataEnd (upper bound dormant by default)", func(t *testing.T) {
		// The start<=end ordering check is unconditional; only the dataEnd
		// upper bound depends on SetDataEnd having been called.
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 10, 20) // ordinary, well-formed node
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		node, err := dTree.Get([]byte("key-000"))
		require.NoError(t, err)
		require.Equal(t, uint64(10), node.Start)
		require.Equal(t, uint64(20), node.End)
	})

	t.Run("Get: a well-formed node with SetDataEnd active is accepted (no false rejection)", func(t *testing.T) {
		tree := NewTree(1)
		tree.Insert([]byte("key-000"), 10, 20)
		data, err := tree.MarshalBinary()
		require.NoError(t, err)

		dTree := NewDiskTree(data)
		dTree.SetDataEnd(100)
		node, err := dTree.Get([]byte("key-000"))
		require.NoError(t, err)
		require.Equal(t, uint64(10), node.Start)
		require.Equal(t, uint64(20), node.End)
	})
}

// TestDiskTreePinnedResidual_InteriorNodeCorruption documents a known,
// accepted gap: a redirected child pointer or an overwritten Start/End
// range, when individually valid, are undetectable by root-only or
// per-node checks - only full-tree structural validation could catch them,
// which is out of scope here. Pins CURRENT behavior so a regression here
// is noticed, not assumed away.
func TestDiskTreePinnedResidual_InteriorNodeCorruption(t *testing.T) {
	t.Run("wrong-child-node: a redirected child pointer causes silent NotFound for intact data", func(t *testing.T) {
		tree := NewTree(4)
		tree.Insert([]byte("m"), 100, 110) // root
		tree.Insert([]byte("b"), 50, 60)   // left child - the key this test looks up
		tree.Insert([]byte("t"), 150, 160) // right child - has no children of its own
		valid, err := tree.MarshalBinary()
		require.NoError(t, err)

		// Root's left child pointer, redirected from "b"'s real offset to
		// "t"'s offset. Both offsets are individually valid, in-bounds node
		// positions - this is not detectable by any O(1) or per-node check,
		// only by validating the whole tree's BST ordering invariant.
		rootKeyLen := int(binary.LittleEndian.Uint32(valid[0:4]))
		rootChildBase := 4 + rootKeyLen + 16
		leftOffset := int64(binary.LittleEndian.Uint64(valid[rootChildBase:]))
		rightOffset := int64(binary.LittleEndian.Uint64(valid[rootChildBase+8:]))
		require.NotEqual(t, leftOffset, rightOffset, "test precondition: left and right children must be distinct nodes")

		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		binary.LittleEndian.PutUint64(corrupt[rootChildBase:], uint64(rightOffset))

		dTree := NewDiskTree(corrupt)
		_, err = dTree.Get([]byte("b"))
		require.ErrorIs(t, err, lsmkv.NotFound,
			"pinned residual: a redirected-but-valid child pointer makes Get resolve to NotFound for data that is still physically present in the buffer")
	})

	t.Run("wrong-range: a node's Start/End overwritten with another key's valid range returns the wrong value silently", func(t *testing.T) {
		tree := NewTree(2)
		tree.Insert([]byte("m"), 100, 110) // root - the key this test looks up
		tree.Insert([]byte("b"), 50, 60)   // left child - its range will be substituted in
		valid, err := tree.MarshalBinary()
		require.NoError(t, err)

		// Root's own Start/End, overwritten with "b"'s valid, in-bounds
		// range. validateNodeRange only checks internal well-formedness
		// (start<=end<=dataEnd), which this satisfies; it cannot tell the
		// range belongs to a different key.
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		rootKeyLen := int(binary.LittleEndian.Uint32(valid[0:4]))
		startOffset := 4 + rootKeyLen
		binary.LittleEndian.PutUint64(corrupt[startOffset:], 50)
		binary.LittleEndian.PutUint64(corrupt[startOffset+8:], 60)

		dTree := NewDiskTree(corrupt)
		node, err := dTree.Get([]byte("m"))
		require.NoError(t, err, "pinned residual: this must NOT error - the range is well-formed, just wrong")
		require.Equal(t, uint64(50), node.Start, "pinned residual: Get(\"m\") silently returns \"b\"'s range instead of \"m\"'s own")
		require.Equal(t, uint64(60), node.End)
	})
}
