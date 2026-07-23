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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// requireSameTree asserts two serialized index blobs describe the same tree:
// identical total size and identical resolution of every key through DiskTree.
// The van Emde Boas writer reorders nodes relative to the level-order Tree
// serializer, so the guarantee is semantic, not byte-for-byte.
func requireSameTree(t *testing.T, want, got []byte, keys [][]byte) {
	t.Helper()
	require.Equal(t, len(want), len(got), "serialized size must match")
	wantTree := NewDiskTree(want)
	gotTree := NewDiskTree(got)
	for _, k := range keys {
		wantNode, wantErr := wantTree.Get(k)
		gotNode, gotErr := gotTree.Get(k)
		require.NoError(t, wantErr, "key=%q", k)
		require.NoError(t, gotErr, "key=%q", k)
		require.Equal(t, wantNode, gotNode, "key=%q", k)
	}
}

func TestTree(t *testing.T) {
	type elem struct {
		key   []byte
		start uint64
		end   uint64
	}

	tree := NewTree(4)

	elements := []elem{
		{
			key:   []byte("foobar"),
			start: 17,
			end:   18,
		},
		{
			key:   []byte("abc"),
			start: 4,
			end:   5,
		},
		{
			key:   []byte("zzz"),
			start: 34,
			end:   35,
		},
		{
			key:   []byte("aaa"),
			start: 1,
			end:   2,
		},
		{
			// makes the tree slightly imbalanced to the right, which in turn assures
			// that we have a nil node in between
			key:   []byte("zzzz"),
			start: 100,
			end:   102,
		},
	}

	t.Run("inserting", func(t *testing.T) {
		for _, elem := range elements {
			tree.Insert(elem.key, elem.start, elem.end)
		}
	})

	t.Run("exact get", func(t *testing.T) {
		key, start, end := tree.Get([]byte("foobar"))
		assert.Equal(t, []byte("foobar"), key)
		assert.Equal(t, uint64(17), start)
		assert.Equal(t, uint64(18), end)

		key, start, end = tree.Get([]byte("abc"))
		assert.Equal(t, []byte("abc"), key)
		assert.Equal(t, uint64(4), start)
		assert.Equal(t, uint64(5), end)

		key, start, end = tree.Get([]byte("zzz"))
		assert.Equal(t, []byte("zzz"), key)
		assert.Equal(t, uint64(34), start)
		assert.Equal(t, uint64(35), end)

		key, start, end = tree.Get([]byte("aaa"))
		assert.Equal(t, []byte("aaa"), key)
		assert.Equal(t, uint64(1), start)
		assert.Equal(t, uint64(2), end)

		key, start, end = tree.Get([]byte("zzzz"))
		assert.Equal(t, []byte("zzzz"), key)
		assert.Equal(t, uint64(100), start)
		assert.Equal(t, uint64(102), end)
	})

	t.Run("marshalling and then reading the byte representation", func(t *testing.T) {
		bytes, err := tree.MarshalBinary()
		require.Nil(t, err)

		dTree := NewDiskTree(bytes)

		t.Run("get", func(t *testing.T) {
			n, err := dTree.Get([]byte("foobar"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Get([]byte("abc"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Get([]byte("zzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Get([]byte("aaa"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Get([]byte("zzzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)
		})

		t.Run("seek", func(t *testing.T) {
			n, err := dTree.Seek([]byte("foobar"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Seek([]byte("f"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Seek([]byte("abc"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Seek([]byte("ab"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Seek([]byte("zzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Seek([]byte("z"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Seek([]byte("aaa"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Seek([]byte("a"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Seek([]byte("zzzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)

			n, err = dTree.Seek([]byte("zzza"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)

			n, err = dTree.Seek([]byte("zzzzz"))
			assert.Equal(t, lsmkv.NotFound, err)
		})

		t.Run("get all keys (for building bloom filters at segment init time)", func(t *testing.T) {
			expected := [][]byte{
				[]byte("aaa"),
				[]byte("abc"),
				[]byte("foobar"),
				[]byte("zzz"),
				[]byte("zzzz"),
			}

			keys, err := dTree.AllKeys()

			require.Nil(t, err)
			assert.ElementsMatch(t, expected, keys)
		})
	})
}

func TestMarshalSortedKeysFromKeys(t *testing.T) {
	// Same elements as TestTree, pre-sorted by key.
	sortedKeys := []Key{
		{Key: []byte("aaa"), ValueStart: 1, ValueEnd: 2},
		{Key: []byte("abc"), ValueStart: 4, ValueEnd: 5},
		{Key: []byte("foobar"), ValueStart: 17, ValueEnd: 18},
		{Key: []byte("zzz"), ValueStart: 34, ValueEnd: 35},
		{Key: []byte("zzzz"), ValueStart: 100, ValueEnd: 102},
	}

	t.Run("empty input returns zero bytes", func(t *testing.T) {
		var buf bytes.Buffer
		n, err := MarshalSortedKeysFromKeys(&buf, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Equal(t, 0, buf.Len())
	})

	t.Run("equivalent to tree MarshalBinary", func(t *testing.T) {
		// Build the same balanced tree via NewBalanced and marshal it.
		// NewBalanced (not Insert) produces a balanced BST over the same keys.
		nodes := make(Nodes, len(sortedKeys))
		keyBytes := make([][]byte, len(sortedKeys))
		for i, k := range sortedKeys {
			nodes[i] = Node{Key: k.Key, Start: uint64(k.ValueStart), End: uint64(k.ValueEnd)}
			keyBytes[i] = k.Key
		}
		tree := NewBalanced(nodes)
		want, err := tree.MarshalBinary()
		require.NoError(t, err)

		// Marshal directly from sorted keys.
		var buf bytes.Buffer
		n, err := MarshalSortedKeysFromKeys(&buf, sortedKeys)
		require.NoError(t, err)

		assert.Equal(t, int64(buf.Len()), n)
		requireSameTree(t, want, buf.Bytes(), keyBytes)
	})

	t.Run("DiskTree can Get every key", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := MarshalSortedKeysFromKeys(&buf, sortedKeys)
		require.NoError(t, err)

		dTree := NewDiskTree(buf.Bytes())
		for _, k := range sortedKeys {
			n, err := dTree.Get(k.Key)
			require.NoError(t, err)
			assert.Equal(t, k.Key, n.Key)
			assert.Equal(t, uint64(k.ValueStart), n.Start)
			assert.Equal(t, uint64(k.ValueEnd), n.End)
		}
	})

	t.Run("DiskTree Seek returns first key >= query", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := MarshalSortedKeysFromKeys(&buf, sortedKeys)
		require.NoError(t, err)

		dTree := NewDiskTree(buf.Bytes())

		n, err := dTree.Seek([]byte("f"))
		require.NoError(t, err)
		assert.Equal(t, []byte("foobar"), n.Key)

		n, err = dTree.Seek([]byte("zzza"))
		require.NoError(t, err)
		assert.Equal(t, []byte("zzzz"), n.Key)

		_, err = dTree.Seek([]byte("zzzzz"))
		assert.Equal(t, lsmkv.NotFound, err)
	})

	t.Run("DiskTree AllKeys returns all keys", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := MarshalSortedKeysFromKeys(&buf, sortedKeys)
		require.NoError(t, err)

		dTree := NewDiskTree(buf.Bytes())
		keys, err := dTree.AllKeys()
		require.NoError(t, err)

		expected := make([][]byte, len(sortedKeys))
		for i, k := range sortedKeys {
			expected[i] = k.Key
		}
		assert.ElementsMatch(t, expected, keys)
	})
}

// TestMarshalSortedKeysVanEmdeBoasOrder pins the on-disk node order to the van
// Emde Boas layout. For a full tree of height 4 (15 keys) it differs from level
// order, so this fails if the writer regresses to level order. The expected
// permutation is the sorted-key indices in write order, hand-derived from the
// vEB recursion (top block {0,1,2}, then bottom subtrees under 3,4,5,6).
func TestMarshalSortedKeysVanEmdeBoasOrder(t *testing.T) {
	const n = 15
	keys := make([]Key, n)
	for i := 0; i < n; i++ {
		keys[i] = Key{Key: []byte(fmt.Sprintf("key-%05d", i)), ValueStart: i, ValueEnd: i + 1}
	}

	var buf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&buf, keys)
	require.NoError(t, err)

	// AllKeys walks the blob sequentially, so it returns keys in write order.
	got, err := NewDiskTree(buf.Bytes()).AllKeys()
	require.NoError(t, err)

	vebSortedIndices := []int{7, 3, 11, 1, 0, 2, 5, 4, 6, 9, 8, 10, 13, 12, 14}
	want := make([][]byte, n)
	for i, idx := range vebSortedIndices {
		want[i] = []byte(fmt.Sprintf("key-%05d", idx))
	}
	assert.Equal(t, want, got)

	// The root must sit at offset 0 so pointer-chasing readers start there.
	root, err := NewDiskTree(buf.Bytes()).Get([]byte("key-00007"))
	require.NoError(t, err)
	assert.Equal(t, want[0], root.Key)
}

// MarshalSortedKeys must place the first key's data at the caller-provided
// dataStartOffset (not a hard-coded HeaderSize), while later keys chain from the
// previous key's ValueEnd. This is what lets inverted segments, whose data
// begins after an extended header, use the direct KeyRedux marshaller.
func TestMarshalSortedKeysDataStartOffset(t *testing.T) {
	keys := []KeyRedux{
		{Key: []byte("aaa"), ValueEnd: 40},
		{Key: []byte("bbb"), ValueEnd: 55},
		{Key: []byte("ccc"), ValueEnd: 70},
	}
	const dataStart = 27 // not HeaderSize — e.g. an inverted extended header

	var buf bytes.Buffer
	_, err := MarshalSortedKeys(&buf, keys, dataStart)
	require.NoError(t, err)

	dTree := NewDiskTree(buf.Bytes())

	// first key starts at the provided offset, ends at its ValueEnd
	n, err := dTree.Get([]byte("aaa"))
	require.NoError(t, err)
	assert.Equal(t, uint64(dataStart), n.Start)
	assert.Equal(t, uint64(40), n.End)

	// later keys chain from the previous ValueEnd
	n, err = dTree.Get([]byte("bbb"))
	require.NoError(t, err)
	assert.Equal(t, uint64(40), n.Start)
	assert.Equal(t, uint64(55), n.End)

	n, err = dTree.Get([]byte("ccc"))
	require.NoError(t, err)
	assert.Equal(t, uint64(55), n.Start)
	assert.Equal(t, uint64(70), n.End)
}

// A dataStartOffset past the first key's ValueEnd would serialize start > end;
// MarshalSortedKeys must reject it rather than emit a corrupt index.
func TestMarshalSortedKeysRejectsOffsetPastFirstValueEnd(t *testing.T) {
	keys := []KeyRedux{{Key: []byte("aaa"), ValueEnd: 10}}
	var buf bytes.Buffer
	_, err := MarshalSortedKeys(&buf, keys, 20)
	require.Error(t, err)
}
