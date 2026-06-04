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
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkIndexesWriteTo(b *testing.B) {
	index := Indexes{
		SecondaryIndexCount: 10,
	}
	start := HeaderSize
	for i := 0; i < 10; i++ {
		key := Key{Key: []byte(fmt.Sprintf("primary%d", i))}
		secondaryLength := 0
		for j := 0; j < 10; j++ {
			secondary := []byte(fmt.Sprintf("secondary%d", j))
			key.SecondaryKeys = append(key.SecondaryKeys, secondary)
			secondaryLength += len(secondary)
		}
		key.ValueStart = start
		key.ValueEnd = start + len(key.Key)*8 + secondaryLength*8
		index.Keys = append(index.Keys, key)
	}

	b.ResetTimer()

	for _, size := range []uint64{4096, math.MaxUint64} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			path := b.TempDir()

			for i := 0; i < b.N; i++ {
				f, err := os.Create(path + fmt.Sprintf("/test%d", i))
				require.NoError(b, err)

				w := bufio.NewWriter(f)

				_, err = index.WriteTo(w)
				require.NoError(b, err)

				require.NoError(b, w.Flush())
				require.NoError(b, f.Sync())
				require.NoError(b, f.Close())
			}
		})
	}
}

// TestMarshalSortedSecondaryFromKeys verifies that marshalSortedSecondaryFromKeys
// produces byte-identical output to the old NewBalanced + MarshalBinaryInto path.
func TestMarshalSortedSecondaryFromKeys(t *testing.T) {
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: 0, ValueEnd: 10, SecondaryKeys: [][]byte{[]byte("zz"), []byte("mm")}},
		{Key: []byte("bbb"), ValueStart: 10, ValueEnd: 20, SecondaryKeys: [][]byte{[]byte("aa"), []byte("zz")}},
		{Key: []byte("ccc"), ValueStart: 20, ValueEnd: 30, SecondaryKeys: [][]byte{[]byte("mm"), []byte("aa")}},
		{Key: []byte("ddd"), ValueStart: 30, ValueEnd: 40, SecondaryKeys: [][]byte{[]byte("ff"), []byte("ff")}},
	}

	for pos := 0; pos < 2; pos++ {
		t.Run(fmt.Sprintf("pos=%d matches NewBalanced+MarshalBinaryInto", pos), func(t *testing.T) {
			// Old path: build secondary nodes, sort, build tree, marshal.
			var secNodes Nodes
			for _, key := range keys {
				if pos < len(key.SecondaryKeys) {
					secNodes = append(secNodes, Node{
						Key:   key.SecondaryKeys[pos],
						Start: uint64(key.ValueStart),
						End:   uint64(key.ValueEnd),
					})
				}
			}
			sort.Sort(secNodes)
			tree := NewBalanced(secNodes)
			var wantBuf bytes.Buffer
			_, err := tree.MarshalBinaryInto(&wantBuf)
			require.NoError(t, err)

			// New path.
			var gotBuf bytes.Buffer
			n, err := marshalSortedSecondaryFromKeys(&gotBuf, keys, pos)
			require.NoError(t, err)

			assert.Equal(t, int64(wantBuf.Len()), n)
			assert.Equal(t, wantBuf.Bytes(), gotBuf.Bytes(),
				"secondary index at pos=%d must be byte-identical to tree-based serialization", pos)
		})
	}

	t.Run("empty secondary index returns zero bytes", func(t *testing.T) {
		var buf bytes.Buffer
		n, err := marshalSortedSecondaryFromKeys(&buf, keys, 5)
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Equal(t, 0, buf.Len())
	})

	t.Run("partial secondary keys", func(t *testing.T) {
		// Only some keys have a secondary key at pos=1.
		partialKeys := []Key{
			{Key: []byte("aaa"), ValueStart: 0, ValueEnd: 10, SecondaryKeys: [][]byte{[]byte("xx")}},
			{Key: []byte("bbb"), ValueStart: 10, ValueEnd: 20, SecondaryKeys: [][]byte{[]byte("yy"), []byte("bb")}},
			{Key: []byte("ccc"), ValueStart: 20, ValueEnd: 30, SecondaryKeys: [][]byte{[]byte("zz"), []byte("aa")}},
		}

		var secNodes Nodes
		for _, key := range partialKeys {
			if 1 < len(key.SecondaryKeys) {
				secNodes = append(secNodes, Node{
					Key:   key.SecondaryKeys[1],
					Start: uint64(key.ValueStart),
					End:   uint64(key.ValueEnd),
				})
			}
		}
		sort.Sort(secNodes)
		tree := NewBalanced(secNodes)
		var wantBuf bytes.Buffer
		_, err := tree.MarshalBinaryInto(&wantBuf)
		require.NoError(t, err)

		var gotBuf bytes.Buffer
		n, err := marshalSortedSecondaryFromKeys(&gotBuf, partialKeys, 1)
		require.NoError(t, err)

		assert.Equal(t, int64(wantBuf.Len()), n)
		assert.Equal(t, wantBuf.Bytes(), gotBuf.Bytes())
	})

	t.Run("DiskTree can look up secondary keys", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := marshalSortedSecondaryFromKeys(&buf, keys, 0)
		require.NoError(t, err)

		dTree := NewDiskTree(buf.Bytes())
		for _, key := range keys {
			n, err := dTree.Get(key.SecondaryKeys[0])
			require.NoError(t, err, "key=%q", key.SecondaryKeys[0])
			assert.Equal(t, key.SecondaryKeys[0], n.Key)
			assert.Equal(t, uint64(key.ValueStart), n.Start)
			assert.Equal(t, uint64(key.ValueEnd), n.End)
		}
	})
}

// TestMarshalSortedKeysFromKeys_LargeN exercises key counts that produce
// incomplete tree levels (non-power-of-two sizes) to catch off-by-one errors
// in BFS position mapping.
func TestMarshalSortedKeysFromKeys_LargeN(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 8, 15, 16, 31, 33, 100, 255, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			keys := make([]Key, n)
			nodes := make(Nodes, n)
			offset := 0
			for i := 0; i < n; i++ {
				k := []byte(fmt.Sprintf("key-%05d", i))
				keys[i] = Key{Key: k, ValueStart: offset, ValueEnd: offset + 10}
				nodes[i] = Node{Key: k, Start: uint64(offset), End: uint64(offset + 10)}
				offset += 10
			}

			// Old path.
			sort.Sort(nodes)
			tree := NewBalanced(nodes)
			var wantBuf bytes.Buffer
			_, err := tree.MarshalBinaryInto(&wantBuf)
			require.NoError(t, err)

			// New path.
			var gotBuf bytes.Buffer
			gotN, err := MarshalSortedKeysFromKeys(&gotBuf, keys)
			require.NoError(t, err)

			assert.Equal(t, int64(wantBuf.Len()), gotN)
			assert.Equal(t, wantBuf.Bytes(), gotBuf.Bytes(),
				"n=%d: output must be byte-identical", n)

			// Verify round-trip through DiskTree.
			dTree := NewDiskTree(gotBuf.Bytes())
			allKeys, err := dTree.AllKeys()
			require.NoError(t, err)
			assert.Len(t, allKeys, n)
		})
	}
}

// TestWriteDirectly verifies that writeDirectly produces valid output and that
// precomputed sizes yield identical bytes to on-the-fly computation.
func TestWriteDirectly(t *testing.T) {
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: int(HeaderSize), ValueEnd: int(HeaderSize) + 10, SecondaryKeys: [][]byte{[]byte("zz"), []byte("mm")}},
		{Key: []byte("bbb"), ValueStart: int(HeaderSize) + 10, ValueEnd: int(HeaderSize) + 20, SecondaryKeys: [][]byte{[]byte("aa"), []byte("zz")}},
		{Key: []byte("ccc"), ValueStart: int(HeaderSize) + 20, ValueEnd: int(HeaderSize) + 30, SecondaryKeys: [][]byte{[]byte("mm"), []byte("aa")}},
	}

	t.Run("precomputed and non-precomputed produce identical output", func(t *testing.T) {
		idxA := &Indexes{
			Keys:                keys,
			SecondaryIndexCount: 2,
		}
		var bufA bytes.Buffer
		nA, err := idxA.WriteTo(&bufA)
		require.NoError(t, err)

		idxB := &Indexes{
			Keys:                        keys,
			SecondaryIndexCount:         2,
			SizesPrecomputed:            true,
			PrecomputedPrimaryIndexSize: computePrimaryIndexSize(keys),
			PrecomputedSecondaryIndexSizes: []int64{
				computeSecondaryIndexSize(keys, 0),
				computeSecondaryIndexSize(keys, 1),
			},
		}
		var bufB bytes.Buffer
		nB, err := idxB.WriteTo(&bufB)
		require.NoError(t, err)

		assert.Equal(t, nA, nB)
		assert.Equal(t, bufA.Bytes(), bufB.Bytes(),
			"precomputed and non-precomputed must produce byte-identical output")
	})

	t.Run("output length matches reported written bytes", func(t *testing.T) {
		idx := &Indexes{
			Keys:                keys,
			SecondaryIndexCount: 2,
		}
		var buf bytes.Buffer
		n, err := idx.WriteTo(&buf)
		require.NoError(t, err)
		assert.Equal(t, int64(buf.Len()), n)
		// offset table (2*8) + primary index + 2 secondary indexes
		assert.True(t, buf.Len() > 16, "output must include offset table + indexes")
	})

	t.Run("no secondary indices bypasses writeDirectly", func(t *testing.T) {
		primaryOnly := []Key{
			{Key: []byte("aaa"), ValueStart: 0, ValueEnd: 10},
			{Key: []byte("bbb"), ValueStart: 10, ValueEnd: 20},
		}
		idx := &Indexes{
			Keys:                primaryOnly,
			SecondaryIndexCount: 0,
		}
		var buf bytes.Buffer
		n, err := idx.WriteTo(&buf)
		require.NoError(t, err)
		assert.Equal(t, int64(buf.Len()), n)

		// Should be readable as a DiskTree (no offset table prefix).
		dTree := NewDiskTree(buf.Bytes())
		node, err := dTree.Get([]byte("aaa"))
		require.NoError(t, err)
		assert.Equal(t, []byte("aaa"), node.Key)
	})
}
