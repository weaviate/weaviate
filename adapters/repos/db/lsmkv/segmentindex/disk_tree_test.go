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
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// A corrupt or truncated on-disk index must never crash the node: every read
// path (Get, Seek/Next, AllKeys) has to return NotFound or an error instead of
// panicking on an out-of-range slice.
func TestDiskTreeCorruptDataNeverPanics(t *testing.T) {
	// Sorted keys; the median ("foobar") becomes the BST root, which the van Emde
	// Boas writer emits first and therefore places at offset 0.
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: 1, ValueEnd: 2},
		{Key: []byte("abc"), ValueStart: 4, ValueEnd: 5},
		{Key: []byte("foobar"), ValueStart: 17, ValueEnd: 18},
		{Key: []byte("zzz"), ValueStart: 34, ValueEnd: 35},
		{Key: []byte("zzzz"), ValueStart: 100, ValueEnd: 102},
	}
	var buf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&buf, keys)
	require.NoError(t, err)
	valid := buf.Bytes()
	require.Greater(t, len(valid), TREE_KEY_STORE_OVERHEAD)

	// The corruption cases below only mean something if the intact blob resolves.
	for _, k := range keys {
		node, err := NewDiskTree(valid).Get(k.Key)
		require.NoError(t, err)
		require.Equal(t, uint64(k.ValueStart), node.Start)
		require.Equal(t, uint64(k.ValueEnd), node.End)
	}

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

// BenchmarkDiskTreeGet compares warm lookup latency between the two on-disk node
// orders the writers can produce: level order (Tree.MarshalBinaryInto) and van
// Emde Boas (MarshalSortedKeysFromKeys, the production layout). Both blobs are
// read through the same DiskTree, so the only difference is node placement. The
// whole index is resident in RAM, so this isolates the CPU-cache/TLB locality
// effect; the larger page-fault win under partial residency needs real I/O and
// is not modelled here.
//
// Keys are 8-byte big-endian docIDs, as the binary-quantized vector store
// writes them, and the win grows with n as the index outgrows the CPU caches.
//
// Run with: go test -run x -bench BenchmarkDiskTreeGet ./adapters/repos/db/lsmkv/segmentindex/
//
// For a single size anchor the sub-benchmark name — an unanchored n=100000 also
// matches n=1000000 and n=10000000:
//
//	go test -run x -bench 'BenchmarkDiskTreeGet/^n=100000$' ./adapters/repos/db/lsmkv/segmentindex/
func BenchmarkDiskTreeGet(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000, 10_000_000} {
		// Setup lives inside the sub-benchmark so selecting one n does not build
		// the blobs for the others (the 10M case costs GBs and minutes).
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := docIDKeys(n)
			levelOrder := NewBalanced(primaryNodes(keys))
			var levelBuf bytes.Buffer
			_, err := levelOrder.MarshalBinaryInto(&levelBuf)
			require.NoError(b, err)

			var vebBuf bytes.Buffer
			_, err = MarshalSortedKeysFromKeys(&vebBuf, keys)
			require.NoError(b, err)

			// Fixed random lookup order, shared across layouts for a fair comparison.
			// A wide probe set spreads lookups across the index so its layout, rather
			// than a few permanently-hot pages, drives the result. Length is a power of
			// two for the cheap index mask below.
			rng := rand.New(rand.NewSource(int64(n)))
			probes := make([][]byte, 65536)
			for i := range probes {
				probes[i] = keys[rng.Intn(n)].Key
			}

			layouts := []struct {
				name string
				data []byte
			}{
				{"level-order", levelBuf.Bytes()},
				{"van-Emde-Boas", vebBuf.Bytes()},
			}
			for _, l := range layouts {
				tree := NewDiskTree(l.data)
				b.Run(l.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := tree.Get(probes[i&(len(probes)-1)]); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
