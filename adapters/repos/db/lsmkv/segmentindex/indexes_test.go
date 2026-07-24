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
	"slices"
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
// builds the same tree as the NewBalanced + MarshalBinaryInto path.
func TestMarshalSortedSecondaryFromKeys(t *testing.T) {
	// Secondary keys deliberately differ in length: equal-length keys give every
	// node the same size, so a wrong node-size lookup still yields correct offsets.
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: 0, ValueEnd: 10, SecondaryKeys: [][]byte{[]byte("zzzzzz"), []byte("m")}},
		{Key: []byte("bbb"), ValueStart: 10, ValueEnd: 20, SecondaryKeys: [][]byte{[]byte("aa"), []byte("zzzz")}},
		{Key: []byte("ccc"), ValueStart: 20, ValueEnd: 30, SecondaryKeys: [][]byte{[]byte("m"), []byte("aaaaaaa")}},
		{Key: []byte("ddd"), ValueStart: 30, ValueEnd: 40, SecondaryKeys: [][]byte{[]byte("ffff"), []byte("ff")}},
	}

	for pos := 0; pos < 2; pos++ {
		t.Run(fmt.Sprintf("pos=%d equivalent to NewBalanced+MarshalBinaryInto", pos), func(t *testing.T) {
			// Reference path: build secondary nodes, sort, build tree, marshal.
			secNodes := secondaryNodes(keys, pos)
			secKeys := nodeKeys(secNodes)
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
			requireSameTree(t, wantBuf.Bytes(), gotBuf.Bytes(), secKeys)
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
			{Key: []byte("bbb"), ValueStart: 10, ValueEnd: 20, SecondaryKeys: [][]byte{[]byte("yy"), []byte("bbbbb")}},
			{Key: []byte("ccc"), ValueStart: 20, ValueEnd: 30, SecondaryKeys: [][]byte{[]byte("zz"), []byte("a")}},
		}

		secNodes := secondaryNodes(partialKeys, 1)
		secKeys := nodeKeys(secNodes)
		sort.Sort(secNodes)
		tree := NewBalanced(secNodes)
		var wantBuf bytes.Buffer
		_, err := tree.MarshalBinaryInto(&wantBuf)
		require.NoError(t, err)

		var gotBuf bytes.Buffer
		n, err := marshalSortedSecondaryFromKeys(&gotBuf, partialKeys, 1)
		require.NoError(t, err)

		assert.Equal(t, int64(wantBuf.Len()), n)
		requireSameTree(t, wantBuf.Bytes(), gotBuf.Bytes(), secKeys)
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

// TestSortedKeyWritersLargeN exercises key counts that produce incomplete tree
// levels (non-power-of-two sizes) to catch off-by-one errors in BFS position
// mapping, and counts from 9 up, where van Emde Boas order stops coinciding with
// level order and every node's offset depends on the reordering.
func TestSortedKeyWritersLargeN(t *testing.T) {
	for _, writer := range sortedKeyWriters() {
		for _, n := range []int{1, 2, 3, 7, 8, 9, 15, 16, 31, 33, 100, 255, 1000} {
			t.Run(fmt.Sprintf("%s/n=%d", writer.name, n), func(t *testing.T) {
				keys := make([]Key, n)
				offset := 0
				for i := 0; i < n; i++ {
					keys[i] = Key{Key: varWidthKey(i), ValueStart: offset, ValueEnd: offset + 10}
					// Secondary keys run in the opposite order and are absent on
					// every third key, so the secondary writer has to sort and
					// filter rather than mirror the primary index.
					if i%3 != 0 {
						keys[i].SecondaryKeys = [][]byte{varWidthKey(n - 1 - i)}
					}
					offset += 10
				}

				nodes := writer.indexedNodes(keys)
				keyBytes := nodeKeys(nodes)

				// Reference path.
				sort.Sort(nodes)
				tree := NewBalanced(nodes)
				var wantBuf bytes.Buffer
				_, err := tree.MarshalBinaryInto(&wantBuf)
				require.NoError(t, err)

				// New path.
				var gotBuf bytes.Buffer
				gotN, err := writer.write(&gotBuf, keys)
				require.NoError(t, err)

				assert.Equal(t, int64(wantBuf.Len()), gotN)
				requireSameTree(t, wantBuf.Bytes(), gotBuf.Bytes(), keyBytes)

				// Verify round-trip through DiskTree.
				dTree := NewDiskTree(gotBuf.Bytes())
				allKeys, err := dTree.AllKeys()
				require.NoError(t, err)
				assert.Len(t, allKeys, len(keyBytes))
			})
		}
	}
}

// FuzzSortedKeyWriters generalizes TestSortedKeyWritersLargeN, which pins both
// the key count and the key shape: one ascending family whose lengths cycle
// through seven values. The fuzzer picks the key set instead, so it reaches
// empty keys, keys sharing long prefixes, widths that vary from key to key, and
// counts anywhere around the power-of-two boundaries where the tree gains a
// level.
//
// Every case asserts the same thing: the writer's output resolves like the
// balanced tree built by NewBalanced over the same nodes, which is the property
// the van Emde Boas reordering has to preserve.
//
// The seed corpus runs on every `go test`; mutation needs an explicit run:
//
//	go test -run x -fuzz FuzzSortedKeyWriters ./adapters/repos/db/lsmkv/segmentindex/
func FuzzSortedKeyWriters(f *testing.F) {
	// Each seed is a length-prefixed key sequence, the encoding fuzzKeys reads.
	f.Add([]byte{})
	f.Add([]byte{0x00})                                   // a single empty key
	f.Add([]byte{0x01, 'a', 0x01, 'b', 0x01, 'c'})        // three one-byte keys
	f.Add([]byte{0x03, 'a', 'a', 'a', 0x01, 'z'})         // mixed widths
	f.Add([]byte{0x02, 'a', 'a', 0x03, 'a', 'a', 'b'})    // shared prefix
	f.Add(bytes.Repeat([]byte{0x01, 'k'}, 40))            // duplicates, deduped away
	f.Add([]byte{0x05, 'k', 'e', 'y'})                    // length runs past the input
	f.Add(bytes.Repeat([]byte{0x02, 'a', 'b', 0x00}, 64)) // many keys, empty ones mixed in

	f.Fuzz(func(t *testing.T, raw []byte) {
		keys := fuzzKeys(raw)
		if len(keys) == 0 {
			return
		}

		for _, writer := range sortedKeyWriters() {
			nodes := writer.indexedNodes(keys)

			// Reference path: the level-order writer over the same nodes.
			sort.Sort(nodes)
			tree := NewBalanced(nodes)
			var wantBuf bytes.Buffer
			_, err := tree.MarshalBinaryInto(&wantBuf)
			require.NoError(t, err, writer.name)

			var gotBuf bytes.Buffer
			gotN, err := writer.write(&gotBuf, keys)
			require.NoError(t, err, writer.name)
			require.Equal(t, int64(gotBuf.Len()), gotN,
				"%s: reported size must match the bytes written", writer.name)

			requireSameTree(t, wantBuf.Bytes(), gotBuf.Bytes(), nodeKeys(nodes))

			// requireSameTree only proves the two writers agree. Check the values
			// themselves too, since MarshalSortedKeys derives Start from the previous
			// key's ValueEnd rather than reading it.
			dTree := NewDiskTree(gotBuf.Bytes())
			for _, node := range nodes {
				got, err := dTree.Get(node.Key)
				require.NoError(t, err, "%s: Get(%q)", writer.name, node.Key)
				assert.Equal(t, node.Key, got.Key, writer.name)
				assert.Equal(t, node.Start, got.Start, "%s: start of %q", writer.name, node.Key)
				assert.Equal(t, node.End, got.End, "%s: end of %q", writer.name, node.Key)

				// Seek descends through readNodeAt rather than Get's own loop, so it
				// reads the child offsets the vEB reordering rewrote via a second path.
				seeked, err := dTree.Seek(node.Key)
				require.NoError(t, err, "%s: Seek(%q)", writer.name, node.Key)
				assert.Equal(t, node.Key, seeked.Key, writer.name)
			}

			allKeys, err := dTree.AllKeys()
			require.NoError(t, err, writer.name)
			assert.ElementsMatch(t, nodeKeys(nodes), allKeys, writer.name)
		}
	})
}

// fuzzKeys decodes raw into the sorted, unique key set the writers require: a
// sequence of one-byte lengths each followed by that many bytes. Duplicates are
// dropped rather than passed on, since a duplicate key breaks the writers'
// contract and would only rediscover Tree.insertAt's panic.
func fuzzKeys(raw []byte) []Key {
	// Bounded so one input stays cheap enough to keep the fuzzer's throughput up.
	const (
		maxKeys   = 256
		maxKeyLen = 48
	)

	var raws [][]byte
	for pos := 0; pos < len(raw) && len(raws) < maxKeys; {
		keyLen := int(raw[pos]) % (maxKeyLen + 1)
		pos++
		if keyLen > len(raw)-pos {
			keyLen = len(raw) - pos
		}
		raws = append(raws, raw[pos:pos+keyLen])
		pos += keyLen
	}

	slices.SortFunc(raws, bytes.Compare)
	raws = slices.CompactFunc(raws, bytes.Equal)

	keys := make([]Key, len(raws))
	offset := 0
	for i, key := range raws {
		// Every key holds a non-empty value, so Start and End differ and a writer
		// that mixed the two up would not still pass.
		span := len(key) + 1
		keys[i] = Key{Key: key, ValueStart: offset, ValueEnd: offset + span}
		offset += span

		// Secondary keys are the primary key reversed — still unique, but in a
		// different order — and absent on every third key, so the secondary writer
		// has to sort and filter rather than mirror the primary index.
		if i%3 != 0 {
			keys[i].SecondaryKeys = [][]byte{reversedKey(key)}
		}
	}

	return keys
}

func reversedKey(key []byte) []byte {
	out := bytes.Clone(key)
	slices.Reverse(out)
	return out
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

	// The offset table is written before the indexes themselves, so a size that
	// does not match what is written makes every offset point at the wrong bytes.
	t.Run("sizes the offset table was built from must match what is written", func(t *testing.T) {
		primarySize := computePrimaryIndexSize(keys)
		secondarySizes := []int64{
			computeSecondaryIndexSize(keys, 0),
			computeSecondaryIndexSize(keys, 1),
		}

		tests := []struct {
			name           string
			keys           []Key
			primarySize    int64
			secondarySizes []int64
			expectedErr    string
		}{
			{
				name:           "primary too small",
				keys:           keys,
				primarySize:    primarySize - 1,
				secondarySizes: secondarySizes,
				expectedErr:    "primary index size mismatch",
			},
			{
				name:           "primary too large",
				keys:           keys,
				primarySize:    primarySize + 1,
				secondarySizes: secondarySizes,
				expectedErr:    "primary index size mismatch",
			},
			{
				name:           "first secondary wrong",
				keys:           keys,
				primarySize:    primarySize,
				secondarySizes: []int64{secondarySizes[0] + 3, secondarySizes[1]},
				expectedErr:    "secondary index 0 size mismatch",
			},
			{
				name:           "last secondary wrong",
				keys:           keys,
				primarySize:    primarySize,
				secondarySizes: []int64{secondarySizes[0], secondarySizes[1] - 3},
				expectedErr:    "secondary index 1 size mismatch",
			},
			{
				name:           "no keys but non-zero sizes",
				keys:           nil,
				primarySize:    primarySize,
				secondarySizes: secondarySizes,
				expectedErr:    "primary index size mismatch",
			},
			{
				name:           "fewer precomputed sizes than secondary indexes falls back to computing",
				keys:           keys,
				primarySize:    primarySize,
				secondarySizes: secondarySizes[:1],
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				idx := &Indexes{
					Keys:                           test.keys,
					SecondaryIndexCount:            2,
					SizesPrecomputed:               true,
					PrecomputedPrimaryIndexSize:    test.primarySize,
					PrecomputedSecondaryIndexSizes: test.secondarySizes,
				}
				var buf bytes.Buffer
				_, err := idx.WriteTo(&buf)
				if test.expectedErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, test.expectedErr)
			})
		}
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
