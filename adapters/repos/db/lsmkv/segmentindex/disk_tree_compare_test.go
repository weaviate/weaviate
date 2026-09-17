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
	"cmp"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// FuzzEightByteCompareParity pins the property GetOffsets' fixed-width fast
// path relies on: for two 8-byte keys, comparing their big-endian uint64
// interpretations gives the same three-way result as bytes.Compare.
func FuzzEightByteCompareParity(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0xff, 0, 0, 0, 0, 0, 0, 1})
	f.Add([]byte{0x80, 0, 0, 0, 0, 0, 0, 0}, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if len(a) != 8 || len(b) != 8 {
			t.Skip()
		}
		got := cmp.Compare(binary.BigEndian.Uint64(a), binary.BigEndian.Uint64(b))
		want := bytes.Compare(a, b)
		if got != want {
			t.Fatalf("compare mismatch for %x vs %x: got %d want %d", a, b, got, want)
		}
	})
}

// TestGetOffsetsMixedKeyWidths descends trees whose keys mix 8-byte and
// variable-length entries, so a single descent alternates between the
// fixed-width fast path and bytes.Compare. An ordering disagreement between
// the two at any node sends the descent down the wrong child, which this
// test observes as NotFound for a present key.
func TestGetOffsetsMixedKeyWidths(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	var keys []Key
	seen := map[string]bool{}
	addKey := func(k []byte) {
		if seen[string(k)] {
			return
		}
		seen[string(k)] = true
		keys = append(keys, Key{Key: k})
	}
	for i := 0; i < 500; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, r.Uint64())
		addKey(k)
	}
	for i := 0; i < 500; i++ {
		k := make([]byte, 1+r.Intn(16))
		r.Read(k)
		if len(k) == 8 {
			k = k[:7]
		}
		addKey(k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
	// unique per-key payload offsets, so the assertions catch a descent that
	// lands on the wrong node, not only one that misses entirely
	for i := range keys {
		keys[i].ValueStart = i * 10
		keys[i].ValueEnd = i*10 + 5
	}

	var vebBuf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&vebBuf, keys)
	require.NoError(t, err)

	var levelBuf bytes.Buffer
	levelTree := NewBalanced(primaryNodes(keys))
	_, err = levelTree.MarshalBinaryInto(&levelBuf)
	require.NoError(t, err)

	absent := [][]byte{
		{},
		{0x01},
		[]byte("no-such-key"),
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}
	for i := 0; i < 100; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, r.Uint64())
		if !seen[string(k)] {
			absent = append(absent, k)
		}
	}

	layouts := map[string][]byte{
		"veb":   vebBuf.Bytes(),
		"level": levelBuf.Bytes(),
	}
	for name, blob := range layouts {
		t.Run(name, func(t *testing.T) {
			tree := NewDiskTree(blob)
			for _, k := range keys {
				start, end, err := tree.GetOffsets(k.Key)
				require.NoErrorf(t, err, "present key %x not found", k.Key)
				assert.Equal(t, uint64(k.ValueStart), start, "start for key %x", k.Key)
				assert.Equal(t, uint64(k.ValueEnd), end, "end for key %x", k.Key)
			}
			for _, k := range absent {
				_, _, err := tree.GetOffsets(k)
				assert.ErrorIsf(t, err, lsmkv.NotFound, "absent key %x", k)
			}
		})
	}
}

// TestGetOffsetsEightByteOrderingEdges pins the descent decisions around the
// values where a signed or little-endian misread of the fast path would
// reorder keys: the 0x80 sign boundary and single-byte differences at both
// ends of the word.
func TestGetOffsetsEightByteOrderingEdges(t *testing.T) {
	words := []uint64{
		0, 1, 0xff,
		0x7fffffffffffffff, 0x8000000000000000, 0x8000000000000001,
		0xff00000000000000, 0xfffffffffffffffe, 0xffffffffffffffff,
	}
	keys := make([]Key, len(words))
	for i, w := range words {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, w)
		keys[i] = Key{Key: k}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
	// unique per-key payload offsets, so the assertions catch a descent that
	// lands on the wrong node, not only one that misses entirely
	for i := range keys {
		keys[i].ValueStart = i * 10
		keys[i].ValueEnd = i*10 + 5
	}

	var buf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&buf, keys)
	require.NoError(t, err)
	tree := NewDiskTree(buf.Bytes())

	for _, k := range keys {
		start, end, err := tree.GetOffsets(k.Key)
		require.NoErrorf(t, err, "key %x not found", k.Key)
		assert.Equal(t, uint64(k.ValueStart), start, fmt.Sprintf("start for key %x", k.Key))
		assert.Equal(t, uint64(k.ValueEnd), end, fmt.Sprintf("end for key %x", k.Key))
	}
}
