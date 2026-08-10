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

package keydoccolumn

import (
	"fmt"
	"testing"

	entinverted "github.com/weaviate/weaviate/entities/inverted"

	"github.com/stretchr/testify/require"
)

// prefixCorpus builds a segment whose keys are uniform width and share a leading
// prefix, so buildKeyColumn selects prefixKeyColumn: key_000..key_0NN, docID i.
func prefixCorpus(t *testing.T, n int) *segment {
	t.Helper()
	keys := make([][]byte, n)
	docs := make([]uint64, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%03d", i))
		docs[i] = uint64(i)
	}
	seg := segFromPairs(keys, docs)
	_, ok := seg.keys.(*prefixKeyColumn)
	require.True(t, ok, "corpus must select prefixKeyColumn for this test to mean anything")
	return seg
}

// TestPrefixColumnShortQueryKeys pins that query keys shorter than the elided
// shared prefix are handled rather than slicing out of range, on both resolve
// dispatch branches. The corpus is sized so the query key count alone decides
// whether resolveMatches merge-scans or binary-searches.
func TestPrefixColumnShortQueryKeys(t *testing.T) {
	const corpusSize = 64 // merge-scan takes over at 10+ query keys
	seg := prefixCorpus(t, corpusSize)
	require.Equal(t, 5, seg.keys.info().prefixLen, "prefix is key_0")

	// dense builds a sorted query of `below` + the first 16 corpus keys + `above`,
	// which is enough keys to push resolveMatches onto the merge-scan branch.
	dense := func(below, above []string) (entinverted.SortedKeys, []uint64) {
		kb := entinverted.NewKeyBuilder(len(below)+16+len(above), 0)
		for _, b := range below {
			kb.AppendString(b)
		}
		docs := make([]uint64, 16)
		for i := 0; i < 16; i++ {
			kb.AppendString(fmt.Sprintf("key_%03d", i))
			docs[i] = uint64(i)
		}
		for _, a := range above {
			kb.AppendString(a)
		}
		keys := kb.Build()
		require.True(t, keys.IsAscending(), "query keys must be ascending")
		return keys, docs
	}

	denseBelow, denseBelowWant := dense([]string{"ke"}, nil)
	denseBoth, denseBothWant := dense([]string{"", "ke", "key_"}, []string{"kez", "kf"})

	tests := []struct {
		name  string
		keys  entinverted.SortedKeys
		want  []uint64
		merge bool
	}{
		{
			name: "short key alone",
			keys: testKeys("ke"),
		},
		{
			name: "short key below the prefix, mixed with a hit",
			keys: testKeys("ke", "key_007"),
			want: []uint64{7},
		},
		{
			name: "key exactly as long as the prefix",
			keys: testKeys("key_0", "key_007"),
			want: []uint64{7},
		},
		{
			name: "empty key",
			keys: testKeys("", "key_007"),
			want: []uint64{7},
		},
		{
			name: "short key above the prefix",
			keys: testKeys("key_007", "kez"),
			want: []uint64{7},
		},
		{
			name:  "short key below the prefix, dense query",
			keys:  denseBelow,
			want:  denseBelowWant,
			merge: true,
		},
		{
			name:  "short keys on both sides of the prefix, dense query",
			keys:  denseBoth,
			want:  denseBothWant,
			merge: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.merge, mergeScanCheaper(tt.keys.Len(), corpusSize),
				"test case must exercise the branch it claims")

			res := newResolution(tt.keys.Len(), 1_000_000)
			seg.scanInto(tt.keys, res, true)

			got := res.SortedDocs()
			if len(got) == 0 {
				got = nil // the cases that match nothing want no documents, not an empty slice
			}
			require.Equal(t, tt.want, got)
		})
	}
}

// TestComparePrefix pins the ordering comparePrefix imposes. It must agree with
// the byte order of the keys themselves, or the query keys carrying the prefix
// would not form the contiguous window queryWindow binary-searches for.
func TestComparePrefix(t *testing.T) {
	prefix := []byte("key_0")

	tests := []struct {
		name string
		key  string
		want int
	}{
		{name: "empty", key: "", want: -1},
		{name: "shorter, below", key: "ke", want: -1},
		{name: "shorter, proper prefix of the prefix", key: "key_", want: -1},
		{name: "shorter, above", key: "kf", want: 1},
		{name: "exactly the prefix", key: "key_0", want: 0},
		{name: "carries the prefix", key: "key_012", want: 0},
		{name: "longer, below", key: "key_-xx", want: -1},
		{name: "longer, above", key: "key_1xx", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, comparePrefix([]byte(tt.key), prefix))
		})
	}
}

// testKeys builds ascending query keys from literals, so a table can state the
// keys it means.
func testKeys(keys ...string) entinverted.SortedKeys {
	kb := entinverted.NewKeyBuilder(len(keys), 0)
	for _, k := range keys {
		kb.AppendString(k)
	}
	return kb.Build()
}
