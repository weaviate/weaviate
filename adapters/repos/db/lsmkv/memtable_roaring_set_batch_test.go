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
	"errors"
	"fmt"
	"math/rand"
	"path"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/inverted"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TestRoaringSetGetBatchMatchesPerKey is the differential the batch read has to
// survive: for any memtable and any batch, reading the batch in one pass must
// answer exactly what reading each key on its own does. The two walk the tree
// completely differently — one descends per key, the other advances two cursors
// past each other — so agreement is the property worth pinning, and the shapes
// below are the ones where a cursor can overshoot: batches far sparser than the
// memtable, far denser, disjoint from it, and sharing only their ends.
func TestRoaringSetGetBatchMatchesPerKey(t *testing.T) {
	tests := []struct {
		name         string
		memtableKeys []string
		batchKeys    []string
	}{
		{"empty batch", []string{"b", "d"}, nil},
		{"empty memtable", nil, []string{"a", "b"}},
		{"single key, hit", []string{"b"}, []string{"b"}},
		{"single key, missed", []string{"b"}, []string{"a", "c"}},
		{"batch entirely before the memtable", []string{"y", "z"}, []string{"a", "b", "c"}},
		{"batch entirely after the memtable", []string{"a", "b"}, []string{"y", "z"}},
		{"disjoint, interleaved", []string{"b", "d", "f"}, []string{"a", "c", "e", "g"}},
		{"identical", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"memtable much sparser", []string{"m"}, letters()},
		{"batch much sparser", letters(), []string{"m"}},
		{"only the ends shared", []string{"a", "m", "z"}, []string{"a", "z"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memtableWith(t, tt.memtableKeys)
			keys := sortedKeysOf(tt.batchKeys)

			hits, err := m.roaringSetGetBatch(keys)
			require.NoError(t, err)

			// what reading one key at a time says, in the same form
			var wantAt []uint32
			for i := 0; i < keys.Len(); i++ {
				_, err := m.roaringSetGet(keys.At(i))
				if errors.Is(err, entlsmkv.NotFound) {
					continue
				}
				require.NoError(t, err)
				wantAt = append(wantAt, uint32(i))
			}

			require.Equal(t, wantAt, hits.At, "positions")
			require.Equal(t, len(wantAt), len(hits.Layers))
			for j, at := range hits.At {
				want, err := m.roaringSetGet(keys.At(int(at)))
				require.NoError(t, err)
				assert.Equalf(t, want.Additions.ToArray(), hits.Layers[j].Additions.ToArray(),
					"additions for key %q", keys.At(int(at)))
			}
		})
	}
}

// TestRoaringSetGetBatchRandomized runs the same differential over random
// shapes, which is where an off-by-one in either cursor's jump shows up: it
// would drop or duplicate a key that no hand-written case happens to place.
func TestRoaringSetGetBatchRandomized(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for round := 0; round < 200; round++ {
		universe := 40
		mem := sampleDistinct(rnd, universe, rnd.Intn(universe+1))
		batch := sampleDistinct(rnd, universe, rnd.Intn(universe+1))

		m := memtableWith(t, mem)
		keys := sortedKeysOf(batch)

		hits, err := m.roaringSetGetBatch(keys)
		require.NoError(t, err)

		var wantAt []uint32
		for i := 0; i < keys.Len(); i++ {
			if _, err := m.roaringSetGet(keys.At(i)); err == nil {
				wantAt = append(wantAt, uint32(i))
			}
		}
		require.Equalf(t, wantAt, hits.At, "round %d: memtable %v batch %v", round, mem, batch)
	}
}

// TestSearchGE pins the exponential reach against a linear scan. from is always
// a position the caller has already found to be before the target, which is the
// precondition the gallop's first step depends on.
func TestSearchGE(t *testing.T) {
	keys := sortedKeysOf([]string{"a", "c", "e", "g", "i", "k", "m", "o", "q", "s"})
	for _, target := range []string{"a", "b", "c", "j", "l", "r", "s", "t", ""} {
		for from := 0; from < keys.Len(); from++ {
			if bytesLess(target, string(keys.At(from))) || target == string(keys.At(from)) {
				continue // precondition: keys[from] must be before target
			}
			want := keys.Len()
			for i := from + 1; i < keys.Len(); i++ {
				if !bytesLess(string(keys.At(i)), target) {
					want = i
					break
				}
			}
			got := searchGE(keys, from, []byte(target))
			assert.Equalf(t, want, got, "searchGE(from=%d, target=%q)", from, target)
		}
	}
}

func memtableWith(t *testing.T, keys []string) *Memtable {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dir := path.Join(t.TempDir(), "fake")

	cl, err := newCommitLogger(dir, StrategyRoaringSet, 0)
	require.NoError(t, err)
	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     dir,
		strategy: StrategyRoaringSet,
	})
	require.NoError(t, err)

	for i, k := range keys {
		require.NoError(t, m.roaringSetAddOne([]byte(k), uint64(i)))
	}
	return m
}

func sortedKeysOf(keys []string) inverted.SortedKeys {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	total := 0
	for _, k := range sorted {
		total += len(k)
	}
	b := inverted.NewKeyBuilder(len(sorted), total)
	for _, k := range sorted {
		b.AppendString(k)
	}
	return b.Build()
}

func sampleDistinct(rnd *rand.Rand, universe, n int) []string {
	perm := rnd.Perm(universe)[:n]
	out := make([]string, 0, n)
	for _, v := range perm {
		out = append(out, fmt.Sprintf("key_%03d", v))
	}
	slices.Sort(out)
	return out
}

func letters() []string {
	out := make([]string, 0, 26)
	for c := byte('a'); c <= 'z'; c++ {
		out = append(out, string([]byte{c}))
	}
	return out
}

func bytesLess(a, b string) bool { return a < b }
