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
	"bytes"
	"fmt"
	"math"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memtableKeyWriter writes keys value-<i> for i in [start, end) into a memtable
// of one strategy. Each strategy lands in a different binary search tree, and
// Memtable.GetKeys walks whichever one holds data.
type memtableKeyWriter struct {
	name     string
	strategy string
	addKeys  func(t *testing.T, m *Memtable, start, end int)
}

// memtableKeyWriters covers every tree GetKeys can walk: key (replace),
// keyMulti (set), keyMap (map and inverted) and roaringSet.
var memtableKeyWriters = []memtableKeyWriter{
	{
		name:     "replace",
		strategy: StrategyReplace,
		addKeys: func(t *testing.T, m *Memtable, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, m.put(cardinalityKey(i), []byte(fmt.Sprintf("doc-%06d", i))))
			}
		},
	},
	{
		name:     "set",
		strategy: StrategySetCollection,
		addKeys: func(t *testing.T, m *Memtable, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, m.append(cardinalityKey(i),
					[]value{{value: []byte(fmt.Sprintf("doc-%06d", i))}}))
			}
		},
	},
	{
		name:     "map",
		strategy: StrategyMapCollection,
		addKeys: func(t *testing.T, m *Memtable, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, m.appendMapSorted(cardinalityKey(i),
					NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
			}
		},
	},
	{
		name:     "inverted",
		strategy: StrategyInverted,
		addKeys: func(t *testing.T, m *Memtable, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, m.appendMapSorted(cardinalityKey(i),
					NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
			}
		},
	},
	{
		name:     "roaringset",
		strategy: StrategyRoaringSet,
		addKeys: func(t *testing.T, m *Memtable, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, m.roaringSetAddOne(cardinalityKey(i), uint64(i)))
			}
		},
	},
}

// The memtables are counted exactly, including the keys a switch leaves in both
// of them, so every case here asserts the true count rather than a tolerance.
func TestMemtableKeysDistinct(t *testing.T) {
	tests := []struct {
		name         string
		activeKeys   int
		flushingKeys int
		// number of leading flushing keys the active memtable rewrites
		overlap  int
		distinct int
	}{
		{name: "active only", activeKeys: 500, distinct: 500},
		{name: "empty active only"},
		{name: "tiny active, large flushing", activeKeys: 1, flushingKeys: 20000, distinct: 20001},
		{name: "large active, tiny flushing", activeKeys: 20000, flushingKeys: 1, distinct: 20001},
		{name: "disjoint", activeKeys: 3000, flushingKeys: 3000, distinct: 6000},
		{name: "fully overlapping", activeKeys: 3000, flushingKeys: 3000, overlap: 3000, distinct: 3000},
		{name: "partially overlapping", activeKeys: 3000, flushingKeys: 3000, overlap: 1200, distinct: 4800},
	}

	for _, w := range memtableKeyWriters {
		t.Run(w.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					const flushingBase = 1000000
					// the active memtable starts at the base for the overlapping keys and
					// runs past the flushing range for the rest
					view := BucketConsistentView{}
					if tt.flushingKeys > 0 {
						view.Flushing = newPopulatedMemtable(t, w, flushingBase, flushingBase+tt.flushingKeys)
						view.Active = newPopulatedMemtable(t, w, flushingBase, flushingBase+tt.overlap)
						w.addKeys(t, view.Active.(*Memtable),
							flushingBase+tt.flushingKeys, flushingBase+tt.flushingKeys+tt.activeKeys-tt.overlap)
					} else {
						view.Active = newPopulatedMemtable(t, w, 0, tt.activeKeys)
					}

					memKeys, err := collectMemtableKeys(view)
					require.NoError(t, err)
					for i, set := range memKeys.sets {
						requireSortedDistinct(t, set, "memtable %d", i)
					}

					assert.Equal(t, uint32(tt.distinct), memKeys.distinct())
				})
			}
		})
	}

	// A roaringsetrange memtable keeps per-bit bitmaps instead of a key set, so
	// GetKeys must fail rather than report it as empty.
	t.Run("roaringsetrange is rejected", func(t *testing.T) {
		m := newMemtableForStrategy(t, StrategyRoaringSetRange)
		_, err := m.GetKeys()
		require.Error(t, err)

		_, err = collectMemtableKeys(BucketConsistentView{Active: m})
		require.Error(t, err)
	})
}

// exactKeys generalizes beyond the two memtables: bloom-less disk segments add
// more sorted sets. Cross-set duplicates must count once.
func TestExactKeysDistinct(t *testing.T) {
	keyRange := func(start, end int) [][]byte {
		set := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			set = append(set, []byte(fmt.Sprintf("value-%06d", i)))
		}
		return set
	}

	tests := []struct {
		name     string
		sets     [][][]byte
		distinct uint32
	}{
		{name: "no sets", distinct: 0},
		{name: "three disjoint", sets: [][][]byte{keyRange(0, 100), keyRange(100, 200), keyRange(200, 300)}, distinct: 300},
		{name: "three identical", sets: [][][]byte{keyRange(0, 100), keyRange(0, 100), keyRange(0, 100)}, distinct: 100},
		{name: "staggered overlap", sets: [][][]byte{keyRange(0, 100), keyRange(50, 150), keyRange(100, 200)}, distinct: 200},
		{name: "empty set among sets", sets: [][][]byte{keyRange(0, 100), nil, keyRange(50, 150)}, distinct: 150},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ek exactKeys
			for _, set := range tt.sets {
				ek.add(set)
			}
			assert.Equal(t, tt.distinct, ek.distinct())
		})
	}
}

func TestDistinctKeysRejectAbove(t *testing.T) {
	tests := []struct {
		name        string
		maxDistinct int
		want        uint64
	}{
		{name: "zero", maxDistinct: 0, want: 0},
		{name: "negative", maxDistinct: -7, want: 0},
		{name: "one", maxDistinct: 1, want: 65},
		{name: "floor applies below 512", maxDistinct: 500, want: 564},
		{name: "floor and relative margin meet at 512", maxDistinct: 512, want: 576},
		{name: "relative margin above 512", maxDistinct: 520, want: 585},
		{name: "relative margin at scale", maxDistinct: 100000, want: 112500},
		{name: "no overflow at max int32", maxDistinct: math.MaxInt32, want: math.MaxInt32 + math.MaxInt32/8},
		{name: "no overflow at max int", maxDistinct: math.MaxInt, want: uint64(math.MaxInt) + uint64(math.MaxInt/8)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, distinctKeysRejectAbove(tt.maxDistinct))
		})
	}
}

func cardinalityKey(i int) []byte {
	return []byte(fmt.Sprintf("value-%06d", i))
}

// requireSortedDistinct pins the invariant exactKeys.distinct() merges on:
// break it and the count is silently wrong rather than an error.
func requireSortedDistinct(t *testing.T, keys [][]byte, msg string, args ...any) {
	t.Helper()
	for i := 1; i < len(keys); i++ {
		require.Negativef(t, bytes.Compare(keys[i-1], keys[i]),
			"%s: keys must be ascending and free of duplicates, but %q at %d does not precede %q",
			fmt.Sprintf(msg, args...), keys[i-1], i-1, keys[i])
	}
}

func newMemtableForStrategy(t *testing.T, strategy string) *Memtable {
	t.Helper()
	logger, _ := test.NewNullLogger()
	memPath := path.Join(t.TempDir(), "fake")

	cl, err := newCommitLogger(memPath, strategy, 0)
	require.NoError(t, err)
	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     memPath,
		strategy: strategy,
	})
	require.NoError(t, err)
	return m
}

// newPopulatedMemtable returns a memtable of the writer's strategy holding keys
// value-<i> for i in [start, end).
func newPopulatedMemtable(t *testing.T, w memtableKeyWriter, start, end int) *Memtable {
	t.Helper()
	m := newMemtableForStrategy(t, w.strategy)
	w.addKeys(t, m, start, end)
	return m
}
