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
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memtableKeyWriter struct {
	name     string
	strategy string
	addKeys  func(t *testing.T, m *Memtable, start, end int)
}

var replaceKeyWriter = memtableKeyWriter{
	name:     "replace",
	strategy: StrategyReplace,
	addKeys: func(t *testing.T, m *Memtable, start, end int) {
		for i := start; i < end; i++ {
			require.NoError(t, m.put(cardinalityKey(i), []byte(fmt.Sprintf("doc-%06d", i))))
		}
	},
}

// memtableKeyWriters covers every tree Memtable.GetKeys can walk: key
// (replace), keyMulti (set), keyMap (map and inverted) and roaringSet.
var memtableKeyWriters = []memtableKeyWriter{
	replaceKeyWriter,
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

func TestMemtableKeysDistinct(t *testing.T) {
	for _, w := range memtableKeyWriters {
		t.Run(w.name, func(t *testing.T) {
			m := newMemtableForStrategy(t, w.strategy)
			// out of order and partly rewritten, so both the in-order walk and
			// the tree's own deduplication have to hold
			w.addKeys(t, m, 300, 600)
			w.addKeys(t, m, 0, 300)
			w.addKeys(t, m, 150, 450)

			keys, err := m.GetKeys()
			require.NoError(t, err)
			requireSortedDistinct(t, keys, "%s memtable", w.name)
			assert.Len(t, keys, 600)
		})
	}

	// A memtable switch leaves the keys rewritten after it in both memtables.
	// The merge is strategy-independent — it walks the view, not the tree — so
	// one strategy covers it.
	t.Run("keys in both memtables count once", func(t *testing.T) {
		view := BucketConsistentView{
			Flushing: newPopulatedMemtable(t, replaceKeyWriter, 0, 3000),
			Active:   newPopulatedMemtable(t, replaceKeyWriter, 1800, 4200),
		}

		memKeys, err := collectMemtableKeys(view)
		require.NoError(t, err)
		for i, set := range memKeys.sets {
			requireSortedDistinct(t, set, "memtable %d", i)
		}
		assert.Equal(t, uint32(4200), memKeys.distinct())
	})

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

// The sets fed to exactKeys are the view's memtables plus any disk segment too
// small to carry a bloom filter, so a key can appear in several of them and
// must still count once.
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

func newPopulatedMemtable(t *testing.T, w memtableKeyWriter, start, end int) *Memtable {
	t.Helper()
	m := newMemtableForStrategy(t, w.strategy)
	w.addKeys(t, m, start, end)
	return m
}
