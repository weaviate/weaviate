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
	"encoding/binary"
	"errors"
	"fmt"
	mathrand "math/rand"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func Test_BinarySearchTreeMap(t *testing.T) {
	t.Run("single row key, single map key", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		pair1 := MapPair{
			Key:   []byte("map-key-1"),
			Value: []byte("map-value-1"),
		}

		tree.insert(rowKey, pair1)

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("map-key-1"),
				Value: []byte("map-value-1"),
			},
		}, res)
	})

	t.Run("single row key, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b2"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)
	})

	t.Run("two row keys, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey1 := []byte("rowkey")
		rowKey2 := []byte("other-rowkey")

		tree.insert(rowKey1, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("z"),
			Value: []byte("z1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x2"),
		})

		res, err := tree.get(rowKey1)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b2"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)

		res, err = tree.get(rowKey2)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("x"),
				Value: []byte("x2"),
			},
			{
				Key:   []byte("z"),
				Value: []byte("z1"),
			},
		}, res)
	})

	t.Run("single row key, deleted map values", func(t *testing.T) {
		tree := &binarySearchTreeMap{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("b"),
			Tombstone: true,
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("a"),
			Tombstone: true,
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:       []byte("a"),
				Tombstone: true,
			},
			{
				Key:       []byte("b"),
				Tombstone: true,
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)
	})
}

func TestBSTMap_Flatten(t *testing.T) {
	t.Run("flattened bst is snapshot of current bst", func(t *testing.T) {
		rowkey1 := "rowkey-1"
		rowkey2 := "rowkey-2"
		rowkey3 := "rowkey-3"
		rowkey4 := "rowkey-4"

		rowkeys := map[string][]byte{
			rowkey1: []byte(rowkey1),
			rowkey2: []byte(rowkey2),
			rowkey3: []byte(rowkey3),
			rowkey4: []byte(rowkey4),
		}
		pairs := map[string]MapPair{
			rowkey1: {
				Key:       []byte("key-1"),
				Value:     []byte("val-1"),
				Tombstone: false,
			},
			rowkey2: {
				Key:       []byte("key-2"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey3: {
				Key:       []byte("key-3"),
				Value:     []byte("val-3"),
				Tombstone: false,
			},
		}
		pairsUpdated := map[string]MapPair{
			rowkey1: {
				Key:       []byte("key-1"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey2: {
				Key:       []byte("key-2"),
				Value:     []byte("val-22"),
				Tombstone: false,
			},
			rowkey3: {
				Key:       []byte("key-3"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey4: {
				Key:       []byte("key-4"),
				Value:     []byte("val-44"),
				Tombstone: false,
			},
		}

		type expectedFlattened struct {
			rowkey []byte
			pair   MapPair
		}
		assertFlattenedMatches := func(t *testing.T, flattened []*binarySearchNodeMap, expected []expectedFlattened) {
			t.Helper()
			require.Len(t, flattened, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp.rowkey, flattened[i].key)
				require.Len(t, flattened[i].values, 1)
				val := flattened[i].values[0]
				assert.Equal(t, exp.pair.Key, val.Key)
				assert.Equal(t, exp.pair.Value, val.Value)
				assert.Equal(t, exp.pair.Tombstone, val.Tombstone)
			}
		}

		bst := &binarySearchTreeMap{}
		// mixed order
		bst.insert(rowkeys[rowkey3], pairs[rowkey3])
		bst.insert(rowkeys[rowkey1], pairs[rowkey1])
		bst.insert(rowkeys[rowkey2], pairs[rowkey2])

		expectedBeforeUpdate := []expectedFlattened{
			{rowkeys[rowkey1], pairs[rowkey1]},
			{rowkeys[rowkey2], pairs[rowkey2]},
			{rowkeys[rowkey3], pairs[rowkey3]},
		}

		flatBeforeUpdate := bst.flattenInOrder()
		assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)

		t.Run("flattened bst does not change on bst update", func(t *testing.T) {
			// mixed order
			bst.insert(rowkeys[rowkey3], pairsUpdated[rowkey3])
			bst.insert(rowkeys[rowkey4], pairsUpdated[rowkey4])
			bst.insert(rowkeys[rowkey1], pairsUpdated[rowkey1])
			bst.insert(rowkeys[rowkey2], pairsUpdated[rowkey2])

			expectedAfterUpdate := []expectedFlattened{
				{rowkeys[rowkey1], pairsUpdated[rowkey1]},
				{rowkeys[rowkey2], pairsUpdated[rowkey2]},
				{rowkeys[rowkey3], pairsUpdated[rowkey3]},
				{rowkeys[rowkey4], pairsUpdated[rowkey4]},
			}

			flatAfterUpdate := bst.flattenInOrder()
			assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)
			assertFlattenedMatches(t, flatAfterUpdate, expectedAfterUpdate)
		})
	})
}

// TestBSTMap_SortedValuesIncremental drives interleaved insert/get sequences
// and checks every get against sortAndDedupValues over the raw pairs inserted so far.
func TestBSTMap_SortedValuesIncremental(t *testing.T) {
	type opKind int
	const (
		opInsert opKind = iota
		opGet
	)

	type step struct {
		op   opKind
		row  string
		pair MapPair
	}

	insert := func(row string, key, value string) step {
		return step{op: opInsert, row: row, pair: MapPair{Key: []byte(key), Value: []byte(value)}}
	}
	tombstone := func(row string, key string) step {
		return step{op: opInsert, row: row, pair: MapPair{Key: []byte(key), Tombstone: true}}
	}
	get := func(row string) step {
		return step{op: opGet, row: row}
	}

	cases := []struct {
		name  string
		steps []step
	}{
		{
			name:  "get on cold row",
			steps: []step{insert("r1", "k1", "v1"), get("r1")},
		},
		{
			name: "get-insert-get with a new map key",
			steps: []step{
				insert("r1", "k1", "v1"), get("r1"),
				insert("r1", "k2", "v2"), get("r1"),
			},
		},
		{
			name: "get-insert-get updating an existing key",
			steps: []step{
				insert("r1", "k1", "v1"), get("r1"),
				insert("r1", "k1", "v1-updated"), get("r1"),
			},
		},
		{
			name: "get-tombstone-get",
			steps: []step{
				insert("r1", "k1", "v1"), get("r1"),
				tombstone("r1", "k1"), get("r1"),
			},
		},
		{
			name: "several rounds of interleaved get/insert",
			steps: []step{
				insert("r1", "k3", "v3"), get("r1"),
				insert("r1", "k1", "v1"), get("r1"),
				insert("r1", "k2", "v2"), get("r1"),
				insert("r1", "k1", "v1-b"), get("r1"),
				insert("r1", "k4", "v4"), get("r1"),
			},
		},
		{
			name: "updates arriving in non-sorted key order",
			steps: []step{
				insert("r1", "k5", "v5"),
				insert("r1", "k1", "v1"),
				insert("r1", "k3", "v3"),
				get("r1"),
				insert("r1", "k2", "v2"),
				insert("r1", "k4", "v4"),
				get("r1"),
			},
		},
		{
			name: "two rows interleaved, writes to one must not disturb the other's cache",
			steps: []step{
				insert("r1", "k1", "v1"), insert("r2", "z1", "y1"),
				get("r1"), get("r2"),
				insert("r2", "z2", "y2"),
				get("r1"), get("r2"),
				insert("r1", "k2", "v2"),
				get("r1"), get("r2"),
			},
		},
		{
			name: "multiple updates to the same key between two gets",
			steps: []step{
				insert("r1", "k1", "v1"), get("r1"),
				insert("r1", "k1", "v1-a"),
				insert("r1", "k1", "v1-b"),
				insert("r1", "k1", "v1-c"),
				get("r1"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := &binarySearchTreeMap{}
			raw := map[string][]MapPair{}

			for i, s := range tc.steps {
				switch s.op {
				case opInsert:
					tree.insert([]byte(s.row), s.pair)
					raw[s.row] = append(raw[s.row], s.pair)
				case opGet:
					want := sortAndDedupValues(raw[s.row])
					got, err := tree.get([]byte(s.row))
					require.NoError(t, err, "step %d", i)
					require.Equal(t, want, got, "step %d", i)
				}
			}
		})
	}
}

// TestBSTMap_SnapshotImmutability verifies that a slice returned by get() is
// never mutated by later inserts, including inserts that update a key
// present in the already-returned snapshot.
func TestBSTMap_SnapshotImmutability(t *testing.T) {
	tree := &binarySearchTreeMap{}
	row := []byte("row")

	tree.insert(row, MapPair{Key: []byte("k1"), Value: []byte("v1")})
	tree.insert(row, MapPair{Key: []byte("k2"), Value: []byte("v2")})
	tree.insert(row, MapPair{Key: []byte("k3"), Value: []byte("v3")})

	snap, err := tree.get(row)
	require.NoError(t, err)

	snapCopy := make([]MapPair, len(snap))
	for i, p := range snap {
		snapCopy[i] = MapPair{
			Key:       append([]byte(nil), p.Key...),
			Value:     append([]byte(nil), p.Value...),
			Tombstone: p.Tombstone,
		}
	}

	// a brand new key, plus an update to a key already present in snap
	tree.insert(row, MapPair{Key: []byte("k4"), Value: []byte("v4")})
	tree.insert(row, MapPair{Key: []byte("k2"), Value: []byte("v2-updated")})

	assert.Equal(t, snapCopy, snap, "previously returned snapshot must not change after later inserts")

	fresh, err := tree.get(row)
	require.NoError(t, err)
	assert.Equal(t, []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2-updated")},
		{Key: []byte("k3"), Value: []byte("v3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	}, fresh)
}

// TestBSTMap_FlattenGivesPrivateCopies verifies that flattenInOrder's
// per-node values slices belong to the caller: mutating them in place (as
// cursor consumers do, see legacyRequireManualSorting) must not corrupt the
// tree's own cached snapshot or a later flatten.
func TestBSTMap_FlattenGivesPrivateCopies(t *testing.T) {
	tree := &binarySearchTreeMap{}
	rowA := []byte("rowA")
	rowB := []byte("rowB")

	tree.insert(rowA, MapPair{Key: []byte("a1"), Value: []byte("va1")})
	tree.insert(rowA, MapPair{Key: []byte("a2"), Value: []byte("va2")})
	tree.insert(rowB, MapPair{Key: []byte("b1"), Value: []byte("vb1")})

	flat := tree.flattenInOrder()
	require.Len(t, flat, 2)

	for _, node := range flat {
		vals := node.values
		for i, j := 0, len(vals)-1; i < j; i, j = i+1, j-1 {
			vals[i], vals[j] = vals[j], vals[i]
		}
		if len(vals) > 0 {
			vals[0].Value = []byte("mutated")
		}
	}

	gotA, err := tree.get(rowA)
	require.NoError(t, err)
	assert.Equal(t, []MapPair{
		{Key: []byte("a1"), Value: []byte("va1")},
		{Key: []byte("a2"), Value: []byte("va2")},
	}, gotA)

	gotB, err := tree.get(rowB)
	require.NoError(t, err)
	assert.Equal(t, []MapPair{
		{Key: []byte("b1"), Value: []byte("vb1")},
	}, gotB)

	flat2 := tree.flattenInOrder()
	require.Len(t, flat2, 2)
	assert.Equal(t, []MapPair{
		{Key: []byte("a1"), Value: []byte("va1")},
		{Key: []byte("a2"), Value: []byte("va2")},
	}, flat2[0].values)
	assert.Equal(t, []MapPair{
		{Key: []byte("b1"), Value: []byte("vb1")},
	}, flat2[1].values)
}

// TestBSTMap_CacheContract pins the cache contract: repeated reads share one
// immutable snapshot (same backing array, zero allocations); an append publishes a replacement, leaving prior snapshots untouched.
func TestBSTMap_CacheContract(t *testing.T) {
	tree := &binarySearchTreeMap{}
	row := []byte("row")

	tree.insert(row, MapPair{Key: []byte("k1"), Value: []byte("v1")})
	tree.insert(row, MapPair{Key: []byte("k2"), Value: []byte("v2")})
	tree.insert(row, MapPair{Key: []byte("k3"), Value: []byte("v3")})

	got1, err := tree.get(row)
	require.NoError(t, err)
	got2, err := tree.get(row)
	require.NoError(t, err)
	require.NotEmpty(t, got1)
	require.Same(t, &got1[0], &got2[0], "two consecutive reads must share the same backing array")

	allocs := testing.AllocsPerRun(100, func() {
		if _, err := tree.get(row); err != nil {
			t.Fatal(err)
		}
	})
	require.Equal(t, float64(0), allocs, "a warm read must not allocate")

	preInsertSnapshot := got2
	preInsertCopy := make([]MapPair, len(preInsertSnapshot))
	copy(preInsertCopy, preInsertSnapshot)

	tree.insert(row, MapPair{Key: []byte("k4"), Value: []byte("v4")})
	_, err = tree.get(row)
	require.NoError(t, err)

	require.Equal(t, len(tree.root.values), tree.root.sortedCache.Load().valueCount)
	require.Equal(t, preInsertCopy, preInsertSnapshot, "a snapshot returned before an insert must not change after it")
}

// TestBSTMap_CacheAccounting pins cacheSizeBytes(): it charges exactly the
// deduped MapPair count on a get, adjusts on structural changes, and is
// never touched by flattenInOrder's private, publish-free reads.
func TestBSTMap_CacheAccounting(t *testing.T) {
	pairSize := int64(unsafe.Sizeof(MapPair{}))

	tree := &binarySearchTreeMap{}
	require.Zero(t, tree.cacheSizeBytes())

	row := []byte("row")
	tree.insert(row, MapPair{Key: []byte("k1"), Value: []byte("v1")})
	tree.insert(row, MapPair{Key: []byte("k3"), Value: []byte("v3")})
	tree.insert(row, MapPair{Key: []byte("k2"), Value: []byte("v2")})

	t.Run("charges the deduped count on first get", func(t *testing.T) {
		_, err := tree.get(row)
		require.NoError(t, err)
		require.Equal(t, 3*pairSize, tree.cacheSizeBytes())
	})

	t.Run("adjusts after appending a new key", func(t *testing.T) {
		tree.insert(row, MapPair{Key: []byte("k4"), Value: []byte("v4")})
		_, err := tree.get(row)
		require.NoError(t, err)
		require.Equal(t, 4*pairSize, tree.cacheSizeBytes())
	})

	t.Run("unchanged after an update to an existing key", func(t *testing.T) {
		tree.insert(row, MapPair{Key: []byte("k2"), Value: []byte("v2-updated")})
		_, err := tree.get(row)
		require.NoError(t, err)
		require.Equal(t, 4*pairSize, tree.cacheSizeBytes())
	})

	t.Run("flattenInOrder on a cold row does not publish a cache", func(t *testing.T) {
		coldTree := &binarySearchTreeMap{}
		coldRow := []byte("cold-row")
		coldTree.insert(coldRow, MapPair{Key: []byte("a"), Value: []byte("1")})
		coldTree.insert(coldRow, MapPair{Key: []byte("b"), Value: []byte("2")})

		flat := coldTree.flattenInOrder()
		require.Len(t, flat, 1)
		require.Zero(t, coldTree.cacheSizeBytes())
	})

	t.Run("flattenInOrder on a warm row does not double-charge", func(t *testing.T) {
		before := tree.cacheSizeBytes()
		flat := tree.flattenInOrder()
		require.Len(t, flat, 1)
		require.Equal(t, before, tree.cacheSizeBytes())
	})

	t.Run("Memtable.Size() grows by the cache bytes after the first getMap", func(t *testing.T) {
		dir := t.TempDir()
		logger, _ := test.NewNullLogger()
		cl, err := newCommitLogger(dir, StrategyMapCollection, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     path.Join(dir, "cache-accounting"),
			strategy: StrategyMapCollection,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, m.commitlog.close())
		})

		mRow := []byte("m-row")
		const k = 5
		for i := 0; i < k; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))
			require.NoError(t, m.appendMapSorted(mRow, MapPair{Key: key, Value: []byte("v")}))
		}

		sizeBefore := m.Size()

		_, err = m.getMap(mRow)
		require.NoError(t, err)
		sizeAfterFirst := m.Size()
		require.Equal(t, sizeBefore+uint64(k*pairSize), sizeAfterFirst)

		_, err = m.getMap(mRow)
		require.NoError(t, err)
		require.Equal(t, sizeAfterFirst, m.Size())
	})
}

// TestBSTMap_RandomizedAgainstReference runs a large fixed-seed sequence of
// inserts/gets against sortAndDedupValues, and re-checks captured snapshots at the end for in-place mutation.
func TestBSTMap_RandomizedAgainstReference(t *testing.T) {
	rng := mathrand.New(mathrand.NewSource(42))
	tree := &binarySearchTreeMap{}

	rowKeys := []string{"row-0", "row-1", "row-2", "row-3"}
	const numMapKeys = 16
	mapKeys := make([][]byte, numMapKeys)
	for i := range mapKeys {
		mapKeys[i] = []byte(fmt.Sprintf("mk-%02d", i))
	}

	raw := map[string][]MapPair{}

	type snapshot struct {
		row  string
		vals []MapPair
		copy []MapPair
	}
	var snapshots []snapshot

	const numOps = 5000
	for op := 0; op < numOps; op++ {
		row := rowKeys[rng.Intn(len(rowKeys))]

		if rng.Intn(2) == 0 {
			pair := MapPair{Key: append([]byte(nil), mapKeys[rng.Intn(numMapKeys)]...)}
			if rng.Intn(10) == 0 {
				pair.Tombstone = true
			} else {
				val := make([]byte, 4)
				rng.Read(val)
				pair.Value = val
			}
			tree.insert([]byte(row), pair)
			raw[row] = append(raw[row], pair)
		} else {
			want := sortAndDedupValues(raw[row])
			got, err := tree.get([]byte(row))
			if len(raw[row]) == 0 {
				require.ErrorIs(t, err, lsmkv.NotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		}

		if op%500 == 0 && len(raw[row]) > 0 {
			vals, err := tree.get([]byte(row))
			require.NoError(t, err)
			cp := make([]MapPair, len(vals))
			for i, p := range vals {
				cp[i] = MapPair{
					Key:       append([]byte(nil), p.Key...),
					Value:     append([]byte(nil), p.Value...),
					Tombstone: p.Tombstone,
				}
			}
			snapshots = append(snapshots, snapshot{row: row, vals: vals, copy: cp})
		}
	}

	for _, s := range snapshots {
		assert.Equal(t, s.copy, s.vals, "captured snapshot for row %q mutated after later inserts", s.row)
	}
}

// TestMemtable_MapConcurrentReadWrite: one writer per row appends a
// deterministic key sequence while readers validate every read is a sorted, contiguous prefix of it.
func TestMemtable_MapConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	cl, err := newCommitLogger(dir, StrategyMapCollection, 0)
	require.NoError(t, err)

	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     path.Join(dir, "concurrent-map"),
		strategy: StrategyMapCollection,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.commitlog.close())
	})

	rowKeys := [][]byte{[]byte("row-0"), []byte("row-1"), []byte("row-2")}
	const opsPerWriter = 2000
	const numReaders = 4

	seqKey := func(seq uint64) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, seq)
		return k
	}
	seqValue := func(seq uint64) []byte {
		return []byte(fmt.Sprintf("v%08d", seq))
	}
	rowIndex := func(key []byte) int {
		for i, row := range rowKeys {
			if bytes.Equal(key, row) {
				return i
			}
		}
		return -1
	}

	// committed[i] counts appendMapSorted calls that have returned for
	// rowKeys[i]; a single writer per row makes it a sound lower bound on
	// what a concurrent read must observe.
	committed := make([]atomic.Int64, len(rowKeys))

	// validateRead pins the single-writer-per-row invariant: a read is
	// always the sorted, unique, contiguous prefix [0, len) of that row's
	// key sequence, at least as long as what was committed before the read.
	validateRead := func(rowIdx int, got []MapPair, committedBefore int64) {
		for i, p := range got {
			if !bytes.Equal(p.Key, seqKey(uint64(i))) {
				t.Errorf("row %d: pair %d has key %x, want seq key %d", rowIdx, i, p.Key, i)
				return
			}
			if !bytes.Equal(p.Value, seqValue(uint64(i))) {
				t.Errorf("row %d: pair %d has value %q, want %q", rowIdx, i, p.Value, seqValue(uint64(i)))
				return
			}
		}
		if int64(len(got)) < committedBefore {
			t.Errorf("row %d: read returned %d pairs, fewer than %d committed before the read", rowIdx, len(got), committedBefore)
		}
	}

	var writersWG sync.WaitGroup
	var allWG sync.WaitGroup
	done := make(chan struct{})

	writersWG.Add(len(rowKeys))
	allWG.Add(len(rowKeys))
	for rowIdx, row := range rowKeys {
		go func(rowIdx int, row []byte) {
			defer writersWG.Done()
			defer allWG.Done()
			for seq := 0; seq < opsPerWriter; seq++ {
				pair := MapPair{Key: seqKey(uint64(seq)), Value: seqValue(uint64(seq))}
				if err := m.appendMapSorted(row, pair); err != nil {
					t.Errorf("appendMapSorted: %v", err)
					return
				}
				committed[rowIdx].Add(1)
			}
		}(rowIdx, row)
	}

	allWG.Add(numReaders)
	for r := 0; r < numReaders; r++ {
		go func() {
			defer allWG.Done()
			for {
				select {
				case <-done:
					return
				default:
					for rowIdx, row := range rowKeys {
						before := committed[rowIdx].Load()
						got, err := m.getMap(row)
						if err != nil && !errors.Is(err, lsmkv.NotFound) {
							t.Errorf("getMap: %v", err)
							continue
						}
						validateRead(rowIdx, got, before)
					}
				}
			}
		}()
	}

	allWG.Add(1)
	go func() {
		defer allWG.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			before := make([]int64, len(rowKeys))
			for i := range rowKeys {
				before[i] = committed[i].Load()
			}

			cursor := m.newMapCursor()
			key, got, err := cursor.first()
			for err == nil {
				if rowIdx := rowIndex(key); rowIdx >= 0 {
					validateRead(rowIdx, got, before[rowIdx])
				}
				key, got, err = cursor.next()
			}
		}
	}()

	writersWG.Wait()
	close(done)
	allWG.Wait()

	for rowIdx, row := range rowKeys {
		n := committed[rowIdx].Load()
		want := make([]MapPair, n)
		for seq := int64(0); seq < n; seq++ {
			want[seq] = MapPair{Key: seqKey(uint64(seq)), Value: seqValue(uint64(seq))}
		}
		got, err := m.getMap(row)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

// BenchmarkBSTMapGet covers the two access patterns sortedValues() is meant
// to speed up: repeated reads with no new writes (cache hit, no sort at all)
// and a read immediately after a single new append (incremental merge of
// just the appended tail against the cached sorted snapshot).
func BenchmarkBSTMapGet(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}

	makeKey := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}

	for _, n := range sizes {
		n := n
		b.Run(fmt.Sprintf("repeat-read/n=%d", n), func(b *testing.B) {
			tree := &binarySearchTreeMap{}
			row := []byte("row")
			for i := 0; i < n; i++ {
				tree.insert(row, MapPair{Key: makeKey(i), Value: []byte("v")})
			}
			if _, err := tree.get(row); err != nil { // warm the cache
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := tree.get(row); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("read-after-update/n=%d", n), func(b *testing.B) {
			tree := &binarySearchTreeMap{}
			row := []byte("row")
			for i := 0; i < n; i++ {
				tree.insert(row, MapPair{Key: makeKey(i), Value: []byte("v")})
			}
			if _, err := tree.get(row); err != nil { // warm the cache
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// updates an existing key, so the deduped row stays at size n
				// and per-op cost doesn't drift across the run
				tree.insert(row, MapPair{Key: makeKey(i % n), Value: []byte("v-updated")})
				if _, err := tree.get(row); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
