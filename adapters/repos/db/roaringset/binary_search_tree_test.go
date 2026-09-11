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

package roaringset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func TestBSTRoaringSet(t *testing.T) {
	t.Run("single key, single set entry", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		bst.Insert(key, Insert{Additions: []uint64{7}})

		res, err := bst.Get(key)
		require.Nil(t, err)

		assert.False(t, res.Additions.Contains(6))
		assert.True(t, res.Additions.Contains(7))
	})

	t.Run("single key, set updated multiple times", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		for i := uint64(7); i < 14; i++ {
			bst.Insert(key, Insert{Additions: []uint64{i}})
		}

		res, err := bst.Get(key)
		require.Nil(t, err)

		assert.False(t, res.Additions.Contains(6))
		for i := uint64(7); i < 14; i++ {
			assert.True(t, res.Additions.Contains(i))
		}
		assert.False(t, res.Additions.Contains(15))
	})

	t.Run("single key, entry added, then deleted", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			bst.Insert(key, Insert{Additions: []uint64{i}})
		}

		bst.Insert(key, Insert{Deletions: []uint64{9}})

		res, err := bst.Get(key)
		require.Nil(t, err)

		// check Additions
		assert.True(t, res.Additions.Contains(7))
		assert.True(t, res.Additions.Contains(8))
		assert.False(t, res.Additions.Contains(9))
		assert.True(t, res.Additions.Contains(10))

		// check Deletions
		assert.True(t, res.Deletions.Contains(9))
	})

	t.Run("single key, entry deleted, then added", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		bst.Insert(key, Insert{Deletions: []uint64{9}})
		bst.Insert(key, Insert{Additions: []uint64{9, 10}})

		res, err := bst.Get(key)
		require.NoError(t, err)

		assert.ElementsMatch(t, []uint64{9, 10}, res.Additions.ToArray())
		assert.Empty(t, res.Deletions.ToArray())
	})

	t.Run("single key, entry added, then deleted, then re-added", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			bst.Insert(key, Insert{Additions: []uint64{i}})
		}

		bst.Insert(key, Insert{Deletions: []uint64{9}})

		bst.Insert(key, Insert{Additions: []uint64{9}})

		res, err := bst.Get(key)
		require.Nil(t, err)

		// check Additions
		assert.True(t, res.Additions.Contains(7))
		assert.True(t, res.Additions.Contains(8))
		assert.True(t, res.Additions.Contains(9))
		assert.True(t, res.Additions.Contains(10))

		// check Deletions
		assert.False(t, res.Deletions.Contains(9))
	})

	t.Run("get is snapshot of underlying bitmaps", func(t *testing.T) {
		bst := &BinarySearchTree{}
		key := []byte("my-key")

		for i := uint64(1); i <= 3; i++ {
			bst.Insert(key, Insert{
				Additions: []uint64{10 + i},
				Deletions: []uint64{10 - i},
			})
		}

		getBeforeUpdate, err := bst.Get(key)
		require.Nil(t, err)

		expectedAdditionsBeforeUpdate := []uint64{11, 12, 13}
		expectedDeletionsBeforeUpdate := []uint64{7, 8, 9}

		assert.ElementsMatch(t, expectedAdditionsBeforeUpdate, getBeforeUpdate.Additions.ToArray())
		assert.ElementsMatch(t, expectedDeletionsBeforeUpdate, getBeforeUpdate.Deletions.ToArray())

		t.Run("gotten layer does not change on bst update", func(t *testing.T) {
			bst.Insert(key, Insert{Additions: []uint64{100}, Deletions: []uint64{1}})

			getAfterUpdate, err := bst.Get(key)
			require.Nil(t, err)

			expectedAdditionsAfterUpdate := []uint64{11, 12, 13, 100}
			expectedDeletionsAfterUpdate := []uint64{1, 7, 8, 9}

			assert.ElementsMatch(t, expectedAdditionsBeforeUpdate, getBeforeUpdate.Additions.ToArray())
			assert.ElementsMatch(t, expectedDeletionsBeforeUpdate, getBeforeUpdate.Deletions.ToArray())

			assert.ElementsMatch(t, expectedAdditionsAfterUpdate, getAfterUpdate.Additions.ToArray())
			assert.ElementsMatch(t, expectedDeletionsAfterUpdate, getAfterUpdate.Deletions.ToArray())
		})
	})
}

func TestBSTRoaringSet_Flatten(t *testing.T) {
	t.Run("flattened bst is snapshot of current bst", func(t *testing.T) {
		key1 := "key-1"
		key2 := "key-2"
		key3 := "key-3"

		bst := &BinarySearchTree{}
		// mixed order
		bst.Insert([]byte(key3), Insert{Additions: []uint64{7, 8, 9}, Deletions: []uint64{77, 88, 99}})
		bst.Insert([]byte(key1), Insert{Additions: []uint64{1, 2, 3}, Deletions: []uint64{11, 22, 33}})
		bst.Insert([]byte(key2), Insert{Additions: []uint64{4, 5, 6}, Deletions: []uint64{44, 55, 66}})

		flatBeforeUpdate := bst.FlattenInOrder()

		expectedBeforeUpdate := []struct {
			key       string
			additions []uint64
			deletions []uint64
		}{
			{key1, []uint64{1, 2, 3}, []uint64{11, 22, 33}},
			{key2, []uint64{4, 5, 6}, []uint64{44, 55, 66}},
			{key3, []uint64{7, 8, 9}, []uint64{77, 88, 99}},
		}

		assert.Len(t, flatBeforeUpdate, len(expectedBeforeUpdate))
		for i, exp := range expectedBeforeUpdate {
			assert.Equal(t, []byte(exp.key), flatBeforeUpdate[i].Key)
			assert.ElementsMatch(t, exp.additions, flatBeforeUpdate[i].Value.Additions.ToArray())
			assert.ElementsMatch(t, exp.deletions, flatBeforeUpdate[i].Value.Deletions.ToArray())
		}

		t.Run("flattened bst does not change on bst update", func(t *testing.T) {
			key4 := "key-4"

			// mixed order
			bst.Insert([]byte(key4), Insert{Additions: []uint64{111, 222, 333}, Deletions: []uint64{444, 555, 666}})
			bst.Insert([]byte(key3), Insert{Additions: []uint64{77, 88}, Deletions: []uint64{7, 8}})
			bst.Insert([]byte(key1), Insert{Additions: []uint64{11, 22}, Deletions: []uint64{1, 2}})

			flatAfterUpdate := bst.FlattenInOrder()

			expectedAfterUpdate := []struct {
				key       string
				additions []uint64
				deletions []uint64
			}{
				{key1, []uint64{3, 11, 22}, []uint64{1, 2, 33}},
				{key2, []uint64{4, 5, 6}, []uint64{44, 55, 66}},
				{key3, []uint64{9, 77, 88}, []uint64{7, 8, 99}},
				{key4, []uint64{111, 222, 333}, []uint64{444, 555, 666}},
			}

			assert.Len(t, flatBeforeUpdate, len(expectedBeforeUpdate))
			for i, exp := range expectedBeforeUpdate {
				assert.Equal(t, []byte(exp.key), flatBeforeUpdate[i].Key)
				assert.ElementsMatch(t, exp.additions, flatBeforeUpdate[i].Value.Additions.ToArray())
				assert.ElementsMatch(t, exp.deletions, flatBeforeUpdate[i].Value.Deletions.ToArray())
			}

			assert.Len(t, flatAfterUpdate, len(expectedAfterUpdate))
			for i, exp := range expectedAfterUpdate {
				assert.Equal(t, []byte(exp.key), flatAfterUpdate[i].Key)
				assert.ElementsMatch(t, exp.additions, flatAfterUpdate[i].Value.Additions.ToArray())
				assert.ElementsMatch(t, exp.deletions, flatAfterUpdate[i].Value.Deletions.ToArray())
			}
		})
	})

	t.Run("the flattened copy sheds the source bitmap's slack", func(t *testing.T) {
		// A range mostly removed again leaves the container sized for what it
		// held, so a copy that keeps the source's size is a buffer copy and one
		// that shrinks reclaimed the slack. 1000 keeps the container
		// array-backed, which is the shape a cursor over a memtable that has
		// seen deletes actually holds.
		bst := new(BinarySearchTree)
		bst.Insert([]byte("key"), Insert{Additions: slice(0, 1000)})
		bst.Insert([]byte("key"), Insert{Deletions: slice(10, 1000)})

		flat := bst.FlattenInOrder()
		require.Len(t, flat, 1)

		source := bst.root.Value.Additions.ToBuffer()
		copied := flat[0].Value.Additions.ToBuffer()

		require.Greater(t, len(source), len(bst.root.Value.Additions.Compacted().ToBuffer()),
			"the fixture no longer carries slack to reclaim; sroar's array/bitmap container threshold may have moved")
		assert.Less(t, len(copied), len(source),
			"a copy the size of the source means the buffer was copied, slack and all")
		assert.ElementsMatch(t, slice(0, 10), flat[0].Value.Additions.ToArray(),
			"shedding slack must not shed values")
	})

	t.Run("the flattened copy keeps a nil side nil", func(t *testing.T) {
		// sroar's Compacted returns an allocated empty bitmap for a nil
		// receiver, so the per-side guard is what stops a nil side coming back
		// as one holding nothing.
		bst := new(BinarySearchTree)
		bst.Insert([]byte("key"), Insert{Additions: slice(0, 4)})
		bst.root.Value.Deletions = nil

		flat := bst.FlattenInOrder()
		require.Len(t, flat, 1)
		assert.Nil(t, flat[0].Value.Deletions)
	})
}

func TestBinarySearchTreeCountsDistinctKeys(t *testing.T) {
	ascendingKeys := func(n int) [][]byte {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("key-%05d", i))
		}
		return keys
	}

	tests := []struct {
		name    string
		inserts [][]byte
		values  Insert
		want    int
	}{
		{name: "no inserts", values: Insert{Additions: []uint64{1}}, want: 0},
		{
			name:    "one key",
			inserts: [][]byte{[]byte("a")},
			values:  Insert{Additions: []uint64{1}},
			want:    1,
		},
		{
			name:    "the same key twice merges",
			inserts: [][]byte{[]byte("a"), []byte("a")},
			values:  Insert{Additions: []uint64{1}},
			want:    1,
		},
		{
			name:    "the same key twice, not adjacent",
			inserts: [][]byte{[]byte("b"), []byte("a"), []byte("b")},
			values:  Insert{Additions: []uint64{1}},
			want:    2,
		},
		{
			// Ascending keys rebalance, so the root moves and insert returns a new
			// one on rows a nil return would have counted as a merge.
			name:    "ascending keys rebalance and still count once each",
			inserts: ascendingKeys(64),
			values:  Insert{Additions: []uint64{1}},
			want:    64,
		},
		{
			// The duplicate lands below the root, where insert relays the subtree's
			// answer rather than deciding it.
			name:    "a key re-inserted below the root merges",
			inserts: [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("c")},
			values:  Insert{Additions: []uint64{1}},
			want:    3,
		},
		{
			name:    "a zero-length key is a key",
			inserts: [][]byte{{}, []byte("a")},
			values:  Insert{Additions: []uint64{1}},
			want:    2,
		},
		{
			name:    "a key carrying only deletions is a key",
			inserts: [][]byte{[]byte("deleted")},
			values:  Insert{Deletions: []uint64{7}},
			want:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := new(BinarySearchTree)
			for _, key := range tt.inserts {
				tree.Insert(key, tt.values)
			}

			require.Equal(t, tt.want, tree.Count())
			require.Len(t, tree.FlattenInOrder(), tt.want,
				"Count must match the nodes a walk of the tree yields")
		})
	}
}

// TestBSTRoaringSetSizeInBytes pins the size model against the shapes a
// per-entry estimate reads backwards.
func TestBSTRoaringSetSizeInBytes(t *testing.T) {
	emptyBitmapBytes := NewBitmap().LenInBytes()

	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	docIDs := func(count int, stride uint64) []uint64 {
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = uint64(i) * stride
		}
		return ids
	}
	// one doc ID per container, so each needs its own allocation
	const sparseStride = 1 << 16

	shapes := []struct {
		name  string
		keys  int
		build func(bst *BinarySearchTree)
	}{
		{"1000 keys of one doc ID each", 1000, func(bst *BinarySearchTree) {
			for i := 0; i < 1000; i++ {
				bst.Insert(key(i), Insert{Additions: []uint64{uint64(i)}})
			}
		}},
		{"one key of 65536 dense doc IDs", 1, func(bst *BinarySearchTree) {
			bst.Insert(key(0), Insert{Additions: docIDs(65536, 1)})
		}},
		{"one key of 1000 dense doc IDs", 1, func(bst *BinarySearchTree) {
			bst.Insert(key(0), Insert{Additions: docIDs(1000, 1)})
		}},
		{"one key of 1000 sparse doc IDs", 1, func(bst *BinarySearchTree) {
			bst.Insert(key(0), Insert{Additions: docIDs(1000, sparseStride)})
		}},
		{"one key of 1000 sparse doc IDs written 100 times", 1, func(bst *BinarySearchTree) {
			for i := 0; i < 100; i++ {
				bst.Insert(key(0), Insert{Additions: docIDs(1000, sparseStride)})
			}
		}},
	}

	// filled outside the subtests, so -run on one comparison still finds them
	sizes := map[string]uint64{}
	for _, shape := range shapes {
		bst := &BinarySearchTree{}
		shape.build(bst)
		sizes[shape.name] = bst.SizeInBytes()

		t.Run(shape.name, func(t *testing.T) {
			require.Len(t, bst.FlattenInOrder(), shape.keys)
			require.Greater(t, bst.SizeInBytes(),
				uint64(shape.keys*(bitmapFixedSizeInBytes+emptyBitmapBytes+len(key(0)))),
				"a key costs its node on top of its own bytes and its bitmap")
		})
	}

	t.Run("a thousand single-doc-ID keys outweigh one key of 65536 doc IDs", func(t *testing.T) {
		require.Greater(t, sizes["1000 keys of one doc ID each"],
			sizes["one key of 65536 dense doc IDs"],
			"per-key structure dominates, though this shape carries 65x fewer doc IDs")
	})

	t.Run("sparse doc IDs outweigh the same count packed densely", func(t *testing.T) {
		require.Greater(t, sizes["one key of 1000 sparse doc IDs"],
			sizes["one key of 1000 dense doc IDs"])
	})

	t.Run("a side the write does not fill is not charged", func(t *testing.T) {
		additionsOnly := &BinarySearchTree{}
		additionsOnly.Insert(key(0), Insert{Additions: []uint64{1}})

		bothSides := &BinarySearchTree{}
		bothSides.Insert(key(0), Insert{Additions: []uint64{1}, Deletions: []uint64{2}})

		require.Equal(t, additionsOnly.SizeInBytes()+
			uint64(bitmapFixedSizeInBytes+NewBitmap(2).LenInBytes()),
			bothSides.SizeInBytes(),
			"a deletions side costs its own struct as well as its buffer")
		// spelled out, so dropping either of the node's terms fails here too
		require.Equal(t, uint64(nodeFixedSizeInBytes+len(key(0))+
			bitmapFixedSizeInBytes+NewBitmap(1).LenInBytes()),
			additionsOnly.SizeInBytes())
	})

	t.Run("filling an absent side later charges its struct, not only its buffer", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert(key(0), Insert{Additions: []uint64{1}})
		before := bst.SizeInBytes()

		bst.Insert(key(0), Insert{Deletions: []uint64{2}})

		require.Equal(t, before+uint64(bitmapFixedSizeInBytes+NewBitmap(2).LenInBytes()),
			bst.SizeInBytes(),
			"the merge delta has to cover the struct a side allocated on first use adds")

		bothSides := &BinarySearchTree{}
		bothSides.Insert(key(0), Insert{Additions: []uint64{1}, Deletions: []uint64{2}})
		require.Equal(t, bothSides.SizeInBytes(), bst.SizeInBytes(),
			"a side allocated on first use costs what one allocated with the node costs")
	})

	t.Run("a longer key costs the bytes it adds", func(t *testing.T) {
		longKey := make([]byte, 512)
		copy(longKey, key(0))

		short := &BinarySearchTree{}
		short.Insert(key(0), Insert{Additions: docIDs(1000, 1)})

		long := &BinarySearchTree{}
		long.Insert(longKey, Insert{Additions: docIDs(1000, 1)})

		// a >= b + delta, since the unsigned subtraction form would wrap and pass
		require.GreaterOrEqual(t, long.SizeInBytes(),
			short.SizeInBytes()+uint64(len(longKey)-len(key(0))),
			"the key's own bytes are part of what the node holds")
	})

	t.Run("adding doc IDs under a key already in the tree grows the total", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert(key(0), Insert{Additions: docIDs(1000, sparseStride)})
		afterFirstWrite := bst.SizeInBytes()

		bst.Insert(key(0), Insert{Additions: docIDs(2000, sparseStride)})

		require.Greater(t, bst.SizeInBytes(), afterFirstWrite,
			"a thousand further containers allocate, and the merge has to charge them")
	})

	t.Run("deleting doc IDs under a key already in the tree grows the total", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert(key(0), Insert{Additions: []uint64{1, 2}, Deletions: []uint64{7, 8}})
		afterFirstWrite := bst.SizeInBytes()

		bst.Insert(key(0), Insert{Deletions: docIDs(1000, sparseStride)})

		require.Greater(t, bst.SizeInBytes(), afterFirstWrite,
			"the deletions side grows on a delete, and it has to be charged too")
	})

	t.Run("rewriting doc IDs already present does not grow the total", func(t *testing.T) {
		written := sizes["one key of 1000 sparse doc IDs written 100 times"]
		require.NotZero(t, written, "the shape name must match the table")
		require.Equal(t, sizes["one key of 1000 sparse doc IDs"], written)
	})
}

// TestBSTRoaringSetHeapRatio scores SizeInBytes against a measured heap delta,
// so the bound its godoc states can be re-derived rather than trusted. It runs
// the shape production writes take, one doc ID per Insert, because that is what
// leaves sroar's buffer half empty.
func TestBSTRoaringSetHeapRatio(t *testing.T) {
	heapAlloc := func() uint64 {
		var ms runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}

	shapes := []struct {
		name string
		keys int
		// perKey doc IDs are inserted one at a time, each a stride apart
		perKey   int
		stride   uint64
		minRatio float64
		maxRatio float64
	}{
		// One write per key leaves no room to grow into, so there is no slack to
		// hide in and the model is scored against heap almost directly. This is
		// the row that pins nodeFixedSizeInBytes: every other assertion in this
		// package puts that constant on both sides of the comparison.
		{"1000 keys of one doc ID each", 1000, 1, 1, 1.0, 1.1},
		// doubling bounds the slack at 2x, so none of these may reach it
		{"one key grown to 200k sequential doc IDs", 1, 200_000, 1, 1.2, 2.0},
		{"one key grown to 200k doc IDs at stride 1024", 1, 200_000, 1024, 1.2, 2.0},
		{"one key grown to 200k doc IDs at stride 8", 1, 200_000, 8, 1.7, 2.0},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			before := heapAlloc()

			bst := &BinarySearchTree{}
			buf := make([]uint64, 1)
			for k := 0; k < shape.keys; k++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(k))
				for d := 0; d < shape.perKey; d++ {
					buf[0] = uint64(d) * shape.stride
					bst.Insert(key, Insert{Additions: buf})
				}
			}

			held := heapAlloc() - before
			runtime.KeepAlive(bst)

			model := bst.SizeInBytes()
			ratio := float64(held) / float64(model)
			t.Logf("held %d, model %d, held/model %.3f", held, model, ratio)

			require.LessOrEqual(t, model, held,
				"every allocation rounds up to a size class, so the model cannot exceed the heap")
			require.GreaterOrEqual(t, ratio, shape.minRatio,
				"the len-to-cap slack this shape leaves is what the model does not count")
			require.LessOrEqual(t, ratio, shape.maxRatio,
				"a term missing from the model shows up here as heap it cannot account for")
		})
	}
}

// TestBSTRoaringSetSizeAcrossInsertArms drives the two arms of insert that an
// ascending fixture never reaches: it descends right every time, and it merges
// only into a single-node tree. Each subtest asserts it landed on its arm before
// checking what the arm computed.
func TestBSTRoaringSetSizeAcrossInsertArms(t *testing.T) {
	// Walks the tree's own nodes rather than FlattenInOrder's copies, which are
	// compacted and so hold fewer bytes than the nodes the total is charged for.
	sumOverNodes := func(t *testing.T, bst *BinarySearchTree) uint64 {
		var total, visited int
		stack := []*BinarySearchNode{bst.root}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if n == nil {
				continue
			}
			visited++
			require.LessOrEqual(t, visited, bst.Count(),
				"the walk reached more nodes than the tree counts, so its links form a cycle")
			total += n.sizeInBytes()
			stack = append(stack, n.left, n.right)
		}
		return uint64(total)
	}
	sparse := func(count int) []uint64 {
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = uint64(i) << 16
		}
		return ids
	}

	t.Run("creating a left child charges what the node holds", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert([]byte("b"), Insert{Additions: []uint64{1}})
		bst.Insert([]byte("a"), Insert{Additions: []uint64{2}})

		require.NotNil(t, bst.root.left, "the second key must arrive as a left child")
		require.Equal(t, []byte("a"), bst.root.left.Key)
		require.Equal(t, sumOverNodes(t, bst), bst.SizeInBytes())
	})

	t.Run("merging below the root grows the total", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert([]byte("b"), Insert{Additions: []uint64{1}})
		bst.Insert([]byte("a"), Insert{Additions: []uint64{2}})
		bst.Insert([]byte("c"), Insert{Additions: []uint64{3}})
		require.Equal(t, []byte("b"), bst.root.Key, "the merge target must sit below the root")
		require.Equal(t, []byte("a"), bst.root.left.Key)
		before := bst.SizeInBytes()

		bst.Insert([]byte("a"), Insert{Additions: sparse(1000)})

		require.Len(t, bst.FlattenInOrder(), 3, "a merge must not add a node")
		require.Greater(t, bst.SizeInBytes(), before)
		require.Equal(t, sumOverNodes(t, bst), bst.SizeInBytes())
	})

	t.Run("deleting does not drift the total from what the nodes hold", func(t *testing.T) {
		bst := &BinarySearchTree{}
		ids := sparse(1024)
		bst.Insert([]byte("k"), Insert{Additions: ids})
		bst.Insert([]byte("k"), Insert{Deletions: ids[1:]})
		bst.Insert([]byte("k"), Insert{Deletions: ids[:1]})
		bst.Insert([]byte("k"), Insert{Additions: ids[:1]})

		// Monotonicity holds by construction and cannot catch a merge that shrank a
		// bitmap, leaving the running total above what the nodes account for; this
		// equality can.
		require.Equal(t, sumOverNodes(t, bst), bst.SizeInBytes())
	})
}

// TestBSTRoaringSetEmptyInsert pins that a write with nothing in it leaves no
// node behind: such a node is charged its own structure and reaches a segment.
func TestBSTRoaringSetEmptyInsert(t *testing.T) {
	key := []byte("my-key")

	t.Run("a write with no values creates no node", func(t *testing.T) {
		for _, values := range []Insert{
			{},
			{Additions: []uint64{}, Deletions: []uint64{}},
		} {
			bst := &BinarySearchTree{}
			bst.Insert(key, values)

			assert.Empty(t, bst.FlattenInOrder())
			assert.Zero(t, bst.SizeInBytes())

			_, err := bst.Get(key)
			assert.ErrorIs(t, err, lsmkv.NotFound)
		}
	})

	t.Run("a write with no values leaves an existing key untouched", func(t *testing.T) {
		bst := &BinarySearchTree{}
		bst.Insert(key, Insert{Additions: []uint64{7}})
		sizeBefore := bst.SizeInBytes()

		bst.Insert(key, Insert{})

		assert.Equal(t, sizeBefore, bst.SizeInBytes())
		res, err := bst.Get(key)
		require.NoError(t, err)
		assert.ElementsMatch(t, []uint64{7}, res.Additions.ToArray())
	})
}

// TestBSTRoaringSetNodeSides pins which sides a node holds. RoaringSetBatchReader.Next
// takes both being nil to mean a memtable holds nothing for the key, but reads a layer
// CloneIfWithin has normalised, never a node's own.
func TestBSTRoaringSetNodeSides(t *testing.T) {
	key := []byte("my-key")

	tests := []struct {
		name          string
		values        Insert
		wantAdditions []uint64
		wantDeletions []uint64
	}{
		{"additions only", Insert{Additions: []uint64{7}}, []uint64{7}, nil},
		{"deletions only", Insert{Deletions: []uint64{7}}, nil, []uint64{7}},
		{"both sides", Insert{Additions: []uint64{7}, Deletions: []uint64{8}}, []uint64{7}, []uint64{8}},
		{"empty slice on the other side", Insert{Additions: []uint64{7}, Deletions: []uint64{}}, []uint64{7}, nil},
		{"same value on both sides", Insert{Additions: []uint64{7}, Deletions: []uint64{7}}, []uint64{}, []uint64{7}},
	}

	assertSides := func(t *testing.T, node *BinarySearchNode, wantAdditions, wantDeletions []uint64) {
		t.Helper()

		if wantAdditions == nil {
			assert.Nil(t, node.Value.Additions)
		} else if assert.NotNil(t, node.Value.Additions) {
			assert.ElementsMatch(t, wantAdditions, node.Value.Additions.ToArray())
		}
		if wantDeletions == nil {
			assert.Nil(t, node.Value.Deletions)
		} else if assert.NotNil(t, node.Value.Deletions) {
			assert.ElementsMatch(t, wantDeletions, node.Value.Deletions.ToArray())
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bst := &BinarySearchTree{}
			bst.Insert(key, test.values)

			assertSides(t, bst.root, test.wantAdditions, test.wantDeletions)
		})

		t.Run(test.name+", then a write to both sides", func(t *testing.T) {
			bst := &BinarySearchTree{}
			bst.Insert(key, test.values)
			bst.Insert(key, Insert{Additions: []uint64{9}, Deletions: []uint64{10}})

			assertSides(t, bst.root,
				append([]uint64{9}, test.wantAdditions...),
				append([]uint64{10}, test.wantDeletions...))
		})
	}
}

// TestBSTRoaringSetSidesStayDisjoint pins the mutual exclusion BitmapLayer
// documents against both writers of it: newBinarySearchNode builds a node for a
// key the tree has not seen, insert merges into one it has, and which runs is
// decided by the key rather than by the rule.
func TestBSTRoaringSetSidesStayDisjoint(t *testing.T) {
	key := []byte("my-key")

	tests := []struct {
		name   string
		values Insert
	}{
		{"one value on both sides", Insert{Additions: []uint64{7}, Deletions: []uint64{7}}},
		{"overlapping ranges", Insert{Additions: []uint64{1, 2, 3}, Deletions: []uint64{2, 3, 4}}},
		{"disjoint sides", Insert{Additions: []uint64{7}, Deletions: []uint64{8}}},
		{"deletion of a seeded value", Insert{Deletions: []uint64{9}}},
		{"across containers", Insert{Additions: []uint64{5, 70000}, Deletions: []uint64{70000}}},
	}

	assertDisjoint := func(t *testing.T, layer BitmapLayer, path string) {
		t.Helper()

		if layer.Additions == nil || layer.Deletions == nil {
			return
		}
		deleted := make(map[uint64]struct{}, layer.Deletions.GetCardinality())
		for _, x := range layer.Deletions.ToArray() {
			deleted[x] = struct{}{}
		}
		for _, x := range layer.Additions.ToArray() {
			_, both := deleted[x]
			require.False(t, both,
				"%s left %d on both sides, so a fold over an earlier segment keeps it", path, x)
		}
	}

	for _, test := range tests {
		t.Run(test.name+", key the tree has not seen", func(t *testing.T) {
			bst := &BinarySearchTree{}
			bst.Insert(key, test.values)

			layer, err := bst.Get(key)
			require.NoError(t, err)
			assertDisjoint(t, layer, "newBinarySearchNode")
		})

		t.Run(test.name+", key the tree already holds", func(t *testing.T) {
			bst := &BinarySearchTree{}
			bst.Insert(key, Insert{Additions: []uint64{9, 70000}})
			bst.Insert(key, test.values)

			layer, err := bst.Get(key)
			require.NoError(t, err)
			assertDisjoint(t, layer, "insert")
		})
	}
}

// TestBSTRoaringSetSizeMatchesNodeWalk pins the running total Insert accumulates
// from insert's deltas against a recomputation over the tree's own nodes. Both
// sides read sizeInBytes, so it pins the accumulation and never the size model.
func TestBSTRoaringSetSizeMatchesNodeWalk(t *testing.T) {
	tests := []struct {
		name  string
		write func(bst *BinarySearchTree, key []byte, i uint64)
	}{
		{"additions only", func(bst *BinarySearchTree, key []byte, i uint64) {
			bst.Insert(key, Insert{Additions: []uint64{i % 97}})
		}},
		{"deletions only", func(bst *BinarySearchTree, key []byte, i uint64) {
			bst.Insert(key, Insert{Deletions: []uint64{i % 97}})
		}},
		{"both sides", func(bst *BinarySearchTree, key []byte, i uint64) {
			bst.Insert(key, Insert{Additions: []uint64{i % 97}, Deletions: []uint64{i%97 + 500}})
		}},
		{"second side filled by a later write", func(bst *BinarySearchTree, key []byte, i uint64) {
			if i%2 == 0 {
				bst.Insert(key, Insert{Additions: []uint64{i % 97}})
			} else {
				bst.Insert(key, Insert{Deletions: []uint64{i%97 + 500}})
			}
		}},
		{"added then removed", func(bst *BinarySearchTree, key []byte, i uint64) {
			bst.Insert(key, Insert{Additions: []uint64{i % 13}})
			bst.Insert(key, Insert{Deletions: []uint64{i % 13}})
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bst := &BinarySearchTree{}
			for i := uint64(0); i < 2000; i++ {
				test.write(bst, []byte(fmt.Sprintf("key-%03d", i%125)), i)
			}

			// The tree's own nodes, not FlattenInOrder's shallow copies: Clone
			// drops an emptied bitmap's buffer, so a copy can charge less than
			// the node it came from.
			walk, seen := 0, 0
			var visit func(n *BinarySearchNode)
			visit = func(n *BinarySearchNode) {
				if n == nil {
					return
				}
				walk += n.sizeInBytes()
				seen++
				visit(n.left)
				visit(n.right)
			}
			visit(bst.root)

			require.NotZero(t, seen, "the walk must reach nodes for the totals to mean anything")
			require.Equal(t, walk, int(bst.SizeInBytes()),
				"the running total has to equal the sum over nodes")
		})
	}
}

// shuffledBenchmarkKeys fixes the shuffle, so two runs differ by the change
// under measurement and not by the rotations the insert order forced.
func shuffledBenchmarkKeys(b *testing.B, count uint64) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		key, err := lexicographicallySortableFloat64(float64(i) / 3)
		require.NoError(b, err)
		keys[i] = key
	}

	r := rand.New(rand.NewSource(1))
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return keys
}

func BenchmarkBinarySearchTreeInsert(b *testing.B) {
	count := uint64(100_000)
	keys := shuffledBenchmarkKeys(b, count)

	insert := Insert{Additions: make([]uint64, 1)}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m := &BinarySearchTree{}
		for value := uint64(0); value < count; value++ {
			insert.Additions[0] = value
			m.Insert(keys[value], insert)
		}
	}
}

func BenchmarkBinarySearchTreeFlatten(b *testing.B) {
	count := uint64(100_000)
	keys := shuffledBenchmarkKeys(b, count)

	// insert
	insert := Insert{Additions: make([]uint64, 1)}
	m := &BinarySearchTree{}
	for value := uint64(0); value < count; value++ {
		insert.Additions[0] = value
		m.Insert(keys[value], insert)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.FlattenInOrder()
	}
}

// BenchmarkBinarySearchTreeCopyWithSlack sweeps the per-node copy shallowCopy
// could use, over bitmaps carrying container slack that
// BenchmarkBinarySearchTreeFlatten's single-value nodes have none of: a buffer copy keeps the slack,
// a union rebuilds the values, and a compacted copy sizes each container to what
// it holds. Reading the three together is what says which one a cursor should
// pay for.
//
// retained-B is the held size. B/op cannot stand in for it: it also counts
// sroar's buffer slop beyond what ToBuffer reports, which understates the
// difference between the copies.
func BenchmarkBinarySearchTreeCopyWithSlack(b *testing.B) {
	const keys = 200

	bst := new(BinarySearchTree)
	for i := 0; i < keys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		bst.Insert(key, Insert{Additions: slice(0, 1000)})
		bst.Insert(key, Insert{Deletions: slice(10, 1000)})
	}

	copies := []struct {
		name string
		copy func(BitmapLayer) BitmapLayer
	}{
		{"compacted", func(l BitmapLayer) BitmapLayer { return l.Compacted() }},
		{"clone", func(l BitmapLayer) BitmapLayer { return l.Clone() }},
		{"condense", func(l BitmapLayer) BitmapLayer {
			return BitmapLayer{Additions: Condense(l.Additions), Deletions: Condense(l.Deletions)}
		}},
	}

	// The tree's own nodes, not FlattenInOrder's output: that output is already a
	// copy, so copying it again would measure every candidate against the one
	// shallowCopy chose rather than against the source.
	var walk func(*BinarySearchNode, func(BitmapLayer) BitmapLayer) int
	walk = func(n *BinarySearchNode, candidate func(BitmapLayer) BitmapLayer) int {
		if n == nil {
			return 0
		}
		copied := candidate(n.Value)
		return walk(n.left, candidate) + copied.LenInBytes() + walk(n.right, candidate)
	}

	for _, cp := range copies {
		b.Run(cp.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var retained int
			for i := 0; i < b.N; i++ {
				retained = walk(bst.root, cp.copy)
			}

			b.ReportMetric(float64(retained), "retained-B")
		})
	}
}

func lexicographicallySortableFloat64(in float64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.BigEndian, in)
	if err != nil {
		return nil, errors.Wrap(err, "serialize float64 value as big endian")
	}

	var out []byte
	if in >= 0 {
		// on positive numbers only flip the sign
		out = buf.Bytes()
		firstByte := out[0] ^ 0x80
		out = append([]byte{firstByte}, out[1:]...)
	} else {
		// on negative numbers flip every bit
		out = make([]byte, 8)
		for i, b := range buf.Bytes() {
			out[i] = b ^ 0xFF
		}
	}

	return out, nil
}
