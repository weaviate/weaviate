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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("the flattened copy keeps the source bitmap's slack", func(t *testing.T) {
		// A range mostly removed again leaves the container sized for what it
		// held, which tells a buffer copy apart from a rebuild through a union.
		// 1000 keeps the container array-backed, and Condense leaves a
		// bitmap-backed one at its full size.
		bst := new(BinarySearchTree)
		bst.Insert([]byte("key"), Insert{Additions: slice(0, 1000)})
		bst.Insert([]byte("key"), Insert{Deletions: slice(10, 1000)})

		flat := bst.FlattenInOrder()
		require.Len(t, flat, 1)

		source := bst.root.Value.Additions.ToBuffer()
		copied := flat[0].Value.Additions.ToBuffer()
		condensed := Condense(bst.root.Value.Additions).ToBuffer()

		require.Greater(t, len(source), len(condensed),
			"the fixture no longer carries slack Condense can reclaim; sroar's array/bitmap container threshold may have moved")
		assert.Equal(t, source, copied, "the copy must be the source buffer, byte for byte")
		assert.Greater(t, len(copied), len(condensed),
			"a copy the size of a condensed bitmap means the values were rebuilt, not copied")
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

func BenchmarkBinarySearchTreeInsert(b *testing.B) {
	count := uint64(100_000)
	keys := make([][]byte, count)

	// generate
	for i := range keys {
		bytes, err := lexicographicallySortableFloat64(float64(i) / 3)
		require.NoError(b, err)
		keys[i] = bytes
	}

	// shuffle
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

	insert := Insert{Additions: make([]uint64, 1)}
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
	keys := make([][]byte, count)

	// generate
	for i := range keys {
		bytes, err := lexicographicallySortableFloat64(float64(i) / 3)
		require.NoError(b, err)
		keys[i] = bytes
	}

	// shuffle
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

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

// BenchmarkBinarySearchTreeFlattenWithSlack flattens bitmaps carrying container
// slack, which BenchmarkBinarySearchTreeFlatten's single-value nodes have none
// of. Copying a buffer keeps that slack, so a cursor opened here holds more
// bytes than a rebuild through a union would leave it.
//
// retained-B is that held size. B/op cannot stand in for it: it also counts
// sroar's buffer slop beyond what ToBuffer reports, which understates the
// difference between the two copies.
func BenchmarkBinarySearchTreeFlattenWithSlack(b *testing.B) {
	const keys = 200

	bst := new(BinarySearchTree)
	for i := 0; i < keys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		bst.Insert(key, Insert{Additions: slice(0, 1000)})
		bst.Insert(key, Insert{Deletions: slice(10, 1000)})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var retained int
	for i := 0; i < b.N; i++ {
		flat := bst.FlattenInOrder()

		retained = 0
		for _, node := range flat {
			retained += node.Value.LenInBytes()
		}
	}

	b.ReportMetric(float64(retained), "retained-B")
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
