//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringset

import (
	"bytes"
	"encoding/binary"
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

	for i := 0; i < b.N; i++ {
		m.FlattenInOrder()
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
