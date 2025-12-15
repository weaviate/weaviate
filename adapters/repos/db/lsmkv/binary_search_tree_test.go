//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"crypto/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test asserts that the *binarySearchTree.insert
// method properly calculates the net additions of a
// new node into the tree
func TestInsertNetAdditions_Replace(t *testing.T) {
	t.Run("single node entry", func(t *testing.T) {
		tree := &binarySearchTree{}

		key := make([]byte, 8)
		val := make([]byte, 8)

		rand.Read(key)
		rand.Read(val)

		n, _ := tree.insert(key, val, nil)
		require.Equal(t, len(key)+len(val), n)
	})

	t.Run("multiple unique node entries", func(t *testing.T) {
		tree := &binarySearchTree{}

		amount := 100
		size := 8

		var n int
		for i := 0; i < amount; i++ {
			key := make([]byte, size)
			val := make([]byte, size)

			rand.Read(key)
			rand.Read(val)

			newAdditions, _ := tree.insert(key, val, nil)
			n += newAdditions
		}

		require.Equal(t, amount*size*2, n)
	})

	t.Run("multiple non-unique node entries", func(t *testing.T) {
		tree := &binarySearchTree{}

		var (
			amount      = 100
			keySize     = 100
			origValSize = 100
			newValSize  = origValSize * 100
			keys        = make([][]byte, amount)
			vals        = make([][]byte, amount)

			netAdditions int
		)

		// write the keys and original values
		for i := range keys {
			key := make([]byte, keySize)
			rand.Read(key)

			val := make([]byte, origValSize)
			rand.Read(val)

			keys[i], vals[i] = key, val
		}

		// make initial inserts
		for i := range keys {
			currentNetAddition, _ := tree.insert(keys[i], vals[i], nil)
			netAdditions += currentNetAddition
		}

		// change the values of the existing keys
		// with new values of different length
		for i := 0; i < amount; i++ {
			val := make([]byte, newValSize)
			rand.Read(val)

			vals[i] = val
		}

		for i := 0; i < amount; i++ {
			currentNetAddition, _ := tree.insert(keys[i], vals[i], nil)
			netAdditions += currentNetAddition
		}

		// Formulas for calculating the total net additions after
		// updating the keys with differently sized values
		expectedFirstNetAdd := amount * (keySize + origValSize)
		expectedSecondNetAdd := (amount * (keySize + newValSize)) - (amount * keySize) - (amount * origValSize)
		expectedNetAdditions := expectedFirstNetAdd + expectedSecondNetAdd

		require.Equal(t, expectedNetAdditions, netAdditions)
	})

	// test to assure multiple tombstone nodes are not created when same value is added and deleted multiple times
	// https://semi-technology.atlassian.net/browse/WEAVIATE-31
	t.Run("consecutive adding and deleting value does not multiply nodes", func(t *testing.T) {
		tree := &binarySearchTree{}

		key := []byte(uuid.New().String())
		value := make([]byte, 100)
		rand.Read(value)

		for i := 0; i < 10; i++ {
			tree.insert(key, value, nil)
			tree.setTombstone(key, nil, nil)
		}

		flat := tree.flattenInOrder()

		require.Equal(t, 1, len(flat))
		require.True(t, flat[0].tombstone)
	})
}

func TestBSTReplace_Flatten(t *testing.T) {
	t.Run("flattened bst is snapshot of current bst", func(t *testing.T) {
		key1 := "key-1"
		key2 := "key-2"
		key3 := "key-3"
		key4 := "key-4"

		keys := map[string][]byte{
			key1: []byte(key1),
			key2: []byte(key2),
			key3: []byte(key3),
			key4: []byte(key4),
		}
		vals := map[string][]byte{
			key1: []byte("val-1"),
			key2: []byte("val-2"),
			key3: []byte("val-3"),
		}
		valsUpdated := map[string][]byte{
			key1: []byte("val-11"),
			key2: []byte("val-22"),
			key3: []byte("val-33"),
			key4: []byte("val-44"),
		}
		skeys := map[string][]byte{
			key1: []byte("skey=1"),
			key2: []byte("skey=2"),
			key3: []byte("skey=3"),
		}
		skeysUpdated := map[string][]byte{
			key1: []byte("skey=11"),
			key2: []byte("skey=22"),
			key3: []byte("skey=33"),
			key4: []byte("skey=44"),
		}

		type expectedFlattened struct {
			key       []byte
			val       []byte
			skey      []byte
			tombstone bool
		}
		assertFlattenedMatches := func(t *testing.T, flattened []*binarySearchNode, expected []expectedFlattened) {
			t.Helper()
			require.Len(t, flattened, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp.key, flattened[i].key)
				assert.Equal(t, exp.val, flattened[i].value)
				assert.Equal(t, exp.skey, flattened[i].secondaryKeys[0])
				assert.Equal(t, exp.tombstone, flattened[i].tombstone)
			}
		}

		bst := &binarySearchTree{}
		// mixed order
		bst.insert(keys[key3], vals[key3], [][]byte{skeys[key3]})
		bst.insert(keys[key1], vals[key1], [][]byte{skeys[key1]})
		bst.setTombstone(keys[key2], vals[key2], [][]byte{skeys[key2]})

		expectedBeforeUpdate := []expectedFlattened{
			{keys[key1], vals[key1], skeys[key1], false},
			{keys[key2], vals[key2], skeys[key2], true},
			{keys[key3], vals[key3], skeys[key3], false},
		}

		flatBeforeUpdate := bst.flattenInOrder()
		assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)

		t.Run("flattened bst does not change on bst update", func(t *testing.T) {
			// mixed order
			bst.setTombstone(keys[key3], valsUpdated[key3], [][]byte{skeysUpdated[key3]})
			bst.insert(keys[key4], valsUpdated[key4], [][]byte{skeysUpdated[key4]})
			bst.setTombstone(keys[key1], valsUpdated[key1], [][]byte{skeysUpdated[key1]})
			bst.insert(keys[key2], valsUpdated[key2], [][]byte{skeysUpdated[key2]})

			expectedAfterUpdate := []expectedFlattened{
				{keys[key1], valsUpdated[key1], skeysUpdated[key1], true},
				{keys[key2], valsUpdated[key2], skeysUpdated[key2], false},
				{keys[key3], valsUpdated[key3], skeysUpdated[key3], true},
				{keys[key4], valsUpdated[key4], skeysUpdated[key4], false},
			}

			flatAfterUpdate := bst.flattenInOrder()
			assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)
			assertFlattenedMatches(t, flatAfterUpdate, expectedAfterUpdate)
		})
	})
}
