package lsmkv

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This test asserts that the *binarySearchTree.insert
// method properly calculates the net additions of a
// new node into the tree
func TestInsertNetAdditions_Replace(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	t.Run("single node entry", func(t *testing.T) {
		tree := &binarySearchTree{}

		key := make([]byte, 8)
		val := make([]byte, 8)

		rand.Read(key)
		rand.Read(val)

		n := tree.insert(key, val, nil)
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

			n += tree.insert(key, val, nil)
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
			netAdditions += tree.insert(keys[i], vals[i], nil)
		}

		// change the values of the existing keys
		// with new values of different length
		for i := 0; i < amount; i++ {
			val := make([]byte, newValSize)
			rand.Read(val)

			vals[i] = val
		}

		for i := 0; i < amount; i++ {
			netAdditions += tree.insert(keys[i], vals[i], nil)
		}

		// Formulas for calculating the total net additions after
		// updating the keys with differently sized values
		expectedFirstNetAdd := amount * (keySize + origValSize)
		expectedSecondNetAdd := (amount * (keySize + newValSize)) - (amount * keySize) - (amount * origValSize)
		expectedNetAdditions := expectedFirstNetAdd + expectedSecondNetAdd

		require.Equal(t, expectedNetAdditions, netAdditions)
	})
}
