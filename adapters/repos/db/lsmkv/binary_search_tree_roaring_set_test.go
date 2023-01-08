package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBSTRoaringSet(t *testing.T) {
	t.Run("single key, single set entry", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		bst.insert(key, roaringSetInsert{additions: []uint64{7}})

		res, err := bst.get(key)
		require.Nil(t, err)

		assert.False(t, res.Additions.Contains(6))
		assert.True(t, res.Additions.Contains(7))
	})

	t.Run("single key, set updated multiple times", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 14; i++ {
			bst.insert(key, roaringSetInsert{additions: []uint64{i}})
		}

		res, err := bst.get(key)
		require.Nil(t, err)

		assert.False(t, res.Additions.Contains(6))
		for i := uint64(7); i < 14; i++ {
			assert.True(t, res.Additions.Contains(i))
		}
		assert.False(t, res.Additions.Contains(15))
	})

	t.Run("single key, entry added, then deleted", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			bst.insert(key, roaringSetInsert{additions: []uint64{i}})
		}

		bst.insert(key, roaringSetInsert{deletions: []uint64{9}})

		res, err := bst.get(key)
		require.Nil(t, err)

		// check additions
		assert.True(t, res.Additions.Contains(7))
		assert.True(t, res.Additions.Contains(8))
		assert.False(t, res.Additions.Contains(9))
		assert.True(t, res.Additions.Contains(10))

		// check deletions
		assert.True(t, res.Deletions.Contains(9))
	})

	t.Run("single key, entry added, then deleted, then re-added", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			bst.insert(key, roaringSetInsert{additions: []uint64{i}})
		}

		bst.insert(key, roaringSetInsert{deletions: []uint64{9}})

		bst.insert(key, roaringSetInsert{additions: []uint64{9}})

		res, err := bst.get(key)
		require.Nil(t, err)

		// check additions
		assert.True(t, res.Additions.Contains(7))
		assert.True(t, res.Additions.Contains(8))
		assert.True(t, res.Additions.Contains(9))
		assert.True(t, res.Additions.Contains(10))

		// check deletions
		assert.False(t, res.Deletions.Contains(9))
	})
}
