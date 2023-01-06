package lsmkv

import (
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBSTRoaringSet(t *testing.T) {
	t.Run("single key, single set entry", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		set := roaringSet{
			additions: sroar.NewBitmap(),
			deletions: sroar.NewBitmap(),
		}

		set.additions.Set(7)

		bst.insert(key, set)

		res, err := bst.get(key)
		require.Nil(t, err)

		assert.False(t, res.additions.Contains(6))
		assert.True(t, res.additions.Contains(7))
	})

	t.Run("single key, set updated multiple times", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 14; i++ {
			set := roaringSet{
				additions: sroar.NewBitmap(),
				deletions: sroar.NewBitmap(),
			}

			set.additions.Set(i)
			bst.insert(key, set)
		}

		res, err := bst.get(key)
		require.Nil(t, err)

		assert.False(t, res.additions.Contains(6))
		for i := uint64(7); i < 14; i++ {
			assert.True(t, res.additions.Contains(i))
		}
		assert.False(t, res.additions.Contains(15))
	})

	t.Run("single key, entry added, then deleted", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			set := roaringSet{
				additions: sroar.NewBitmap(),
				deletions: sroar.NewBitmap(),
			}

			set.additions.Set(i)
			bst.insert(key, set)
		}

		set := roaringSet{
			additions: sroar.NewBitmap(),
			deletions: sroar.NewBitmap(),
		}
		set.deletions.Set(9)
		bst.insert(key, set)

		res, err := bst.get(key)
		require.Nil(t, err)

		// check additions
		assert.True(t, res.additions.Contains(7))
		assert.True(t, res.additions.Contains(8))
		assert.False(t, res.additions.Contains(9))
		assert.True(t, res.additions.Contains(10))

		// check deletions
		assert.True(t, res.deletions.Contains(9))
	})

	t.Run("single key, entry added, then deleted, then re-added", func(t *testing.T) {
		bst := &binarySearchTreeRoaringSet{}
		key := []byte("my-key")

		for i := uint64(7); i < 11; i++ {
			set := roaringSet{
				additions: sroar.NewBitmap(),
				deletions: sroar.NewBitmap(),
			}

			set.additions.Set(i)
			bst.insert(key, set)
		}

		set := roaringSet{
			additions: sroar.NewBitmap(),
			deletions: sroar.NewBitmap(),
		}
		set.deletions.Set(9)
		bst.insert(key, set)

		set2 := roaringSet{
			additions: sroar.NewBitmap(),
			deletions: sroar.NewBitmap(),
		}
		set2.additions.Set(9)
		bst.insert(key, set2)

		res, err := bst.get(key)
		require.Nil(t, err)

		// check additions
		assert.True(t, res.additions.Contains(7))
		assert.True(t, res.additions.Contains(8))
		assert.True(t, res.additions.Contains(9))
		assert.True(t, res.additions.Contains(10))

		// check deletions
		assert.False(t, res.deletions.Contains(9))
	})
}
