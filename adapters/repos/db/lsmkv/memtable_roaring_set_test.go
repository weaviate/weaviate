package lsmkv

import (
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtableRoaringSet(t *testing.T) {
	t.Run("inserting individual entries", func(t *testing.T) {
		m, err := newMemtable("fake", StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddOne(key1, 1))
		assert.Nil(t, m.roaringSetAddOne(key1, 2))
		assert.Nil(t, m.roaringSetAddOne(key2, 3))
		assert.Nil(t, m.roaringSetAddOne(key2, 4))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.additions.Contains(1))
		assert.True(t, setKey1.additions.Contains(2))
		assert.False(t, setKey1.additions.Contains(3))
		assert.False(t, setKey1.additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.additions.Contains(1))
		assert.False(t, setKey2.additions.Contains(2))
		assert.True(t, setKey2.additions.Contains(3))
		assert.True(t, setKey2.additions.Contains(4))
	})

	t.Run("inserting lists", func(t *testing.T) {
		m, err := newMemtable("fake", StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddList(key1, []uint64{1, 2}))
		assert.Nil(t, m.roaringSetAddList(key2, []uint64{3, 4}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.additions.Contains(1))
		assert.True(t, setKey1.additions.Contains(2))
		assert.False(t, setKey1.additions.Contains(3))
		assert.False(t, setKey1.additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.additions.Contains(1))
		assert.False(t, setKey2.additions.Contains(2))
		assert.True(t, setKey2.additions.Contains(3))
		assert.True(t, setKey2.additions.Contains(4))
	})

	t.Run("inserting bitmaps", func(t *testing.T) {
		m, err := newMemtable("fake", StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		bm1 := sroar.NewBitmap()
		bm1.SetMany([]uint64{1, 2})
		assert.Nil(t, m.roaringSetAddBitmap(key1, bm1))
		bm2 := sroar.NewBitmap()
		bm2.SetMany([]uint64{3, 4})
		assert.Nil(t, m.roaringSetAddBitmap(key2, bm2))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.additions.Contains(1))
		assert.True(t, setKey1.additions.Contains(2))
		assert.False(t, setKey1.additions.Contains(3))
		assert.False(t, setKey1.additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.additions.Contains(1))
		assert.False(t, setKey2.additions.Contains(2))
		assert.True(t, setKey2.additions.Contains(3))
		assert.True(t, setKey2.additions.Contains(4))
	})

	t.Run("deleting entries", func(t *testing.T) {
		m, err := newMemtable("fake", StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveOne(key1, 7))
		assert.Nil(t, m.roaringSetRemoveOne(key2, 8))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.False(t, setKey1.additions.Contains(7))
		assert.True(t, setKey1.deletions.Contains(7))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.additions.Contains(8))
		assert.True(t, setKey2.deletions.Contains(8))
	})
}
