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

package lsmkv

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestMemtableRoaringSet(t *testing.T) {
	memPath := func() string {
		return path.Join(t.TempDir(), "fake")
	}

	t.Run("inserting individual entries", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddOne(key1, 1))
		assert.Nil(t, m.roaringSetAddOne(key1, 2))
		assert.Nil(t, m.roaringSetAddOne(key2, 3))
		assert.Nil(t, m.roaringSetAddOne(key2, 4))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("inserting lists", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddList(key1, []uint64{1, 2}))
		assert.Nil(t, m.roaringSetAddList(key2, []uint64{3, 4}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("inserting bitmaps", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		bm1 := roaringset.NewBitmap(1, 2)
		assert.Nil(t, m.roaringSetAddBitmap(key1, bm1))
		bm2 := roaringset.NewBitmap(3, 4)
		assert.Nil(t, m.roaringSetAddBitmap(key2, bm2))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing individual entries", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveOne(key1, 7))
		assert.Nil(t, m.roaringSetRemoveOne(key2, 8))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.False(t, setKey1.Additions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(7))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(8))
		assert.True(t, setKey2.Deletions.Contains(8))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing lists", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveList(key1, []uint64{7, 8}))
		assert.Nil(t, m.roaringSetRemoveList(key2, []uint64{9, 10}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey1.Additions.GetCardinality())
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey2.Additions.GetCardinality())
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing bitmaps", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveBitmap(key1, roaringset.NewBitmap(7, 8)))
		assert.Nil(t, m.roaringSetRemoveBitmap(key2, roaringset.NewBitmap(9, 10)))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey1.Additions.GetCardinality())
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey2.Additions.GetCardinality())
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("adding/removing bitmaps", func(t *testing.T) {
		m, err := newMemtable(memPath(), StrategyRoaringSet, 0, nil)
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddRemoveBitmaps(key1,
			roaringset.NewBitmap(1, 2), roaringset.NewBitmap(7, 8)))
		assert.Nil(t, m.roaringSetAddRemoveBitmaps(key2,
			roaringset.NewBitmap(3, 4), roaringset.NewBitmap(9, 10)))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 2, setKey1.Additions.GetCardinality())
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 2, setKey2.Additions.GetCardinality())
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})
}
