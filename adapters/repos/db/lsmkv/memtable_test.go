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
	"errors"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// This test prevents a regression on
// https://www.youtube.com/watch?v=OS8taasZl8k
func Test_MemtableSecondaryKeyBug(t *testing.T) {
	dir := t.TempDir()

	logger, _ := test.NewNullLogger()
	cl, err := newCommitLogger(dir, StrategyReplace, 0)
	require.NoError(t, err)

	m, err := newMemtable(path.Join(dir, "will-never-flush"), StrategyReplace, 1, cl, nil, logger, false, nil, false, nil)
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, m.commitlog.close())
	})

	t.Run("add initial value", func(t *testing.T) {
		err = m.put([]byte("my-key"), []byte("my-value"),
			WithSecondaryKey(0, []byte("secondary-key-initial")))
		require.Nil(t, err)
	})

	t.Run("retrieve by primary", func(t *testing.T) {
		val, err := m.get([]byte("my-key"))
		require.Nil(t, err)
		assert.Equal(t, []byte("my-value"), val)
	})

	t.Run("retrieve by initial secondary", func(t *testing.T) {
		val, err := m.getBySecondary(0, []byte("secondary-key-initial"))
		require.Nil(t, err)
		assert.Equal(t, []byte("my-value"), val)
	})

	t.Run("update value with different secondary key", func(t *testing.T) {
		err = m.put([]byte("my-key"), []byte("my-value-updated"),
			WithSecondaryKey(0, []byte("different-secondary-key")))
		require.Nil(t, err)
	})

	t.Run("retrieve by primary again", func(t *testing.T) {
		val, err := m.get([]byte("my-key"))
		require.Nil(t, err)
		assert.Equal(t, []byte("my-value-updated"), val)
	})

	t.Run("retrieve by updated secondary", func(t *testing.T) {
		val, err := m.getBySecondary(0, []byte("different-secondary-key"))
		require.Nil(t, err)
		assert.Equal(t, []byte("my-value-updated"), val)
	})

	t.Run("retrieve by initial secondary - should not find anything", func(t *testing.T) {
		val, err := m.getBySecondary(0, []byte("secondary-key-initial"))
		assert.Equal(t, lsmkv.NotFound, err)
		assert.Nil(t, val)
	})
}

func TestMemtable_Exists(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	cl, err := newCommitLogger(dir, StrategyReplace, 0)
	require.NoError(t, err)

	m, err := newMemtable(path.Join(dir, "test"), StrategyReplace, 0, cl, nil, logger, false, nil, false, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.commitlog.close())
	})

	t.Run("key exists", func(t *testing.T) {
		err := m.put([]byte("exists-key"), []byte("value"))
		require.NoError(t, err)

		err = m.exists([]byte("exists-key"))
		require.NoError(t, err)
	})

	t.Run("key does not exist", func(t *testing.T) {
		err := m.exists([]byte("nonexistent-key"))
		assert.ErrorIs(t, err, lsmkv.NotFound)
	})

	t.Run("key is tombstoned with deletion time", func(t *testing.T) {
		key := []byte("deleted-key")
		err := m.put(key, []byte("value"))
		require.NoError(t, err)

		deletionTime := time.Now()
		err = m.setTombstoneWith(key, deletionTime)
		require.NoError(t, err)

		err = m.exists(key)
		require.Error(t, err)
		assert.True(t, errors.Is(err, lsmkv.Deleted))

		// Verify deletion time is preserved
		var deletedErr lsmkv.ErrDeleted
		require.True(t, errors.As(err, &deletedErr))
		assert.WithinDuration(t, deletionTime, deletedErr.DeletionTime(), time.Millisecond)
	})

	t.Run("exists returns same error as get for deleted key", func(t *testing.T) {
		key := []byte("compare-key")
		err := m.put(key, []byte("value"))
		require.NoError(t, err)

		err = m.setTombstone(key)
		require.NoError(t, err)

		_, getErr := m.get(key)
		existsErr := m.exists(key)

		// Both should return Deleted error
		assert.True(t, errors.Is(getErr, lsmkv.Deleted))
		assert.True(t, errors.Is(existsErr, lsmkv.Deleted))
	})

	t.Run("exists returns same error as get for all cases", func(t *testing.T) {
		// Test nonexistent key
		_, getErr := m.get([]byte("never-existed"))
		existsErr := m.exists([]byte("never-existed"))
		assert.ErrorIs(t, getErr, lsmkv.NotFound)
		assert.ErrorIs(t, existsErr, lsmkv.NotFound)

		// Test existing key
		key := []byte("consistency-key")
		err := m.put(key, []byte("value"))
		require.NoError(t, err)

		_, getErr = m.get(key)
		existsErr = m.exists(key)
		assert.NoError(t, getErr)
		assert.NoError(t, existsErr)
	})
}

func TestMemtable_SecondaryKeyDeleteBug(t *testing.T) {
	key := []byte("my-key")
	value := []byte("my-value")
	secKey := []byte("secondary-key")
	otherSecKey := []byte("other-secondary-key")

	initMemtable := func(t *testing.T) *Memtable {
		t.Helper()

		dir := t.TempDir()

		logger, _ := test.NewNullLogger()
		cl, err := newCommitLogger(dir, StrategyReplace, 0)
		require.NoError(t, err)

		m, err := newMemtable(path.Join(dir, "will-never-flush"), StrategyReplace, 1, cl, nil, logger, false, nil, false, nil)
		require.NoError(t, err)

		return m
	}
	populateMemtable := func(t *testing.T, m *Memtable) {
		t.Helper()

		// add initial value
		err := m.put(key, value, WithSecondaryKey(0, secKey))
		require.NoError(t, err)

		// retrieve by primary
		val, err := m.get(key)
		require.NoError(t, err)
		require.Equal(t, value, val)

		// retrieve by secondary
		val, err = m.getBySecondary(0, secKey)
		require.NoError(t, err)
		require.Equal(t, value, val)
	}

	t.Run("delete existing keys", func(t *testing.T) {
		t.Run("delete without secondary key", func(t *testing.T) {
			memtable := initMemtable(t)
			populateMemtable(t, memtable)

			err := memtable.setTombstone(key)
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Nil(t, node.secondaryKeys[0])
			})
		})

		t.Run("delete with secondary key", func(t *testing.T) {
			memtable := initMemtable(t)
			populateMemtable(t, memtable)

			err := memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, secKey, node.secondaryKeys[0])
			})
		})

		t.Run("delete with other secondary key", func(t *testing.T) {
			memtable := initMemtable(t)
			populateMemtable(t, memtable)

			err := memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, otherSecKey, node.secondaryKeys[0])
			})
		})

		t.Run("delete with secondary key, then other secondary key", func(t *testing.T) {
			memtable := initMemtable(t)
			populateMemtable(t, memtable)

			err := memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)
			err = memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, otherSecKey, node.secondaryKeys[0])
			})
		})

		t.Run("delete with other secondary key, then secondary key", func(t *testing.T) {
			memtable := initMemtable(t)
			populateMemtable(t, memtable)

			err := memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)
			err = memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, secKey, node.secondaryKeys[0])
			})
		})
	})

	t.Run("delete non existent keys", func(t *testing.T) {
		t.Run("delete without secondary key", func(t *testing.T) {
			memtable := initMemtable(t)

			err := memtable.setTombstone(key)
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Nil(t, node.secondaryKeys[0])
			})
		})

		t.Run("delete with secondary key", func(t *testing.T) {
			memtable := initMemtable(t)

			err := memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, secKey, node.secondaryKeys[0])
			})
		})

		t.Run("delete with other secondary key", func(t *testing.T) {
			memtable := initMemtable(t)

			err := memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, otherSecKey, node.secondaryKeys[0])
			})
		})

		t.Run("delete with secondary key, then other secondary key", func(t *testing.T) {
			memtable := initMemtable(t)

			err := memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)
			err = memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, otherSecKey, nodes[0].secondaryKeys[0])
			})
		})

		t.Run("delete with other secondary key, then secondary key", func(t *testing.T) {
			memtable := initMemtable(t)

			err := memtable.setTombstone(key, WithSecondaryKey(0, otherSecKey))
			require.NoError(t, err)
			err = memtable.setTombstone(key, WithSecondaryKey(0, secKey))
			require.NoError(t, err)

			t.Run("retrieve by primary", func(t *testing.T) {
				val, err := memtable.get(key)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, secKey)
				assert.ErrorIs(t, err, lsmkv.Deleted)
				assert.Nil(t, val)
			})

			t.Run("retrieve by other secondary", func(t *testing.T) {
				val, err := memtable.getBySecondary(0, otherSecKey)
				assert.ErrorIs(t, err, lsmkv.NotFound)
				assert.Nil(t, val)
			})

			t.Run("flattened", func(t *testing.T) {
				nodes := memtable.key.flattenInOrder()
				require.Len(t, nodes, 1)

				node := nodes[0]
				assert.Equal(t, key, node.key)
				assert.Nil(t, node.value)
				assert.True(t, node.tombstone)
				require.Len(t, node.secondaryKeys, 1)
				assert.Equal(t, secKey, node.secondaryKeys[0])
			})
		})
	})
}

func TestMemtable_PutDeletePut(t *testing.T) {
	key := []byte("my-key")
	value := []byte("my-value")
	secKey := []byte("secondary-key")

	initMemtable := func(t *testing.T) *Memtable {
		t.Helper()

		dir := t.TempDir()

		logger, _ := test.NewNullLogger()
		cl, err := newCommitLogger(dir, StrategyReplace, 0)
		require.NoError(t, err)

		m, err := newMemtable(path.Join(dir, "will-never-flush"), StrategyReplace, 1, cl, nil, logger, false, nil, false, nil)
		require.NoError(t, err)

		return m
	}

	t.Run("without secondary key", func(t *testing.T) {
		m := initMemtable(t)

		err := m.put(key, value)
		require.NoError(t, err)

		val, err := m.get(key)
		require.NoError(t, err)
		require.Equal(t, value, val)

		err = m.setTombstone(key)
		require.NoError(t, err)

		val, err = m.get(key)
		require.ErrorIs(t, err, lsmkv.Deleted)
		require.Nil(t, val)

		err = m.put(key, value)
		require.NoError(t, err)

		val, err = m.get(key)
		require.NoError(t, err)
		require.Equal(t, value, val)
	})

	t.Run("with secondary key", func(t *testing.T) {
		m := initMemtable(t)

		err := m.put(key, value, WithSecondaryKey(0, secKey))
		require.NoError(t, err)

		val, err := m.get(key)
		require.NoError(t, err)
		require.Equal(t, value, val)
		val, err = m.getBySecondary(0, secKey)
		require.NoError(t, err)
		require.Equal(t, value, val)

		err = m.setTombstone(key, WithSecondaryKey(0, secKey))
		require.NoError(t, err)

		val, err = m.get(key)
		require.ErrorIs(t, err, lsmkv.Deleted)
		require.Nil(t, val)
		val, err = m.getBySecondary(0, secKey)
		require.ErrorIs(t, err, lsmkv.Deleted)
		require.Nil(t, val)

		err = m.put(key, value, WithSecondaryKey(0, secKey))
		require.NoError(t, err)

		val, err = m.get(key)
		require.NoError(t, err)
		require.Equal(t, value, val)
		val, err = m.getBySecondary(0, secKey)
		require.NoError(t, err)
		require.Equal(t, value, val)
	})
}
