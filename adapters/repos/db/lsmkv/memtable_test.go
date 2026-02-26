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

	m, err := newMemtable(path.Join(dir, "will-never-flush"), StrategyReplace, 1, cl, nil, logger, false, nil, false, nil, nil, false)
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

	m, err := newMemtable(path.Join(dir, "test"), StrategyReplace, 0, cl, nil, logger, false, nil, false, nil, nil, false)
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
