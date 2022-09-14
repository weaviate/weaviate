package lsmkv

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test prevents a regression on
// https://www.youtube.com/watch?v=OS8taasZl8k
func Test_MemtableSecondaryKeyBug(t *testing.T) {
	dir := t.TempDir()
	m, err := newMemtable(path.Join(dir, "will-never-flush"), StrategyReplace, 1, nil)
	require.Nil(t, err)

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
		assert.Equal(t, NotFound, err)
		assert.Nil(t, val)
	})
}
