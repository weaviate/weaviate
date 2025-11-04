package spfresh

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestVersionMapPersistence(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	versionMap, err := NewVersionMap(store, "test", StoreConfig{})
	require.NoError(t, err)

	t.Run("get unknown vector", func(t *testing.T) {
		_, err = versionMap.Get(context.Background(), 1)
		require.True(t, errors.Is(err, ErrVectorNotFound))
	})

	t.Run("set and get vector", func(t *testing.T) {
		err = versionMap.Set(context.Background(), 1, VectorVersion(5))
		require.NoError(t, err)
		version, err := versionMap.Get(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(5), version)
	})

	t.Run("increment vector", func(t *testing.T) {
		version, err := versionMap.Increment(context.Background(), 1, VectorVersion(5))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(6), version)
	})

	t.Run("mark deleted vector and check if it is deleted", func(t *testing.T) {
		_, err = versionMap.MarkDeleted(context.Background(), 1)
		require.NoError(t, err)
		deleted, err := versionMap.IsDeleted(context.Background(), 1)
		require.NoError(t, err)
		require.True(t, deleted)
	})
}
