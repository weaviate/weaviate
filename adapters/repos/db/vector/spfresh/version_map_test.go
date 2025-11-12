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

	t.Run("increment unknown vector", func(t *testing.T) {
		version, err := versionMap.Increment(context.Background(), 1, VectorVersion(0))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), version)
	})

	t.Run("increment existing vector", func(t *testing.T) {
		version, err := versionMap.Increment(context.Background(), 1, VectorVersion(1))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)
	})

	t.Run("mark deleted vector and check if it is deleted", func(t *testing.T) {
		_, err := versionMap.MarkDeleted(context.Background(), 1)
		require.NoError(t, err)
		deleted, err := versionMap.IsDeleted(context.Background(), 1)
		require.NoError(t, err)
		require.True(t, deleted)
	})
}
