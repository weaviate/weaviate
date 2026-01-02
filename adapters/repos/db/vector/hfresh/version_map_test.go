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

package hfresh

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makeVersionMap(t *testing.T) *VersionMap {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	versionMap, err := NewVersionMap(bucket)
	require.NoError(t, err)
	return versionMap
}

func TestVectorVersion(t *testing.T) {
	var ve VectorVersion

	require.Equal(t, uint8(0), ve.Version())
	require.False(t, ve.Deleted())

	ve = VectorVersion(5)
	require.Equal(t, uint8(5), ve.Version())
	require.False(t, ve.Deleted())

	ve = VectorVersion(127)
	require.Equal(t, uint8(127), ve.Version())
	require.False(t, ve.Deleted())

	ve = VectorVersion(128)
	require.Equal(t, uint8(0), ve.Version())
	require.True(t, ve.Deleted())

	ve = VectorVersion(255)
	require.Equal(t, uint8(127), ve.Version())
	require.True(t, ve.Deleted())
}

func TestVersionMap(t *testing.T) {
	ctx := t.Context()

	t.Run("get unknown vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		_, err := versionMap.Get(ctx, 1)
		require.True(t, errors.Is(err, ErrVectorNotFound))
	})

	t.Run("get existing vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		want, err := versionMap.Increment(ctx, 1, VectorVersion(0))
		require.NoError(t, err)

		got, err := versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("increment unknown vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(0))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), version)
	})

	t.Run("increment existing vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(0))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), version)

		version, err = versionMap.Increment(ctx, 1, version)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)
	})

	t.Run("increment with wrong previous version", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(0))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), version)

		version, err = versionMap.Increment(ctx, 1, VectorVersion(0))
		require.Error(t, err)
		require.Equal(t, VectorVersion(1), version)

		version, err = versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), version)
	})

	t.Run("increment with wraparound", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		var version VectorVersion
		var err error
		for i := range 127 {
			version, err = versionMap.Increment(ctx, 1, version)
			require.NoError(t, err)
			require.EqualValues(t, i+1, version.Version())
		}

		version, err = versionMap.Increment(ctx, 1, version)
		require.NoError(t, err)
		require.EqualValues(t, 0, version.Version())

		version, err = versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 0, version.Version())
		require.False(t, version.Deleted())
	})

	t.Run("mark deleted vector and check if it is deleted", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		_, err := versionMap.MarkDeleted(ctx, 1)
		require.NoError(t, err)
		deleted, err := versionMap.IsDeleted(ctx, 1)
		require.NoError(t, err)
		require.True(t, deleted)
	})
}
