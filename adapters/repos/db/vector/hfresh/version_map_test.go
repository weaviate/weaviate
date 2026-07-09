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

package hfresh

import (
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makeVersionMap(t *testing.T) *VersionMap {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewVersionMap(bucket)
}

// Restore must bulk-load every persisted version into the in-memory paged
// array at startup. Without it, every first access of a vector ID during the
// posting scan falls back to an LSM point read — thousands of random disk
// reads per query on a freshly started node. The scan is partitioned by the
// first byte of the LE-encoded ID and run in parallel.
func TestVersionMapRestore(t *testing.T) {
	ctx := t.Context()
	logger, _ := logrustest.NewNullLogger()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)

	// persist versions, simulating a previous run. IDs are chosen to land in
	// several different first-byte partitions of the LE encoding while
	// staying within the paged array's ~1B capacity.
	want := map[uint64]VectorVersion{
		0:           3,
		1:           4,
		42:          tombstoneMask | 5, // deleted
		255:         9,
		256:         10,
		257:         11,
		65_536:      12,
		1_000_000:   7,
		500_000_000: 8,
		999_999_999: 13,
	}
	vm := NewVersionMap(bucket)
	for id, version := range want {
		require.NoError(t, vm.store.Set(ctx, id, version))
	}

	// unrelated data under a different prefix in the same shared bucket must
	// not be picked up
	sizes := NewPostingSizesStore(bucket, postingSizesBucketPrefix)
	require.NoError(t, sizes.Set(ctx, 42, 123))

	for _, concurrency := range []string{"1", "8"} {
		t.Run("concurrency="+concurrency, func(t *testing.T) {
			t.Setenv("HFRESH_RESTORE_CONCURRENCY", concurrency)

			// fresh map over the same bucket, as after a restart
			vm2 := NewVersionMap(bucket)
			count, err := vm2.Restore(ctx, logger)
			require.NoError(t, err)
			require.EqualValues(t, len(want), count)

			// the versions are in memory: pages exist and hold the right values
			for id, version := range want {
				page, slot := vm2.data.GetPageFor(id)
				require.NotNil(t, page, "id %d not restored into memory", id)
				require.Equal(t, version, page[slot], "id %d", id)
			}

			// and the public API agrees
			v, err := vm2.Get(ctx, 42)
			require.NoError(t, err)
			require.True(t, v.Deleted())
			v, err = vm2.Get(ctx, 500_000_000)
			require.NoError(t, err)
			require.Equal(t, uint8(8), v.Version())
		})
	}
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

		v, err := versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(1), v)
	})

	t.Run("get existing vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		want, err := versionMap.Increment(ctx, 1, VectorVersion(1))
		require.NoError(t, err)

		got, err := versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("increment unknown vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(1))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)
	})

	t.Run("increment existing vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(1))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)

		version, err = versionMap.Increment(ctx, 1, version)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(3), version)
	})

	t.Run("increment with wrong previous version", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, VectorVersion(1))
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)

		version, err = versionMap.Increment(ctx, 1, VectorVersion(1))
		require.Error(t, err)
		require.Equal(t, VectorVersion(2), version)

		version, err = versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)
	})

	t.Run("increment with wraparound", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version := v1
		var err error
		for i := range 126 {
			version, err = versionMap.Increment(ctx, 1, version)
			require.NoError(t, err)
			require.EqualValues(t, i+2, version.Version())
		}

		version, err = versionMap.Increment(ctx, 1, version)
		require.NoError(t, err)
		require.EqualValues(t, 0, version.Version())

		version, err = versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 0, version.Version())
		require.False(t, version.Deleted())
	})

	t.Run("mark unknown vector as deleted", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		_, err := versionMap.MarkDeleted(ctx, 1)
		require.NoError(t, err)
	})

	t.Run("mark vector as deleted and check if it is deleted", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, v1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)

		_, err = versionMap.MarkDeleted(ctx, 1)
		require.NoError(t, err)

		deleted, err := versionMap.IsDeleted(ctx, 1)
		require.NoError(t, err)
		require.True(t, deleted)
	})

	t.Run("mark deleted vector as deleted", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		version, err := versionMap.Increment(ctx, 1, v1)
		require.NoError(t, err)
		require.Equal(t, VectorVersion(2), version)

		v, err := versionMap.MarkDeleted(ctx, 1)
		require.NoError(t, err)
		require.True(t, v.Deleted())

		v, err = versionMap.MarkDeleted(ctx, 1)
		require.NoError(t, err)
		require.True(t, v.Deleted())

		deleted, err := versionMap.IsDeleted(ctx, 1)
		require.NoError(t, err)
		require.True(t, deleted)
	})

	t.Run("check if unknown vector is deleted", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		deleted, err := versionMap.IsDeleted(ctx, 1)
		require.NoError(t, err)
		require.False(t, deleted)
	})

	t.Run("get non-cached vector", func(t *testing.T) {
		versionMap := makeVersionMap(t)

		v3 := v1.Increment().Increment()
		err := versionMap.store.Set(ctx, 1, v3)
		require.NoError(t, err)

		v, err := versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, v3, v)
	})
}

func TestVersionStore(t *testing.T) {
	ctx := t.Context()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	versionStore := NewVersionStore(bucket)

	// get unknown vector
	v, err := versionStore.Get(ctx, 1)
	require.ErrorIs(t, err, ErrVectorNotFound)
	require.Equal(t, VectorVersion(0), v)

	// set and get vector
	err = versionStore.Set(ctx, 1, VectorVersion(5))
	require.NoError(t, err)

	v, err = versionStore.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, VectorVersion(5), v)

	// update and get vector
	err = versionStore.Set(ctx, 1, VectorVersion(10))
	require.NoError(t, err)

	v, err = versionStore.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, VectorVersion(10), v)
}
