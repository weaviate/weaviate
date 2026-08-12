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
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func makeVersionMap(t *testing.T) *VersionMap {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewVersionMap(bucket)
}

// MarkDeleted must persist the tombstone even when the vector is already
// deleted in memory: if a previous persist failed (or was lost), a retry has
// to heal the store, otherwise the vector resurrects on restart.
func TestMarkDeletedRepersistsTombstone(t *testing.T) {
	ctx := t.Context()
	vm := makeVersionMap(t)

	_, err := vm.MarkDeleted(ctx, 7)
	require.NoError(t, err)

	// simulate a store that missed the tombstone (failed/lost persist)
	require.NoError(t, vm.store.Set(ctx, 7, VectorVersion(3)))

	// memory already says deleted — the retry must still persist
	v, err := vm.MarkDeleted(ctx, 7)
	require.NoError(t, err)
	require.True(t, v.Deleted())

	got, err := vm.store.Get(ctx, 7)
	require.NoError(t, err)
	require.True(t, got.Deleted(), "tombstone was not re-persisted to the store")
}

// Concurrent writers and readers must leave memory and store converged: the
// persist path re-reads memory under a per-id persist lock, so the last
// persist always lands the newest value. Run with -race.
func TestVersionMapConcurrentConvergence(t *testing.T) {
	ctx := context.Background()
	vm := makeVersionMap(t)

	const numIDs = 32
	const writersPerID = 4
	const incrementsPerWriter = 20

	var wg sync.WaitGroup
	for id := range uint64(numIDs) {
		for range writersPerID {
			wg.Go(func() {
				for range incrementsPerWriter {
					cur, err := vm.Get(ctx, id)
					if err != nil {
						return
					}
					if cur.Deleted() {
						return
					}
					_, _ = vm.Increment(ctx, id, cur) // CAS may fail; retried next round
				}
			})
		}
		// concurrent readers
		wg.Go(func() {
			for range 100 {
				_, _ = vm.IsDeleted(ctx, id)
			}
		})
	}
	// delete a subset concurrently with the increments
	for id := range uint64(numIDs) {
		if id%4 != 0 {
			continue
		}
		wg.Go(func() {
			_, _ = vm.MarkDeleted(ctx, id)
		})
	}
	wg.Wait()

	// memory and store must agree for every id, and deletes must stick
	for id := range uint64(numIDs) {
		page, slot := vm.data.GetPageFor(id)
		require.NotNil(t, page, "id %d", id)
		mem := page[slot]

		stored, err := vm.store.Get(ctx, id)
		require.NoError(t, err, "id %d", id)
		require.Equal(t, mem, stored, "memory and store diverged for id %d", id)

		if id%4 == 0 {
			require.True(t, mem.Deleted(), "delete lost for id %d", id)
		}
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

		// wraparound must skip 0: the in-memory paged array uses 0 as its
		// "empty slot" sentinel, so a live vector at version 0 would be
		// indistinguishable from a never-loaded one (and e.g. the warmup
		// sweep could roll it back to an older persisted version)
		version, err = versionMap.Increment(ctx, 1, version)
		require.NoError(t, err)
		require.EqualValues(t, 1, version.Version())

		version, err = versionMap.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 1, version.Version())
		require.False(t, version.Deleted())

		// no number of increments may ever produce the reserved value 0
		v := VectorVersion(1)
		for range 300 {
			v = v.Increment()
			require.NotZero(t, v.Version())
		}
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

func TestVersionMapWarmup(t *testing.T) {
	ctx := t.Context()
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "warmup", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)

	// a previous process lifetime persisted versions: live ones and a few
	// deleted ones
	previous := NewVersionMap(bucket)
	const n = uint64(10_000)
	for i := uint64(0); i < n; i++ {
		require.NoError(t, previous.store.Set(ctx, i, VectorVersion(2)))
	}
	for i := uint64(0); i < 10; i++ {
		_, err := previous.MarkDeleted(ctx, i)
		require.NoError(t, err)
	}

	// flush so the sweep reads from disk segments, as after a real restart
	require.NoError(t, bucket.FlushAndSwitch())

	// simulate the restart: fresh map over the same bucket, then warm up
	m := NewVersionMap(bucket)
	count, err := m.Warmup(ctx)
	require.NoError(t, err)
	require.Equal(t, int(n), count, "every persisted version must be installed")

	// versions must be correct straight from memory
	deleted, err := m.IsDeleted(ctx, 5)
	require.NoError(t, err)
	require.True(t, deleted, "deleted flag must survive the warmup")

	deleted, err = m.IsDeleted(ctx, 500)
	require.NoError(t, err)
	require.False(t, deleted)

	v, err := m.Get(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, VectorVersion(2), v)

	// memory always wins: an entry loaded (and possibly being written)
	// before the warmup must not be clobbered by the sweep
	m2 := NewVersionMap(bucket)
	v, err = m2.Get(ctx, 42) // faults version 2 into memory
	require.NoError(t, err)
	require.Equal(t, VectorVersion(2), v)
	require.NoError(t, m2.store.Set(ctx, 42, VectorVersion(9))) // store diverges

	_, err = m2.Warmup(ctx)
	require.NoError(t, err)
	v, err = m2.Get(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, VectorVersion(2), v, "warmup must not overwrite in-memory state")

	// warming up an already-warm map installs nothing new
	count, err = m.Warmup(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestVersionMapWarmupConcurrentWithInserts pins that the posting-map pass
// of the warmup holds the per-posting lock: inserts mutate posting metadata
// in place, so an unlocked read is a data race (caught by -race).
func TestVersionMapWarmupConcurrentWithInserts(t *testing.T) {
	const preload, total = 200, 400
	vectors, _ := testinghelpers.RandomVecs(total, 1, 32)

	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	logger, _ := test.NewNullLogger()
	cfg.Logger = logger
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector,
		func(ctx context.Context, id uint64, _ string) ([]float32, error) {
			return vectors[id], nil
		})
	index := makeHFreshWithConfig(t, store, cfg, uc)

	ctx := t.Context()
	for i := range preload {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				index.warmVersionMap()
			}
		}
	}, logger)

	for i := preload; i < total; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}
	close(stop)
	wg.Wait()
}

func TestVersionMapEnsureDefault(t *testing.T) {
	ctx := t.Context()
	m := makeVersionMap(t)

	// unknown vector: installs the implicit first version without a store read
	require.True(t, m.EnsureDefault(7))
	page, slot := m.data.GetPageFor(7)
	require.NotNil(t, page)
	require.Equal(t, v1, page[slot], "default must be installed in memory")

	// second call is a no-op
	require.False(t, m.EnsureDefault(7))

	// a vector with a known version must not be touched
	v, err := m.Increment(ctx, 7, v1)
	require.NoError(t, err)
	require.False(t, m.EnsureDefault(7))
	got, err := m.Get(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, v, got, "EnsureDefault must never overwrite a live version")
}
