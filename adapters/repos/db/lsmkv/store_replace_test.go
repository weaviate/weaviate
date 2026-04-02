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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func newTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	logger, _ := test.NewNullLogger()

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("compaction", logger, 1),
		cyclemanager.NewCallbackGroup("compactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	return store
}

func TestStore_ReplaceBuckets(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "original",
			WithStrategy(StrategyReplace)))
		origBucket := store.Bucket("original")
		require.NotNil(t, origBucket)
		require.NoError(t, origBucket.Put([]byte("key1"), []byte("old_value")))
		require.NoError(t, origBucket.FlushAndSwitch())

		require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement",
			WithStrategy(StrategyReplace)))
		replBucket := store.Bucket("replacement")
		require.NotNil(t, replBucket)
		require.NoError(t, replBucket.Put([]byte("key1"), []byte("new_value")))
		require.NoError(t, replBucket.Put([]byte("key2"), []byte("extra")))
		require.NoError(t, replBucket.FlushAndSwitch())

		require.NoError(t, store.ReplaceBuckets(ctx, "original", "replacement"))

		// Bucket "original" should now serve replacement data.
		b := store.Bucket("original")
		require.NotNil(t, b)

		val, err := b.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("new_value"), val)

		val, err = b.Get([]byte("key2"))
		require.NoError(t, err)
		assert.Equal(t, []byte("extra"), val)

		// Replacement name should no longer exist.
		assert.Nil(t, store.Bucket("replacement"))
	})

	t.Run("non-existent original bucket", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement",
			WithStrategy(StrategyReplace)))

		err := store.ReplaceBuckets(ctx, "nonexistent", "replacement")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		// Replacement bucket should still be accessible.
		assert.NotNil(t, store.Bucket("replacement"))
	})

	t.Run("non-existent replacement bucket", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "original",
			WithStrategy(StrategyReplace)))

		err := store.ReplaceBuckets(ctx, "original", "nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		// Original bucket should still be accessible.
		assert.NotNil(t, store.Bucket("original"))
	})

	t.Run("old bucket directory is cleaned up", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "original",
			WithStrategy(StrategyReplace)))
		origBucket := store.Bucket("original")
		require.NoError(t, origBucket.Put([]byte("key"), []byte("val")))
		require.NoError(t, origBucket.FlushAndSwitch())

		require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement",
			WithStrategy(StrategyReplace)))
		replBucket := store.Bucket("replacement")
		require.NoError(t, replBucket.Put([]byte("key"), []byte("val2")))
		require.NoError(t, replBucket.FlushAndSwitch())

		origDir := origBucket.dir

		require.NoError(t, store.ReplaceBuckets(ctx, "original", "replacement"))

		// The ___del directory should have been removed.
		_, err := os.Stat(origDir + "___del")
		assert.True(t, os.IsNotExist(err), "old bucket dir should be cleaned up")

		// The replacement bucket should now live at the original's path.
		b := store.Bucket("original")
		require.NotNil(t, b)
		assert.Equal(t, origDir, b.dir)
	})

	t.Run("concurrent reads during replace", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket",
			WithStrategy(StrategyReplace)))
		require.NoError(t, store.Bucket("bucket").Put([]byte("key"), []byte("original")))
		require.NoError(t, store.Bucket("bucket").FlushAndSwitch())

		require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_new",
			WithStrategy(StrategyReplace)))
		require.NoError(t, store.Bucket("bucket_new").Put([]byte("key"), []byte("replaced")))
		require.NoError(t, store.Bucket("bucket_new").FlushAndSwitch())

		const numReaders = 10
		const readsPerReader = 100

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, numReaders+1)

		// Start readers that continuously read from the bucket.
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			i := i
			go func() {
				defer wg.Done()
				<-start
				for j := 0; j < readsPerReader; j++ {
					b := store.Bucket("bucket")
					if b == nil {
						errs[i] = fmt.Errorf("bucket was nil on read %d", j)
						return
					}
					_, err := b.Get([]byte("key"))
					if err != nil {
						errs[i] = err
						return
					}
				}
			}()
		}

		// Start the replace concurrently with reads.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := store.ReplaceBuckets(ctx, "bucket", "bucket_new"); err != nil {
				errs[numReaders] = err
			}
		}()

		close(start)
		wg.Wait()

		for i, err := range errs {
			assert.NoError(t, err, "goroutine %d had error", i)
		}

		// After replace, reads should return replacement data.
		b := store.Bucket("bucket")
		require.NotNil(t, b)
		val, err := b.Get([]byte("key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("replaced"), val)
	})
}

func TestStore_RenameBucket(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "old_name",
			WithStrategy(StrategyReplace)))
		bucket := store.Bucket("old_name")
		require.NotNil(t, bucket)
		require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
		require.NoError(t, bucket.FlushAndSwitch())

		bucket.UpdateStatus(storagestate.StatusReadOnly)

		require.NoError(t, store.RenameBucket(ctx, "old_name", "new_name"))

		// Old name should be gone.
		assert.Nil(t, store.Bucket("old_name"))

		// New name should have the data.
		b := store.Bucket("new_name")
		require.NotNil(t, b)
		val, err := b.Get([]byte("key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), val)

		// Directory should be at the new location.
		expectedDir := filepath.Join(store.dir, "new_name")
		assert.Equal(t, expectedDir, b.dir)

		// Old directory should not exist.
		oldDir := filepath.Join(store.dir, "old_name")
		_, err = os.Stat(oldDir)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("target already exists", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_a",
			WithStrategy(StrategyReplace)))
		require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket_b",
			WithStrategy(StrategyReplace)))

		store.Bucket("bucket_a").UpdateStatus(storagestate.StatusReadOnly)

		err := store.RenameBucket(ctx, "bucket_a", "bucket_b")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")

		// Both buckets should still be accessible under original names.
		assert.NotNil(t, store.Bucket("bucket_a"))
		assert.NotNil(t, store.Bucket("bucket_b"))
	})

	t.Run("source not found", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		err := store.RenameBucket(ctx, "nonexistent", "new_name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("not ReadOnly", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		store := newTestStore(t, "")
		defer store.Shutdown(ctx)

		require.NoError(t, store.CreateOrLoadBucket(ctx, "bucket",
			WithStrategy(StrategyReplace)))

		err := store.RenameBucket(ctx, "bucket", "new_name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "READONLY")

		// Bucket should still be accessible under original name.
		assert.NotNil(t, store.Bucket("bucket"))
		assert.Nil(t, store.Bucket("new_name"))
	})
}
