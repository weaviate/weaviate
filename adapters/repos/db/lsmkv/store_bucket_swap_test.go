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

//go:build integrationTest

package lsmkv

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	return store
}

// --- SwapBucketPointer tests ---

func TestStore_SwapBucketPointer_HappyPath(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	// Create two Replace-strategy buckets with different data.
	require.NoError(t, store.CreateOrLoadBucket(ctx, "main", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	mainBucket := store.Bucket("main")
	require.NotNil(t, mainBucket)
	require.NoError(t, mainBucket.Put([]byte("key"), []byte("old-value")))

	replacementBucket := store.Bucket("replacement")
	require.NotNil(t, replacementBucket)
	require.NoError(t, replacementBucket.Put([]byte("key"), []byte("new-value")))

	// Swap: "main" now points to the replacement bucket.
	oldBucket, err := store.SwapBucketPointer(ctx, "main", "replacement")
	require.NoError(t, err)

	// The returned old bucket should still have the old data.
	val, err := oldBucket.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("old-value"), val)

	// Store.Bucket("main") should now serve new data.
	swapped := store.Bucket("main")
	require.NotNil(t, swapped)
	val, err = swapped.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-value"), val)

	// "replacement" should no longer exist.
	assert.Nil(t, store.Bucket("replacement"))
}

func TestStore_SwapBucketPointer_TargetNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "source", WithStrategy(StrategyReplace)))

	_, err := store.SwapBucketPointer(ctx, "nonexistent", "source")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target bucket")
	assert.Contains(t, err.Error(), "not found")
}

func TestStore_SwapBucketPointer_SourceNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))

	_, err := store.SwapBucketPointer(ctx, "target", "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source bucket")
	assert.Contains(t, err.Error(), "not found")
}

func TestStore_SwapBucketPointer_ConcurrentReads(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "main", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	mainBucket := store.Bucket("main")
	require.NoError(t, mainBucket.Put([]byte("key"), []byte("old-value")))
	replacementBucket := store.Bucket("replacement")
	require.NoError(t, replacementBucket.Put([]byte("key"), []byte("new-value")))

	// Spin up concurrent readers that continuously read from "main".
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				b := store.Bucket("main")
				if b == nil {
					// Should never happen since "main" is always present.
					continue
				}
				val, err := b.Get([]byte("key"))
				if err != nil {
					continue
				}
				// Value must be one of the two valid values.
				if val != nil {
					v := string(val)
					if v != "old-value" && v != "new-value" {
						t.Errorf("unexpected value: %s", v)
						return
					}
				}
			}
		}()
	}

	// Perform the swap while readers are active.
	oldBucket, err := store.SwapBucketPointer(ctx, "main", "replacement")
	require.NoError(t, err)

	// Stop readers.
	close(stop)
	wg.Wait()

	// Verify final state.
	val, err := store.Bucket("main").Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-value"), val)

	// Old bucket still has old data.
	val, err = oldBucket.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("old-value"), val)
}

func TestStore_SwapBucketPointer_OldBucketShutdown(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "main", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	require.NoError(t, store.Bucket("replacement").Put([]byte("key"), []byte("new-value")))

	oldBucket, err := store.SwapBucketPointer(ctx, "main", "replacement")
	require.NoError(t, err)

	// Shut down the old bucket.
	require.NoError(t, oldBucket.Shutdown(ctx))

	// Store should still work — the new bucket is alive.
	val, err := store.Bucket("main").Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-value"), val)
}

func TestStore_SwapBucketPointer_StoreClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "main", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	require.NoError(t, store.Shutdown(ctx))

	_, err := store.SwapBucketPointer(ctx, "main", "replacement")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

// --- FinalizeBucketSwap tests ---

func TestStore_FinalizeBucketSwap_HappyPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	// Create a bucket at a non-canonical directory name (simulates post-swap state).
	nonCanonicalName := "prop_searchable_ingest"
	require.NoError(t, store.CreateOrLoadBucket(ctx, nonCanonicalName, WithStrategy(StrategyReplace)))
	require.NoError(t, store.Bucket(nonCanonicalName).Put([]byte("key"), []byte("value")))

	currentDir := filepath.Join(dir, nonCanonicalName)
	canonicalDir := filepath.Join(dir, "prop_searchable")
	backupDir := filepath.Join(dir, "prop_searchable_bak")

	require.NoError(t, store.FinalizeBucketSwap(ctx, nonCanonicalName, canonicalDir, currentDir, backupDir))

	// The non-canonical directory should no longer exist; canonical should.
	_, err = os.Stat(currentDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(canonicalDir)
	require.NoError(t, err)

	// Bucket should still be accessible and serve data.
	b := store.Bucket(nonCanonicalName)
	require.NotNil(t, b)
	val, err := b.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), val)

	// New writes should work (memtable was recreated).
	require.NoError(t, b.Put([]byte("key2"), []byte("value2")))
	val, err = b.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), val)

	require.NoError(t, store.Shutdown(ctx))
}

func TestStore_FinalizeBucketSwap_BackupDirCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	nonCanonicalName := "prop_ingest"
	require.NoError(t, store.CreateOrLoadBucket(ctx, nonCanonicalName, WithStrategy(StrategyReplace)))

	currentDir := filepath.Join(dir, nonCanonicalName)
	canonicalDir := filepath.Join(dir, "prop")
	backupDir := filepath.Join(dir, "prop_bak")

	// Create a backup dir with some content to simulate the old bucket's dir.
	require.NoError(t, os.MkdirAll(filepath.Join(backupDir, "subdir"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "file.txt"), []byte("stale"), 0o600))

	require.NoError(t, store.FinalizeBucketSwap(ctx, nonCanonicalName, canonicalDir, currentDir, backupDir))

	// Backup dir should be removed.
	_, err = os.Stat(backupDir)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, store.Shutdown(ctx))
}

func TestStore_FinalizeBucketSwap_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Shutdown(ctx)

	err := store.FinalizeBucketSwap(ctx, "nonexistent", "/a", "/b", "/c")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestStore_FinalizeBucketSwap_FullRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Phase 1: Create store with two buckets and perform a runtime swap.
	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "prop_searchable", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "prop_searchable_ingest", WithStrategy(StrategyReplace)))

	require.NoError(t, store.Bucket("prop_searchable").Put([]byte("k1"), []byte("original")))
	require.NoError(t, store.Bucket("prop_searchable_ingest").Put([]byte("k1"), []byte("migrated")))

	// Runtime swap.
	oldBucket, err := store.SwapBucketPointer(ctx, "prop_searchable", "prop_searchable_ingest")
	require.NoError(t, err)

	// Verify swap.
	val, err := store.Bucket("prop_searchable").Get([]byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("migrated"), val)
	assert.Nil(t, store.Bucket("prop_searchable_ingest"))

	// Shut down old bucket and rename its dir to _bak.
	oldDir := oldBucket.GetDir()
	require.NoError(t, oldBucket.Shutdown(ctx))
	bakDir := oldDir + "_bak"
	require.NoError(t, os.Rename(oldDir, bakDir))

	// Shut down store (simulates restart).
	require.NoError(t, store.Shutdown(ctx))

	// Phase 2: Reopen store, load from non-canonical dir, finalize.
	store2, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	// The bucket is on disk at "prop_searchable_ingest" dir (the replacement's original dir).
	// Load it under its dir name.
	ingestDir := filepath.Join(dir, "prop_searchable_ingest")
	canonicalDir := filepath.Join(dir, "prop_searchable")

	// The ingest dir still exists on disk because SwapBucketPointer is memory-only.
	_, err = os.Stat(ingestDir)
	require.NoError(t, err, "ingest dir should still exist on disk")

	require.NoError(t, store2.CreateOrLoadBucket(ctx, "prop_searchable_ingest", WithStrategy(StrategyReplace)))

	// Verify data loaded from the ingest dir.
	val, err = store2.Bucket("prop_searchable_ingest").Get([]byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("migrated"), val)

	// Finalize: rename ingest dir → canonical dir, remove _bak.
	require.NoError(t, store2.FinalizeBucketSwap(ctx,
		"prop_searchable_ingest", canonicalDir, ingestDir, bakDir))

	// Verify: canonical dir exists, ingest dir gone, _bak gone.
	_, err = os.Stat(canonicalDir)
	require.NoError(t, err)
	_, err = os.Stat(ingestDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(bakDir)
	require.True(t, os.IsNotExist(err))

	// Data still accessible.
	val, err = store2.Bucket("prop_searchable_ingest").Get([]byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("migrated"), val)

	// New writes work.
	require.NoError(t, store2.Bucket("prop_searchable_ingest").Put([]byte("k2"), []byte("post-finalize")))
	val, err = store2.Bucket("prop_searchable_ingest").Get([]byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("post-finalize"), val)

	require.NoError(t, store2.Shutdown(ctx))
}
