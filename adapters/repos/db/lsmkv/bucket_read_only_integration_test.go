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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestReadOnlyBucket_RecoverWALIntoMemory_NoWrites verifies that a bucket opened
// with WithReadOnly on a copy that still has an active write-ahead-log:
//   - replays the WAL into an in-memory memtable so all data is queryable,
//   - rejects write API calls (immutable),
//   - performs ZERO filesystem writes (the copy is chmod'd read-only, and the
//     directory listing is identical before and after open + reads).
func TestReadOnlyBucket_RecoverWALIntoMemory_NoWrites(t *testing.T) {
	ctx := testCtx()
	dirOrig := t.TempDir()
	dirCopy := t.TempDir()

	key1, key2, key3 := []byte("key-1"), []byte("key-2"), []byte("key-3")
	orig1 := []byte("original value for key1")
	updated3 := []byte("updated value for key 3")

	// 1. Build state in the original dir and leave it in the active WAL (no flush).
	b, err := NewBucketCreator().NewBucket(ctx, dirOrig, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithMinWalThreshold(0))
	require.Nil(t, err)
	b.SetMemtableThreshold(1e9) // never flush during the test

	require.Nil(t, b.Put(key1, orig1))
	require.Nil(t, b.Put(key2, []byte("value for key2 - deleted later")))
	require.Nil(t, b.Put(key3, []byte("first value for key3")))
	require.Nil(t, b.Delete(key2))
	require.Nil(t, b.Put(key3, updated3))
	require.Nil(t, b.WriteWAL()) // ensure the WAL is on disk

	// 2. Copy the WAL into the follower copy, then drop the original.
	copyGlob(t, dirOrig, "*.wal", dirCopy)
	require.Nil(t, b.Shutdown(ctx))

	// 3. Make the copy a hard read-only mount: any write attempt now fails loudly.
	filesBefore := snapshotTree(t, dirCopy)
	makeTreeReadOnly(t, dirCopy)
	t.Cleanup(func() { makeTreeWritable(t, dirCopy) })

	// 4. Open read-only and verify the WAL was recovered into memory.
	ro, err := NewBucketCreator().NewBucket(ctx, dirCopy, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithReadOnly(true))
	require.Nil(t, err)
	defer ro.Shutdown(ctx)

	res, err := ro.Get(key1)
	require.Nil(t, err)
	assert.Equal(t, orig1, res)

	res, err = ro.Get(key2)
	require.Nil(t, err)
	assert.Nil(t, res, "deleted key must not be recovered")

	res, err = ro.Get(key3)
	require.Nil(t, err)
	assert.Equal(t, updated3, res, "the newest value must win after replay")

	// 5. Writes are rejected.
	assert.ErrorIs(t, ro.Put(key1, []byte("nope")), ErrImmutable)

	// 6. Nothing was written to the copy.
	filesAfter := snapshotTree(t, dirCopy)
	assert.Equal(t, filesBefore, filesAfter, "read-only open must not create, modify, or delete any file")
}

// TestReadOnlyBucket_MissingBloomFilter_ComputedInMemory verifies that when a
// segment in the copy is missing its .bloom sidecar (as can happen in a
// crash-consistent snapshot of a live writer), a read-only bucket recomputes it
// in memory, serves correct reads, and never writes the sidecar back.
func TestReadOnlyBucket_MissingBloomFilter_ComputedInMemory(t *testing.T) {
	ctx := testCtx()
	dirOrig := t.TempDir()
	dirCopy := t.TempDir()

	key1 := []byte("alpha")
	val1 := []byte("alpha-value")

	// 1. Build a bucket with an on-disk segment (Shutdown flushes the memtable).
	b, err := NewBucketCreator().NewBucket(ctx, dirOrig, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	require.Nil(t, b.Put(key1, val1))
	require.Nil(t, b.FlushAndSwitch()) // create a disk segment + sidecars
	require.Nil(t, b.Shutdown(ctx))

	// 2. Copy everything, then remove the .bloom sidecar(s) from the copy.
	copyGlob(t, dirOrig, "*", dirCopy)
	removed := 0
	for _, f := range snapshotTree(t, dirCopy) {
		if filepath.Ext(f) == ".bloom" {
			require.Nil(t, os.Remove(filepath.Join(dirCopy, f)))
			removed++
		}
	}
	require.Greater(t, removed, 0, "fixture must contain at least one .bloom sidecar")

	// 3. Hard read-only mount.
	filesBefore := snapshotTree(t, dirCopy)
	makeTreeReadOnly(t, dirCopy)
	t.Cleanup(func() { makeTreeWritable(t, dirCopy) })

	// 4. Open read-only: the missing bloom is recomputed in memory, reads work.
	ro, err := NewBucketCreator().NewBucket(ctx, dirCopy, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithReadOnly(true))
	require.Nil(t, err)
	defer ro.Shutdown(ctx)

	res, err := ro.Get(key1)
	require.Nil(t, err)
	assert.Equal(t, val1, res)

	// 5. The bloom sidecar was NOT recreated and nothing else changed.
	filesAfter := snapshotTree(t, dirCopy)
	assert.Equal(t, filesBefore, filesAfter, "read-only open must not persist a recomputed sidecar")
	for _, f := range filesAfter {
		assert.NotEqual(t, ".bloom", filepath.Ext(f), "a .bloom sidecar must not be written on a read-only follower")
	}
}

// copyGlob copies files matching glob from src into dst using cp, matching the
// style of the other lsmkv integration tests.
func copyGlob(t *testing.T, src, glob, dst string) {
	t.Helper()
	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("cp -r %s/%s %s", src, glob, dst))
	var out bytes.Buffer
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("copy %q from %s to %s: %v\n%s", glob, src, dst, err, out.String())
	}
}

// snapshotTree returns the sorted list of regular-file relative paths under dir.
func snapshotTree(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files = append(files, rel)
		return nil
	})
	require.Nil(t, err)
	sort.Strings(files)
	return files
}

func makeTreeReadOnly(t *testing.T, dir string) {
	t.Helper()
	require.Nil(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0o500)
		}
		return os.Chmod(path, 0o400)
	}))
}

func makeTreeWritable(t *testing.T, dir string) {
	t.Helper()
	// best-effort restore so t.TempDir cleanup can remove the tree
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(path, 0o700)
		} else {
			_ = os.Chmod(path, 0o600)
		}
		return nil
	})
}
