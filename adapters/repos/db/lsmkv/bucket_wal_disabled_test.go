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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestBucketWALDisabled_NoWALFilesCreated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write some data
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, b.Put([]byte("key2"), []byte("value2")))

	// Verify no .wal files exist
	assertNoWALFiles(t, dir)
}

func TestBucketWALDisabled_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write and read back
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, b.Put([]byte("key2"), []byte("value2")))

	val1, err := b.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := b.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), val2)
}

func TestBucketWALDisabled_WALEnabledRegression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Create a bucket WITH WAL enabled (default behavior)
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	// Write data to trigger WAL creation
	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))

	// Verify .wal files exist when WAL is enabled
	hasWAL := hasWALFiles(t, dir)
	assert.True(t, hasWAL, "expected .wal files to exist when WAL is enabled")
}

func TestBucketWALDisabled_RecoverySkipped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	// Create bucket with WAL disabled, write data, flush to segment, then reopen
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)

	require.NoError(t, b.Put([]byte("key1"), []byte("value1")))

	// Force flush to segment
	require.NoError(t, b.FlushAndSwitch())

	// Shut down
	require.NoError(t, b.Shutdown(ctx))

	// Reopen the bucket with WAL disabled
	b2, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWALDisabled(),
	)
	require.NoError(t, err)
	defer b2.Shutdown(ctx)

	// Data should be readable from flushed segments (no WAL recovery needed)
	val, err := b2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func assertNoWALFiles(t *testing.T, dir string) {
	t.Helper()
	assert.False(t, hasWALFiles(t, dir), "expected no .wal files in %s", dir)
}

func hasWALFiles(t *testing.T, dir string) bool {
	t.Helper()
	var found bool
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".wal" {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}
