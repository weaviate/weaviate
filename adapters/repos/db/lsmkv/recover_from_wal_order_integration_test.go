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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// Reproduces the crash-during-flush state: two non-empty WALs on disk, an older
// one (memtable being flushed) and a newer one (active memtable). Recovery must
// keep the newest as the active memtable, else the older stale values shadow the
// newer flushed data. The iteration loop defeats the random WAL-file map order,
// so a recovery that keeps the wrong WAL active is caught deterministically.
func TestReplaceStrategy_RecoverFromMultipleWALs_NewestWins(t *testing.T) {
	key := []byte("key-1")

	tests := []struct {
		name         string
		newerWAL     []byte // written under the larger (newer) timestamp
		expectedGet  []byte
		expectNilGet bool
	}{
		{
			name:        "newer WAL updates the key",
			newerWAL:    makeReplaceWAL(t, key, []byte("fresh-v2"), false),
			expectedGet: []byte("fresh-v2"),
		},
		{
			name:         "newer WAL deletes the key",
			newerWAL:     makeReplaceWAL(t, key, nil, true),
			expectNilGet: true,
		},
	}

	// stale value lives in the older WAL (the memtable being flushed at crash)
	olderWAL := makeReplaceWAL(t, key, []byte("stale-v1"), false)

	const iterations = 30
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < iterations; i++ {
				dir := t.TempDir()
				// Fixed-width (19-digit) unix-nano timestamps; lexicographic order
				// equals chronological order.
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, "segment-1000000000000000000.wal"), olderWAL, 0o644))
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, "segment-2000000000000000000.wal"), tt.newerWAL, 0o644))

				b, err := NewBucketCreator().NewBucket(testCtx(), dir, "", nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					WithStrategy(StrategyReplace), WithMinWalThreshold(0))
				require.NoError(t, err)

				res, err := b.Get(key)
				require.NoError(t, err)
				if tt.expectNilGet {
					assert.Nilf(t, res, "iteration %d: newer WAL's delete must win", i)
				} else {
					assert.Equalf(t, tt.expectedGet, res,
						"iteration %d: newer WAL's value must win over the older WAL", i)
				}

				require.NoError(t, b.Shutdown(context.Background()))
			}
		})
	}
}

// makeReplaceWAL writes a single Put (or Delete when del is true) to a throwaway
// bucket and returns the resulting WAL's raw bytes for staging in a recovery dir.
func makeReplaceWAL(t *testing.T, key, value []byte, del bool) []byte {
	t.Helper()

	dir := t.TempDir()
	b, err := NewBucketCreator().NewBucket(testCtx(), dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithMinWalThreshold(0))
	require.NoError(t, err)

	if del {
		require.NoError(t, b.Delete(key))
	} else {
		require.NoError(t, b.Put(key, value))
	}
	require.NoError(t, b.WriteWAL())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var walName string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walName = e.Name()
		}
	}
	require.NotEmpty(t, walName, "expected exactly one .wal file")

	data, err := os.ReadFile(filepath.Join(dir, walName))
	require.NoError(t, err)
	require.NotEmpty(t, data)

	require.NoError(t, b.Shutdown(context.Background()))
	return data
}
