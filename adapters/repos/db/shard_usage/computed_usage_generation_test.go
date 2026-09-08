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

package shardusage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/usage/types"
)

// TestSaveComputedUsageData_Generation covers the window between a usage scan
// and the save that publishes it. The dimensions bucket lock is released when
// the scan returns, so a drop can clear the rows and invalidate the record in
// between; the generation the scan carries is what makes the save notice.
func TestSaveComputedUsageData_Generation(t *testing.T) {
	const shardName = "shard1"
	usage := &types.ShardUsage{Name: shardName, ObjectsCount: 7}

	newShard := func(t *testing.T) string {
		t.Helper()
		indexPath := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(indexPath, shardName), 0o700))
		return indexPath
	}

	t.Run("publishes when nothing invalidated in between", func(t *testing.T) {
		indexPath := newShard(t)

		gen := ComputedUsageGeneration(indexPath, shardName)
		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.True(t, saved)

		got, err := LoadComputedUsageData(indexPath, shardName)
		require.NoError(t, err)
		require.Equal(t, int64(7), got.ShardUsage.ObjectsCount)
	})

	t.Run("declines a record whose rows were cleared mid-scan", func(t *testing.T) {
		indexPath := newShard(t)

		// The scan starts here...
		gen := ComputedUsageGeneration(indexPath, shardName)
		// ...a drop clears the rows and invalidates while it is assembling...
		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, shardName))
		// ...and only then does the scan try to publish what it read.
		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.False(t, saved,
			"the record was computed from rows a drop has since cleared; publishing it "+
				"means a vector re-created under the same name is billed for the old one")

		require.NoFileExists(t, usageTmpFilePath(indexPath, shardName),
			"nothing may be left on disk for the next collection to serve")
	})

	t.Run("an invalidation with no file to remove still counts", func(t *testing.T) {
		indexPath := newShard(t)
		gen := ComputedUsageGeneration(indexPath, shardName)

		// Nothing on disk to delete, so the bump is the only trace the
		// invalidation leaves — which is why it happens before the removal.
		computedUsageGenerationFor(indexPath, shardName).Add(1)

		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.False(t, saved)
		require.NoFileExists(t, usageTmpFilePath(indexPath, shardName))
	})

	// SaveComputedUsageData checks the generation before the write and again
	// after it. The second check covers a window a single-threaded test cannot
	// reach: an invalidation that passes the first check, finds no file to
	// remove, and is then overwritten by this save. Run concurrently instead,
	// asserting the invariant both checks exist to hold — a published record is
	// never one that was invalidated while it was being written.
	t.Run("concurrent invalidation never leaves a published stale record", func(t *testing.T) {
		for range 200 {
			indexPath := newShard(t)
			gen := ComputedUsageGeneration(indexPath, shardName)

			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = RemoveComputedUsageDataForUnloadedShard(indexPath, shardName)
			}()

			saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
			require.NoError(t, err)
			<-done

			if !saved {
				require.NoFileExists(t, usageTmpFilePath(indexPath, shardName),
					"a declined save must leave nothing for the next collection to serve")
				continue
			}
			require.Equal(t, gen, ComputedUsageGeneration(indexPath, shardName),
				"a record was published even though the shard was invalidated while "+
					"it was written; the next collection will serve pre-drop numbers")
		}
	})

	t.Run("a later scan publishes again", func(t *testing.T) {
		indexPath := newShard(t)

		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, shardName))
		gen := ComputedUsageGeneration(indexPath, shardName)
		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.True(t, saved, "invalidation must not wedge the cache permanently")
	})

	t.Run("generations are per shard", func(t *testing.T) {
		indexPath := newShard(t)
		require.NoError(t, os.MkdirAll(filepath.Join(indexPath, "other"), 0o700))

		gen := ComputedUsageGeneration(indexPath, shardName)
		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, "other"))

		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.True(t, saved, "another shard's drop must not stop this one caching")
	})
}
