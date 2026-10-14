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
	"strings"
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

	// Run concurrently: the window this guards, an invalidation that finds no
	// file to remove because the save has not written yet, cannot be scheduled
	// from a single goroutine. Only the end state is asserted. A save that
	// publishes and returns before the invalidation runs is correct too, and
	// the invalidation then removes what it published.
	t.Run("concurrent invalidation never leaves a published stale record", func(t *testing.T) {
		for range 200 {
			indexPath := newShard(t)
			gen := ComputedUsageGeneration(indexPath, shardName)

			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = RemoveComputedUsageDataForUnloadedShard(indexPath, shardName)
			}()

			_, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
			require.NoError(t, err)
			<-done

			require.NoFileExists(t, usageTmpFilePath(indexPath, shardName),
				"a record outlived an invalidation that ran alongside its save; the "+
					"next collection will serve pre-drop numbers")
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

	t.Run("a deleted shard's count is forgotten", func(t *testing.T) {
		indexPath := newShard(t)

		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, shardName))
		require.Equal(t, uint64(1), ComputedUsageGeneration(indexPath, shardName),
			"precondition: the shard must have a count to forget")
		require.Equal(t, 1, generationEntriesUnder(indexPath))

		ForgetComputedUsageGeneration(indexPath, shardName)
		require.Zero(t, generationEntriesUnder(indexPath),
			"nothing else removes these: a collection with tenant churn would keep "+
				"one entry per tenant ever created until the process restarts")
	})

	t.Run("dropping a collection forgets every shard under it", func(t *testing.T) {
		indexPath := newShard(t)
		other := newShard(t)

		for _, name := range []string{shardName, "cold-tenant"} {
			require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, name))
		}
		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(other, shardName))
		require.Equal(t, 2, generationEntriesUnder(indexPath))

		ForgetComputedUsageGenerationsUnder(indexPath)
		require.Zero(t, generationEntriesUnder(indexPath))
		require.Equal(t, 1, generationEntriesUnder(other),
			"another collection's counts must survive")
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

// generationEntriesUnder counts the invalidation counts held for one index.
func generationEntriesUnder(indexPath string) int {
	prefix := indexPath + "/"
	n := 0
	computedUsageGenerations.Range(func(key, _ any) bool {
		if name, ok := key.(string); ok && strings.HasPrefix(name, prefix) {
			n++
		}
		return true
	})
	return n
}
