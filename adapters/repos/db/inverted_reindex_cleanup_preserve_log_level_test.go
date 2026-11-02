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

package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// This runs inside the RAFT apply loop, so an Info line here costs one per
// tenant.
func TestCleanStaleSidecarDirsPreservedLogAtDebug(t *testing.T) {
	root := t.TempDir()
	hookLogger, hook := test.NewNullLogger()
	hookLogger.SetLevel(logrus.DebugLevel)

	shard := &Shard{
		name:  "s1",
		index: &Index{logger: hookLogger, Config: IndexConfig{RootPath: root, ClassName: "C"}},
	}
	require.NoError(t, os.MkdirAll(shard.pathLSM(), 0o777))

	const mainBucket = "property_category_searchable"
	const preserved = 3
	dirs := map[string]bool{}
	for gen := 1; gen <= preserved; gen++ {
		name := mainBucket + "__enable_searchable_ingest" + genSuffix(gen)
		require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), name), 0o777))
		dirs[name] = true
	}

	shard.cleanStaleSidecarDirsWithPreserved(mainBucket, migrationPreservingOnly(dirs))

	var infoCount, preservedCount int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.InfoLevel {
			infoCount++
		}
		if e.Level == logrus.DebugLevel && strings.Contains(e.Message, "preserving the sidecar dir") {
			preservedCount++
		}
	}
	require.Equal(t, 0, infoCount,
		"preserving a committed migration's sidecar dir must not log at Info inside the RAFT apply loop")
	require.Equal(t, preserved, preservedCount, "one Debug line per preserved sidecar dir")
	for name := range dirs {
		require.DirExists(t, filepath.Join(shard.pathLSM(), name), "a preserved sidecar dir must survive")
	}
}

// dirsSwept must count real removed dirs, not be inflated by looping over index types.
func TestApplyPathCountsSweptDirsNotIndexTypes(t *testing.T) {
	ctx := testCtx()
	className := "ApplySweepCount" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	// Two dirs under two index types and none under the third, so neither a
	// count per index type nor one per dir-holding type can land on 4.
	var seeded []string
	for _, indexType := range []string{"filterable", "searchable"} {
		mainBucket, ok := mainBucketForPropertyIndex(prop.Name, indexType)
		require.True(t, ok)
		for gen := 1; gen <= 2; gen++ {
			dir := filepath.Join(shard.pathLSM(), mainBucket+"__reindex"+genSuffix(gen))
			require.NoError(t, os.MkdirAll(dir, 0o777))
			seeded = append(seeded, dir)
		}
	}

	var counts migrationSweepCounts
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	shard.updatePropertyBuckets(ctx, eg, prop, &counts)
	require.NoError(t, eg.Wait())

	require.Equal(t, int64(4), counts.dirsSwept.Load(), "one per removed dir")
	for _, dir := range seeded {
		require.NoDirExists(t, dir)
	}
}

// A swept count must not include a dir still on disk: preserved, or its removal failed.
func TestCleanStaleSidecarDirsCountsOnlyRemovedDirs(t *testing.T) {
	const mainBucket = "property_category_searchable"
	dir := mainBucket + "__reindex" + genSuffix(1)

	tests := []struct {
		name      string
		preserved bool
		// unremovable puts a file inside a read-only dir, so its removal fails.
		unremovable bool
		wantSwept   int
	}{
		{name: "a removable dir", wantSwept: 1},
		{name: "a preserved dir", preserved: true, wantSwept: 0},
		{name: "a dir whose removal fails", unremovable: true, wantSwept: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.unremovable && os.Geteuid() == 0 {
				t.Skip("root ignores dir permissions")
			}
			logger, _ := test.NewNullLogger()
			shard := &Shard{
				name:  "s1",
				index: &Index{logger: logger, Config: IndexConfig{RootPath: t.TempDir(), ClassName: "C"}},
			}
			path := filepath.Join(shard.pathLSM(), dir)
			require.NoError(t, os.MkdirAll(path, 0o777))
			if tc.unremovable {
				require.NoError(t, os.WriteFile(filepath.Join(path, "segment.db"), nil, 0o666))
				require.NoError(t, os.Chmod(path, 0o500))
				t.Cleanup(func() { _ = os.Chmod(path, 0o777) })
			}

			preserved := map[string]bool{}
			if tc.preserved {
				preserved[dir] = true
			}

			got := shard.cleanStaleSidecarDirsWithPreserved(mainBucket, migrationPreservingOnly(preserved))

			require.Equal(t, tc.wantSwept, got)
			if tc.wantSwept == 0 {
				require.DirExists(t, path)
			} else {
				require.NoDirExists(t, path)
			}
		})
	}
}

type errAfterCtx struct {
	context.Context
	calls atomic.Int32
	after int32
}

func (c *errAfterCtx) Err() error {
	if c.calls.Add(1) > c.after {
		return context.Canceled
	}
	return nil
}

// An apply that fails after sweeping a dir still logs that dir, since it is gone.
func TestApplyPathFailingMidLoopKeepsItsCount(t *testing.T) {
	ctx := testCtx()
	class := newTestClassWithProps("ApplySweepPartial"+uuid.NewString()[:8], []string{"title"})
	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	mainBucket, ok := mainBucketForPropertyIndex(prop.Name, "filterable")
	require.True(t, ok)
	require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), mainBucket+"__reindex"+genSuffix(1)), 0o777))

	// Fails the loop's check before the second index type, after the first swept.
	failing := &errAfterCtx{Context: ctx, after: 1}
	var counts migrationSweepCounts
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	shard.updatePropertyBuckets(failing, eg, prop, &counts)
	require.Error(t, eg.Wait())
	require.Equal(t, int64(1), counts.dirsSwept.Load())
}
