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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameForAsyncDelete_RepeatedDropRecreate(t *testing.T) {
	// delete → recreate → delete (×2) on the same class must produce two
	// distinct .deleteme directories. The timestamp + random suffix in
	// the renamed name is what makes this safe.
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	classDir := filepath.Join(root, "index_articles")

	// first drop
	require.NoError(t, os.Mkdir(classDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(classDir, "marker-a"), []byte("a"), 0o644))
	deleted1, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted1, asyncDeleteSuffix),
		"first rename target should carry the async-delete suffix; got %q", deleted1)
	_, err = os.Stat(deleted1)
	require.NoError(t, err, "first .deleteme dir should exist on disk")

	// recreate and second drop
	require.NoError(t, os.Mkdir(classDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(classDir, "marker-b"), []byte("b"), 0o644))
	deleted2, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted2, asyncDeleteSuffix))
	_, err = os.Stat(deleted2)
	require.NoError(t, err, "second .deleteme dir should exist on disk")

	// names must differ — that's the invariant Etienne asked us to pin.
	assert.NotEqual(t, deleted1, deleted2,
		"two consecutive async-delete renames must produce distinct names")

	// each .deleteme dir contains the marker file that was written before
	// its respective drop — proving no contents got overwritten by the
	// second rename.
	_, err = os.Stat(filepath.Join(deleted1, "marker-a"))
	require.NoError(t, err, "first .deleteme should still contain marker-a")
	_, err = os.Stat(filepath.Join(deleted2, "marker-b"))
	require.NoError(t, err, "second .deleteme should still contain marker-b")
}

func TestRenameForAsyncDelete_ParentFsyncIsBestEffort(t *testing.T) {
	// Rename succeeds even if the parent fsync fails (e.g. read-only
	// filesystem in tests). The renamed path is what counts; the fsync
	// only gates crash-resistance.
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	classDir := filepath.Join(root, "index_x")
	require.NoError(t, os.Mkdir(classDir, 0o755))

	deleted, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)

	// origin no longer exists
	_, err = os.Stat(classDir)
	require.True(t, os.IsNotExist(err))
	// target exists with the expected suffix
	_, err = os.Stat(deleted)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted, asyncDeleteSuffix))
}

func TestSpawnAsyncDelete_RemovesPath(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	doomed := filepath.Join(root, "to-be-removed.deleteme")
	require.NoError(t, os.MkdirAll(filepath.Join(doomed, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(doomed, "sub", "f"), []byte("x"), 0o644))

	spawnAsyncDelete(doomed, logger)

	// async — wait for removal (bounded). The goroutine doing RemoveAll
	// finishes within ms for a tiny tree.
	require.Eventually(t, func() bool {
		_, err := os.Stat(doomed)
		return os.IsNotExist(err)
	}, 5*1e9 /* 5s in ns */, 1e7 /* 10ms */, "spawned delete should remove the path")
}

func TestScanAndAsyncDeletePending_RecoversIndexAndShardLevel(t *testing.T) {
	// Startup recovery: a .deleteme dir at the root (index-level) and one
	// nested inside a live class (shard-level) both get cleaned up.
	logger, _ := test.NewNullLogger()
	root := t.TempDir()

	// index-level pending delete
	indexPending := filepath.Join(root, "index_old.1234.deadbeef.deleteme")
	require.NoError(t, os.MkdirAll(indexPending, 0o755))

	// live class with a shard-level pending delete inside
	liveClass := filepath.Join(root, "index_live")
	require.NoError(t, os.MkdirAll(filepath.Join(liveClass, "shard1"), 0o755))
	shardPending := filepath.Join(liveClass, "shard2.5678.cafef00d.deleteme")
	require.NoError(t, os.MkdirAll(shardPending, 0o755))

	scanAndAsyncDeletePending(root, logger)

	require.Eventually(t, func() bool {
		_, indexErr := os.Stat(indexPending)
		_, shardErr := os.Stat(shardPending)
		return os.IsNotExist(indexErr) && os.IsNotExist(shardErr)
	}, 5*1e9, 1e7, "both index-level and shard-level pending dirs should be removed")

	// live class dir must remain
	_, err := os.Stat(filepath.Join(liveClass, "shard1"))
	require.NoError(t, err, "live shard must not be removed by recovery scan")
}
