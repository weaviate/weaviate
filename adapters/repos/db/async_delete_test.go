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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameForAsyncDelete_RepeatedDropRecreate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	classDir := filepath.Join(root, "index_articles")

	require.NoError(t, os.Mkdir(classDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(classDir, "marker-a"), []byte("a"), 0o644))
	deleted1, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted1, asyncDeleteSuffix))
	_, err = os.Stat(deleted1)
	require.NoError(t, err)

	require.NoError(t, os.Mkdir(classDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(classDir, "marker-b"), []byte("b"), 0o644))
	deleted2, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted2, asyncDeleteSuffix))
	_, err = os.Stat(deleted2)
	require.NoError(t, err)

	// the invariant Etienne asked us to pin
	assert.NotEqual(t, deleted1, deleted2,
		"two consecutive async-delete renames must produce distinct names")

	_, err = os.Stat(filepath.Join(deleted1, "marker-a"))
	require.NoError(t, err, "first .deleteme should still contain marker-a")
	_, err = os.Stat(filepath.Join(deleted2, "marker-b"))
	require.NoError(t, err, "second .deleteme should still contain marker-b")
}

func TestRenameForAsyncDelete_RenameInvariants(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	classDir := filepath.Join(root, "index_x")
	require.NoError(t, os.Mkdir(classDir, 0o755))

	deleted, err := renameForAsyncDelete(classDir, logger)
	require.NoError(t, err)

	_, err = os.Stat(classDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(deleted)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(deleted, asyncDeleteSuffix))
}

func TestRenameForAsyncDelete_MissingSourceIsNoOp(t *testing.T) {
	logger, _ := test.NewNullLogger()
	missing := filepath.Join(t.TempDir(), "never_existed")

	deleted, err := renameForAsyncDelete(missing, logger)
	require.NoError(t, err, "dropping an already-gone path must be a no-op, not an error")
	require.Empty(t, deleted, "no target to delete when there was nothing to rename")
}

func TestSpawnAsyncDelete_BoundedConcurrency(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	doomed := filepath.Join(root, "blocked.deleteme")
	require.NoError(t, os.Mkdir(doomed, 0o755))

	// Fill the semaphore so the next spawn must wait for a free slot.
	slots := cap(asyncDeleteSem)
	for i := 0; i < slots; i++ {
		asyncDeleteSem <- struct{}{}
	}

	spawnAsyncDelete(doomed, logger)

	require.Never(t, func() bool {
		_, err := os.Stat(doomed)
		return os.IsNotExist(err)
	}, 200*time.Millisecond, 20*time.Millisecond,
		"a spawn launched while the sem is full must not delete the path")

	// Release one slot; the blocked spawn should now complete.
	<-asyncDeleteSem
	require.Eventually(t, func() bool {
		_, err := os.Stat(doomed)
		return os.IsNotExist(err)
	}, 60*time.Second, 10*time.Millisecond,
		"deletion should proceed once a slot is freed")

	// Drain the remaining slots so other tests start clean.
	for i := 0; i < slots-1; i++ {
		<-asyncDeleteSem
	}
}

func TestSpawnAsyncDelete_RemovesPath(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	doomed := filepath.Join(root, "to-be-removed.deleteme")
	require.NoError(t, os.MkdirAll(filepath.Join(doomed, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(doomed, "sub", "f"), []byte("x"), 0o644))

	spawnAsyncDelete(doomed, logger)

	require.Eventually(t, func() bool {
		_, err := os.Stat(doomed)
		return os.IsNotExist(err)
	}, 60*time.Second, 10*time.Millisecond, "spawned delete should remove the path")
}

func TestScanAndAsyncDeletePending_RecoversIndexAndShardLevel(t *testing.T) {
	logger, _ := test.NewNullLogger()
	root := t.TempDir()

	indexPending := filepath.Join(root, "index_old.1234.deadbeef.deleteme")
	require.NoError(t, os.MkdirAll(indexPending, 0o755))

	liveClass := filepath.Join(root, "index_live")
	require.NoError(t, os.MkdirAll(filepath.Join(liveClass, "shard1"), 0o755))
	shardPending := filepath.Join(liveClass, "shard2.5678.cafef00d.deleteme")
	require.NoError(t, os.MkdirAll(shardPending, 0o755))

	scanAndAsyncDeletePending(root, logger)

	require.Eventually(t, func() bool {
		_, indexErr := os.Stat(indexPending)
		_, shardErr := os.Stat(shardPending)
		return os.IsNotExist(indexErr) && os.IsNotExist(shardErr)
	}, 60*time.Second, 10*time.Millisecond, "both index-level and shard-level pending dirs should be removed")

	_, err := os.Stat(filepath.Join(liveClass, "shard1"))
	require.NoError(t, err, "live shard must not be removed by recovery scan")
}
