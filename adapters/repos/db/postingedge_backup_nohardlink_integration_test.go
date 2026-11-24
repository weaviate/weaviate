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

package db

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	postingedgeent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// A backup without hardlinks, on a file system that has none, uploads the
// files of a live shard from where they are and never has its indexes copy
// what they change in place (backupShardWithoutHardlinks). A posting-edge
// index rewrites its meta, journal, slots and centroids in place, and lists
// only what it never rewrites: such a backup would hold an index without its
// meta, which does not open. The backup of a live shard with such an index
// is refused, with nothing halted; the shard goes on as it was.
func TestPostingEdge_BackupWithoutHardlinksIsRefused(t *testing.T) {
	const (
		class = "PostingEdgeNoHardlink"
		dims  = 32
	)
	ctx := context.Background()
	uc := postingedgeent.NewDefaultUserConfig()
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, uc, false, false,
		func(i *Index) { i.Config.PostingEdgeBaseSize = 150 })
	shard := underlyingShard(t, shd)
	r := rand.New(rand.NewSource(5))
	for _, err := range shard.PutObjectBatch(ctx, createRandomObjects(r, class, 300, dims)) {
		require.NoError(t, err)
	}
	dirs, err := filepath.Glob(filepath.Join(shard.path(), "*.postingedge.d"))
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dirs[0], "meta.json"))
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the index builds its base")

	_, err = idx.backupShardWithoutHardlinks(ctx, shard.name, nil, newReindexGate(idx.db, reindexGateMaxAge))
	require.Error(t, err, "a backup without hardlinks does not hold the files the index rewrites in place")
	require.ErrorContains(t, err, "hardlinks")
	require.False(t, shard.haltedForTransfer(), "the shard is not left halted")
	require.NoError(t, shard.PutObject(ctx, createRandomObjects(r, class, 1, dims)[0]), "the shard goes on")
}
