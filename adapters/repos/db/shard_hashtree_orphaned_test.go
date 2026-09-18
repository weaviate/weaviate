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
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func TestShardInitDiscardsOrphanedHashtree(t *testing.T) {
	ctx := context.Background()
	const class = "OrphanedHashtreeOnInit"
	sl, idx := testShard(t, ctx, class, withAsyncScheduler(t))
	s := concreteShard(t, sl)
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	flushShard(t, ctx, sl)
	enableAndAwaitAsync(t, ctx, s)
	planted := hashtreeRoot(s)
	require.NoError(t, sl.Shutdown(ctx))
	require.Len(t, htFilesInDir(t, s.pathHashTree()), 1)
	idx.replicationConfigLock.Lock()
	idx.Config.ReplicationFactor = 2
	idx.Config.AsyncReplicationConfig = minAsyncReplicationConfig()
	idx.replicationConfigLock.Unlock()

	s2 := reopenShard(t, ctx, idx, s.name)
	require.Equal(t, planted, hashtreeRoot(s2), "control: an intact store loads the cached tree")
	require.NoError(t, s2.Shutdown(ctx))
	require.Len(t, htFilesInDir(t, s2.pathHashTree()), 1)

	require.NoError(t, os.RemoveAll(filepath.Join(shardPathLSM(idx.path(), s.name), helpers.ObjectsBucketLSM)))
	s3 := reopenShard(t, ctx, idx, s.name)
	require.NotEqual(t, planted, hashtreeRoot(s3))
	require.Empty(t, htFilesInDir(t, s3.pathHashTree()))
}

func reopenShard(t *testing.T, ctx context.Context, idx *Index, name string) *Shard {
	t.Helper()
	idx.shards.LoadAndDelete(name)
	sl, release, err := idx.getOrInitShard(ctx, name)
	require.NoError(t, err)
	release()
	s := concreteShard(t, sl)
	awaitHashtreeInitialized(t, s)
	return s
}

func hashtreeRoot(s *Shard) hashtree.Digest {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	return s.hashtree.Root()
}
