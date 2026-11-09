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

// TestShardInitDiscardsOrphanedHashtreeDespiteFileTrouble: the discard only unlinks, so a corrupt, undeletable or absent .ht still loads the shard.
func TestShardInitDiscardsOrphanedHashtreeDespiteFileTrouble(t *testing.T) {
	tests := []struct {
		name        string
		mangle      func(t *testing.T, dir string)
		seam        func(string) error
		wantDemoted bool
	}{
		{
			name: "corrupt snapshot",
			mangle: func(t *testing.T, dir string) {
				for _, e := range htFilesInDir(t, dir) {
					require.NoError(t, os.WriteFile(filepath.Join(dir, e.Name()), []byte("garbage"), 0o600))
				}
			},
		},
		{
			name: "truncated snapshot",
			mangle: func(t *testing.T, dir string) {
				for _, e := range htFilesInDir(t, dir) {
					require.NoError(t, os.Truncate(filepath.Join(dir, e.Name()), 3))
				}
			},
		},
		{
			name:   "no snapshot at all",
			mangle: func(t *testing.T, dir string) { require.NoError(t, os.RemoveAll(dir)) },
		},
		{
			name:        "undeletable snapshot",
			mangle:      func(t *testing.T, dir string) {},
			seam:        func(string) error { return os.ErrPermission },
			wantDemoted: true,
		},
		{
			name: "undeletable corrupt snapshot",
			mangle: func(t *testing.T, dir string) {
				for _, e := range htFilesInDir(t, dir) {
					require.NoError(t, os.WriteFile(filepath.Join(dir, e.Name()), []byte("garbage"), 0o600))
				}
			},
			seam:        func(string) error { return os.ErrPermission },
			wantDemoted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			class := "OrphanedHashtreeTrouble"
			sl, idx := testShard(t, ctx, class, withAsyncScheduler(t))
			s := concreteShard(t, sl)
			for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
			}
			flushShard(t, ctx, sl)
			enableAndAwaitAsync(t, ctx, s)
			planted := hashtreeRoot(s)
			require.NoError(t, sl.Shutdown(ctx))
			dir := s.pathHashTree()
			require.Len(t, htFilesInDir(t, dir), 1)

			idx.replicationConfigLock.Lock()
			idx.Config.ReplicationFactor = 2
			idx.Config.AsyncReplicationConfig = minAsyncReplicationConfig()
			idx.replicationConfigLock.Unlock()

			require.NoError(t, os.RemoveAll(filepath.Join(shardPathLSM(idx.path(), s.name), helpers.ObjectsBucketLSM)))
			tc.mangle(t, dir)

			if tc.seam != nil {
				prev := removeHashtreeFile
				removeHashtreeFile = tc.seam
				defer func() { removeHashtreeFile = prev }()
			}

			s2 := reopenShard(t, ctx, idx, s.name)
			require.NotEqual(t, planted, hashtreeRoot(s2), "the orphaned tree must never be served")
			require.Empty(t, htFilesInDir(t, dir), "no trustable .ht may survive the discard")

			if tc.wantDemoted {
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				var demoted int
				for _, e := range entries {
					if filepath.Ext(e.Name()) == ".tmp" {
						demoted++
					}
				}
				require.Equal(t, 1, demoted, "an undeletable .ht must demote to a stray .tmp, not fail the load")
			}
		})
	}
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
