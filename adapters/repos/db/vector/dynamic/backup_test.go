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

package dynamic

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestSnapshotMutableFiles_Dynamic covers the dynamic index's two roles in an active-shard
// backup: (1) SnapshotMutableFiles delegates to the underlying flat index, copying its meta.db;
// (2) the shard-level index.db, snapshotted via a bbolt read tx, is unaffected by an in-place
// write after the snapshot.
func TestSnapshotMutableFiles_Dynamic(t *testing.T) {
	ctx := context.Background()
	const dims = 8
	const n = 100

	sharedDBPath := filepath.Join(t.TempDir(), "index.db")
	db, err := bbolt.Open(sharedDBPath, 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	rootPath := t.TempDir()
	dp := distancer.NewL2SquaredProvider()
	vectors, _ := testinghelpers.RandomVecs(n, 0, dims)

	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}

	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              rootPath,
		ID:                    "dynamic-backup-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      dp,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           noopCallback,
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}, ent.UserConfig{
		Threshold: uint64(n),
		Distance:  dp.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}
	require.False(t, index.Upgraded())

	// (1) Delegation: while still flat, the underlying meta.db is snapshotted.
	staging := t.TempDir()
	relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
	require.NoError(t, err)
	require.Equal(t, []string{"meta.db"}, relPaths)
	stagedMeta := filepath.Join(staging, "meta.db")
	require.NotEqual(t, ino(t, filepath.Join(rootPath, "meta.db")), ino(t, stagedMeta),
		"delegated flat meta.db must be an independent copy")

	// (2) Snapshot index.db as Shard.CreateBackupSnapshot does (bbolt read tx), then mutate
	// it in place as the flat->hnsw upgrade would; the staged copy must keep the pre-snapshot state.
	stagedIndexDB := filepath.Join(staging, "index.db")
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(stagedIndexDB, 0o600)
	}))
	require.NotEqual(t, ino(t, sharedDBPath), ino(t, stagedIndexDB),
		"staged index.db must be an independent copy")

	stagedBeforeUpgraded := readUpgradedFlag(t, stagedIndexDB, index.dbKey())
	require.False(t, stagedBeforeUpgraded, "precondition: not yet upgraded at snapshot time")

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		return b.Put(index.dbKey(), []byte{1})
	}))

	require.False(t, readUpgradedFlag(t, stagedIndexDB, index.dbKey()),
		"staged index.db must be unchanged by the post-snapshot in-place write")
}

// readUpgradedFlag reports the dynamic upgraded flag for key in a staged index.db copy.
func readUpgradedFlag(t *testing.T, path string, key []byte) bool {
	t.Helper()
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer db.Close()

	var upgraded bool
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		if b == nil {
			return nil
		}
		v := b.Get(key)
		upgraded = len(v) > 0 && v[0] != 0
		return nil
	}))
	return upgraded
}

func ino(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}
