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
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
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

// TestRestoreUpgradedLegacyDynamic pins the second bug the snapshot change closes.
// An upgraded LEGACY (TargetVector == "") dynamic index records upgraded=true only in
// the shard-level index.db; its init legacy branch never infers that state from the HNSW
// dir (unlike the target-vector branch). So restoring index.db loads as HNSW, but a missing
// index.db silently reverts to flat while only HNSW data exists.
func TestRestoreUpgradedLegacyDynamic(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	const dims = 16
	const n = 200

	vectors, _ := testinghelpers.RandomVecs(n, 0, dims)
	dp := distancer.NewL2SquaredProvider()
	noopCallback := cyclemanager.NewCallbackGroupNoop()

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}
	uc := ent.UserConfig{
		Threshold: uint64(n),
		Distance:  dp.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}

	rootPath := t.TempDir()
	indexID := "legacy-restore-test"
	makeConfig := func(root string, sharedDB *bbolt.DB) Config {
		return Config{
			AllocChecker:     memwatch.NewDummyMonitor(),
			RootPath:         root,
			ID:               indexID,
			TargetVector:     "", // legacy / default vector — the bug's trigger
			Logger:           logger,
			DistanceProvider: dp,
			MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
				return hnsw.NewCommitLogger(root, indexID, logger, noopCallback)
			},
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
			SharedDB:                     sharedDB,
			MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
			AsyncIndexingEnabled:         true,
		}
	}

	store := testinghelpers.NewDummyStore(t)

	// Build a legacy dynamic index, populate it, and upgrade to HNSW: this writes
	// upgraded=true into index.db and creates the HNSW commitlog dir.
	srcDBPath := filepath.Join(rootPath, "index.db")
	srcDB, err := bbolt.Open(srcDBPath, 0o666, nil)
	require.NoError(t, err)

	idx, err := New(makeConfig(rootPath, srcDB), uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)

	compressionhelpers.Concurrently(logger, uint64(n), func(i uint64) {
		require.NoError(t, idx.Add(ctx, i, vectors[i]))
	})

	wg := sync.WaitGroup{}
	wg.Add(1)
	require.NoError(t, idx.Upgrade(func() { wg.Done() }))
	wg.Wait()
	require.Equal(t, common.IndexType(common.IndexTypeHNSW), idx.UnderlyingIndex(), "precondition: upgraded to HNSW")

	// Snapshot index.db the way Shard.CreateBackupSnapshot does, then close the live index.
	backupDir := t.TempDir()
	backupDBPath := filepath.Join(backupDir, "index.db")
	require.NoError(t, srcDB.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(backupDBPath, 0o600)
	}))
	require.NoError(t, idx.Shutdown(ctx))
	require.NoError(t, srcDB.Close())

	t.Run("with restored index.db: loads as HNSW (fix)", func(t *testing.T) {
		restoreRoot := t.TempDir()
		require.NoError(t, copyFile(filepath.Join(restoreRoot, "index.db"), backupDBPath))
		db, err := bbolt.Open(filepath.Join(restoreRoot, "index.db"), 0o666, nil)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })

		restored, err := New(makeConfig(restoreRoot, db), uc, testinghelpers.NewDummyStore(t))
		require.NoError(t, err)
		t.Cleanup(func() { restored.Shutdown(ctx) })
		require.Equal(t, common.IndexType(common.IndexTypeHNSW), restored.UnderlyingIndex(),
			"restored legacy dynamic index must load as HNSW")
	})

	t.Run("without index.db: silently reverts to flat (the bug the fix closes)", func(t *testing.T) {
		bugRoot := t.TempDir()
		// fresh, empty index.db — as restore created before the gap was closed.
		db, err := bbolt.Open(filepath.Join(bugRoot, "index.db"), 0o666, nil)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })

		reverted, err := New(makeConfig(bugRoot, db), uc, testinghelpers.NewDummyStore(t))
		require.NoError(t, err)
		t.Cleanup(func() { reverted.Shutdown(ctx) })
		require.Equal(t, common.IndexType(common.IndexTypeFlat), reverted.UnderlyingIndex(),
			"without a restored index.db the legacy branch reverts to flat — this is why index.db must be backed up")
	})
}

func copyFile(dst, src string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
}
