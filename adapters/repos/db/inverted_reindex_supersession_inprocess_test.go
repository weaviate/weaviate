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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestBackToBackMigrationsRetireInProcess(t *testing.T) {
	const propName = "title"

	ctx := testCtx()
	className := "BackToBackRetire_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	for _, obj := range makeConvergenceTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	failed, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 1, propName)
	failed.processOneSwapPropFn = func(context.Context, *lsmkv.Store, int, string) (*lsmkv.Bucket, error) {
		return nil, errors.New("injected swap failure")
	}
	require.NoError(t, failed.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, failed.RunPrepareOnShard(ctx, shard))
	require.Error(t, failed.RunSwapOnShard(ctx, shard))

	registry := shard.migrationMirrorRegistry()
	require.Equal(t, 1, registry.ArmedMigrationMirrors(),
		"a swap error must leave the mirror armed: the restart promotion that follows removes the "+
			"only other copy of every write made after the error")
	failedStaged := failed.ingestBucketName(propName)
	require.DirExists(t, filepath.Join(shard.pathLSM(), failedStaged))

	successor, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 2, propName)
	require.NoError(t, successor.RunReindexOnlyOnShard(ctx, shard))
	require.Equal(t, 2, registry.ArmedMigrationMirrors(),
		"two mirrors on one property are a steady state until the older one is superseded")

	require.NoError(t, successor.RunPrepareOnShard(ctx, shard))
	require.NoError(t, successor.RunSwapOnShard(ctx, shard))

	require.Zero(t, registry.ArmedMigrationMirrors(),
		"the successor's flip retires the predecessor's mirror and disarms its own on completion")
	_, stillRecorded := shard.migrationRecords.Get(failed.migrationRecordKey())
	require.False(t, stillRecorded, "a fully superseded record has nothing left to answer for")
	require.NoDirExists(t, filepath.Join(shard.pathLSM(), failedStaged),
		"the retired migration's staged directory goes with its record")
	require.Nil(t, shard.store.Bucket(failedStaged),
		"and its bucket is shut down before the directory is removed")

	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "zulu")))

	live := shard.store.Bucket(successor.strategy.SourceBucketName(propName))
	require.NotNil(t, live, "the successor's staged bucket is the live one from its flip on")
	bm, release, err := live.RoaringSetGet(ctx, []byte("alpha"))
	require.NoError(t, err)
	defer release()
	require.NotNil(t, bm)
	require.NotEmpty(t, bm.ToArray(),
		"retiring the predecessor must not reach the successor's live data")
}

func TestTrimOlderGenerationsLeavesRecordOwnedDirsAlone(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name       string
		keepRecord bool
		unreadable bool
		wantDir    bool
	}{
		{
			name:       "a record still names the older migration's staged directory",
			keepRecord: true,
			wantDir:    true,
		},
		{
			name: "no record names it, so nothing else will ever reclaim it",
		},
		{
			name:       "a record that does not decode withholds the whole trim",
			unreadable: true,
			wantDir:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "TrimRecordAware_" + uuid.NewString()[:8]
			class := newEnableFilterableTestClass(className, propName)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			older, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 1, propName)
			staged := older.ingestBucketName(propName)
			tracker := older.strategy.MigrationDirName()
			require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), staged), 0o777))
			require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), migrationsDir, tracker), 0o777))

			if tt.keepRecord {
				subject := older.migrationSubject(shard, []string{propName}, time.Now())
				require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
			}
			if tt.unreadable {
				require.NoError(t, os.MkdirAll(shard.migrationRecords.Dir(), 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(shard.migrationRecords.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
				require.NoError(t, shard.migrationRecords.Load())
			}

			logger, _ := test.NewNullLogger()
			newer, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 2, propName)
			newer.trimOlderGenerationsLocked(logger, shard, []string{propName})

			require.Equal(t, tt.wantDir, dirExists(t, filepath.Join(shard.pathLSM(), staged)),
				"staged directory of the older migration")
			require.Equal(t, tt.wantDir, dirExists(t, filepath.Join(shard.pathLSM(), migrationsDir, tracker)),
				"tracker directory of the older migration")
		})
	}
}

// A swap removes the directory it displaced, which on a back-to-back migration
// is the predecessor's staged directory holding its only migrated copy. When
// the successor covers just some of the predecessor's properties, retirement
// leaves that record standing, so the swap has to leave what it still names.
func TestASwapLeavesADisplacedDirAnotherRecordStillNames(t *testing.T) {
	ctx := testCtx()
	className := "SwapKeepsHeldDir_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, "title", "body")

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	for _, obj := range makeConvergenceTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	predecessor, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 1, "title", "body")
	require.NoError(t, predecessor.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, predecessor.RunPrepareOnShard(ctx, shard))
	require.NoError(t, predecessor.RunSwapOnShard(ctx, shard))

	held := predecessor.ingestBucketName("title")
	require.DirExists(t, filepath.Join(shard.pathLSM(), held),
		"the flip leaves the migrated data at the staged name until a load promotes it")

	// Only "title" is superseded, so the predecessor keeps its record and goes
	// on naming both staged directories.
	successor, _ := newEnableFilterableTaskAtGeneration(t, idx, className, 2, "title")
	require.NoError(t, successor.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, successor.RunPrepareOnShard(ctx, shard))
	require.NoError(t, successor.RunSwapOnShard(ctx, shard))

	rec, stillRecorded := shard.migrationRecords.Get(predecessor.migrationRecordKey())
	require.True(t, stillRecorded,
		"a record whose properties are only partly superseded is not retired")
	require.Equal(t, held, rec.Subject().StagedDirs["title"])

	require.DirExists(t, filepath.Join(shard.pathLSM(), held),
		"the successor's flip must not remove the directory the predecessor names as its staged data")
}
