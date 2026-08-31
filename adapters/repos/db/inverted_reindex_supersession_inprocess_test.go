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

// TestBackToBackMigrationsRetireInProcess pins in-process retirement: a
// failed migration's mirror stays armed (tearing it down would route writes
// into a directory a restart deletes), and a second migration on the same
// property must retire it before its own flip, or the copy falls back onto
// the successor's live bucket once the stale staged name stops resolving.
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

	// The write path has to be intact afterwards: a mirror copy that fails
	// fails the user's write with it.
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

// TestTrimOlderGenerationsLeavesRecordOwnedDirsAlone pins that the trim
// leaves alone any directory a record still names (removing it would delete
// an open bucket's directory out from under a live mirror), and reclaims
// only what no record can attribute.
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
			// The records are the whole protection set here. One that does not
			// decode may be the one naming this directory, and "not in the set
			// I could read" is not "nobody owns it".
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
