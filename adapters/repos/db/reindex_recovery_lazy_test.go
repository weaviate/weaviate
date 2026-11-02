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
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery runs when a shard loads, not at boot; a shard that never loads costs nothing.
func TestRecoveryRunsWhenAShardLoadsNotAtBoot(t *testing.T) {
	const warm, cold = "tenant-warm", "tenant-cold"
	ctx := testCtx()
	className := "LazyRecovery_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	recovery := NewMigrationRecoveryReindexer(true, logger, nil, p)
	idx.shardReindexer = recovery

	desc := distributedtask.TaskDescriptor{ID: "T", Version: 3}
	subjects := map[string]MigrationSubject{}
	lazy := map[string]*LazyLoadShard{}
	for _, name := range []string{warm, cold} {
		subject := testMigrationSubject(desc.Version, StrategyCodeSearchableRetokenize, "title")
		subject.TaskID, subject.Collection = desc.ID, className
		subject.Key.UnitID = testMigrationUnitFor(idx, name)
		lsm := shardPathLSM(idx.path(), name)
		for _, dir := range []string{subject.Props["title"].Staged, subject.Props["title"].Sidecar} {
			require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
		}
		require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordIterated(subject)))
		subjects[name] = subject

		shard := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
		idx.shards.Store(name, shard)
		lazy[name] = shard
		t.Cleanup(func() {
			if shard.isLoaded() {
				require.NoError(t, shard.Shutdown(context.Background()))
			}
		})
	}

	require.NoError(t, lazy[warm].Load(ctx))
	require.NotZero(t, lazy[warm].shard.migrationMirrors.ArmedMigrationMirrors(),
		"a flip the next promotion acts on needs its writes mirrored from the first write")
	cached := p.cachedReindexTasks(desc, subjects[warm].Key.UnitID)
	require.Len(t, cached, 1, "the swap must reuse the task whose mirror is armed")
	reloaded := recovery.RecoverMigrations(lazy[warm].shard)
	require.Len(t, reloaded, 1)
	require.Same(t, cached[0], reloaded[0], "a reload must re-arm the instance the provider swaps")

	for _, name := range []string{warm, cold} {
		reread := NewMigrationRecordStore(shardPathLSM(idx.path(), name), logger)
		require.NoError(t, reread.Load())
		rec, ok := reread.Get(subjects[name].Key)
		require.True(t, ok)
		require.False(t, rec.Subject().Unmirrored,
			"%s: an armed mirror and a shard that never loaded both leave nothing unmirrored", name)
	}
	require.False(t, lazy[cold].isLoaded())
	require.Empty(t, p.cachedReindexTasks(desc, subjects[cold].Key.UnitID))
}

// Recovery's unmirrored stamp must land before the reconciler promotes a decided flip.
func TestShardLoadStampsAnUnarmableFlipBeforeTheReconcilerPromotesIt(t *testing.T) {
	const name = "tenant-unbuildable"
	ctx := testCtx()
	className := "LazyRecoveryStamp_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	// The writer accepts this record; recovery cannot build a task from it.
	subject.MigrationType, subject.TargetTokenization = ReindexTypeEnableSearchable, ""
	subject.TaskID, subject.Collection = "T", className
	subject.Key.UnitID = testMigrationUnitFor(idx, name)
	dirs := subject.Props["title"]
	lsm := shardPathLSM(idx.path(), name)
	for _, dir := range []string{dirs.Staged, dirs.Canonical} {
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
	}
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordSwapped(subject,
		subject.Properties(), map[string]string{"title": dirs.Canonical})))

	loaded, err := idx.initShard(ctx, name, class, nil, true, true)
	require.NoError(t, err)
	defer loaded.Shutdown(context.Background())

	rec, ok := loaded.(*Shard).migrationRecords.Get(subject.Key)
	require.True(t, ok)
	require.True(t, rec.Subject().Unmirrored)
	require.Equal(t, MigrationStateSwapped, rec.State(), "a stale staged copy is never promoted")
	require.DirExists(t, filepath.Join(lsm, dirs.Staged), "nothing renamed it over the canonical dir")
}

// The shard keeps serving when a mirror fails to arm; only the stale swap is blocked.
func TestShardLoadBlocksTheSwapOfAMigrationWhoseMirrorFailedToArm(t *testing.T) {
	const name = "tenant-arm-fails"
	ctx := testCtx()
	className := "LazyRecoveryArm_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)

	desc := distributedtask.TaskDescriptor{ID: "T", Version: 3}
	subject := testMigrationSubject(desc.Version, StrategyCodeSearchableRetokenize, "title")
	subject.TaskID, subject.Collection = desc.ID, className
	subject.Key.UnitID = testMigrationUnitFor(idx, name)
	built, err := buildRecoveryTasks(subject, name, logger, nil)
	require.NoError(t, err)
	task := built[0]
	subject.Props["title"] = MigrationPropertyDirs{
		Staged:    task.ingestBucketName("title"),
		Canonical: subject.Props["title"].Canonical,
		Sidecar:   task.reindexBucketName("title"),
	}
	lsm := shardPathLSM(idx.path(), name)
	staged := filepath.Join(lsm, subject.Props["title"].Staged)
	corrupt := filepath.Join(staged, "segment-1.db")
	require.NoError(t, os.MkdirAll(staged, 0o777))
	// Fails the arm the way a failing disk would.
	require.NoError(t, os.WriteFile(corrupt, []byte("not a segment"), 0o600))
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordMerged(subject)))

	loaded, err := idx.initShard(ctx, name, class, nil, true, true)
	require.NoError(t, err, "the shard loads despite the fault")
	shard := loaded.(*Shard)
	defer shard.Shutdown(context.Background())

	obj := createTestObjectWithText(className, "written after the load")
	require.NoError(t, shard.PutObject(ctx, obj))
	found, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, found, "and serves")

	rec, ok := shard.migrationRecords.Get(subject.Key)
	require.True(t, ok)
	require.True(t, rec.Subject().Unmirrored, "the write above is missing from the staged copy")
	require.Empty(t, p.cachedReindexTasks(desc, subject.Key.UnitID), "no swap may run the task that never armed")

	// Once the fault clears, the staged copy is openable but still stale.
	require.NoError(t, os.Remove(corrupt))
	require.ErrorContains(t, task.RunSwapOnShard(ctx, shard), "no double-write mirror armed")
	require.Equal(t, filepath.Base(shard.store.Bucket(subject.Props["title"].Canonical).GetDir()),
		subject.Props["title"].Canonical, "the canonical bucket still serves")
}
