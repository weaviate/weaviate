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

// The shard keeps serving when a mirror fails to arm; the stale migration fails at prepare.
func TestShardLoadBlocksTheSwapOfAMigrationWhoseMirrorFailedToArm(t *testing.T) {
	for _, tt := range []struct {
		name   string
		record func(MigrationSubject) MigrationRecord
	}{
		{name: "merged", record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) }},
		{name: "iterated", record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			name := "tenant-arm-fails-" + tt.name
			ctx := testCtx()
			className := "LazyRecoveryArm_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			defer shd.Shutdown(context.Background())

			logger, _ := test.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
			idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)

			// Both halves of one unit; only the searchable one fails to arm.
			desc := distributedtask.TaskDescriptor{ID: "T", Version: 3}
			lsm := shardPathLSM(idx.path(), name)
			var tasks []*ShardReindexTaskGeneric
			var subjects []MigrationSubject
			for _, code := range []MigrationStrategyCode{StrategyCodeSearchableRetokenize, StrategyCodeFilterableRetokenize} {
				subject := testMigrationSubject(desc.Version, code, "title")
				subject.TaskID, subject.Collection = desc.ID, className
				subject.Key.UnitID = testMigrationUnitFor(idx, name)
				built, err := buildRecoveryTasks(subject, name, logger, nil)
				require.NoError(t, err)
				subject.Props["title"] = MigrationPropertyDirs{
					Staged:    built[0].ingestBucketName("title"),
					Canonical: subject.Props["title"].Canonical,
					Sidecar:   built[0].reindexBucketName("title"),
				}
				for _, dir := range []string{subject.Props["title"].Staged, subject.Props["title"].Sidecar} {
					require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
				}
				require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(tt.record(subject)))
				tasks = append(tasks, built[0])
				subjects = append(subjects, subject)
			}
			// Fails the arm the way a failing disk would.
			corrupt := filepath.Join(lsm, subjects[0].Props["title"].Staged, "segment-1.db")
			require.NoError(t, os.WriteFile(corrupt, []byte("not a segment"), 0o600))

			loaded, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err, "the shard loads despite the fault")
			shard := loaded.(*Shard)
			defer shard.Shutdown(context.Background())

			obj := createTestObjectWithText(className, "written after the load")
			require.NoError(t, shard.PutObject(ctx, obj))
			found, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
			require.NoError(t, err)
			require.NotNil(t, found, "and serves")

			rec, ok := shard.migrationRecords.Get(subjects[0].Key)
			require.True(t, ok)
			require.True(t, rec.Subject().Unmirrored, "the write above is missing from the staged copy")
			require.Empty(t, p.cachedReindexTasks(desc, subjects[0].Key.UnitID),
				"a cached sibling alone would swap half the unit")

			// Once the fault clears, the staged copy is openable but still stale.
			require.NoError(t, os.Remove(corrupt))
			prepOK, prep := p.runShardPrepPhase(ctx, subjects[0].Key.UnitID, shard, tasks, false, logger)
			require.False(t, prepOK, "a failed prepare fails the task before any replica swaps")
			require.Len(t, prep.Errs, 1)
			require.Contains(t, prep.Errs[0], "no double-write mirror armed")
			require.ErrorContains(t, tasks[0].RunSwapOnShard(ctx, shard), "no double-write mirror armed")
			for _, subject := range subjects {
				rec, ok := shard.migrationRecords.Get(subject.Key)
				require.True(t, ok)
				require.False(t, rec.FlipDecided(), "%s: nothing flipped", subject.Key.StrategyCode)
			}
			require.Equal(t, filepath.Base(shard.store.Bucket(subjects[0].Props["title"].Canonical).GetDir()),
				subjects[0].Props["title"].Canonical, "the canonical bucket still serves")
		})
	}
}

// With the stamp itself unwritable, nothing blocks the stale swap, so the shard must not load.
func TestShardLoadFailsWhenAMirrorFailsToArmAndItsStampCannotBeWritten(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the read-only records dir")
	}
	const name = "tenant-arm-and-stamp-fail"
	ctx := testCtx()
	className := "LazyRecoveryStampFail_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)

	subject := testMigrationSubject(3, StrategyCodeSearchableRetokenize, "title")
	subject.TaskID, subject.Collection = "T", className
	subject.Key.UnitID = testMigrationUnitFor(idx, name)
	built, err := buildRecoveryTasks(subject, name, logger, nil)
	require.NoError(t, err)
	subject.Props["title"] = MigrationPropertyDirs{
		Staged:    built[0].ingestBucketName("title"),
		Canonical: subject.Props["title"].Canonical,
		Sidecar:   built[0].reindexBucketName("title"),
	}
	lsm := shardPathLSM(idx.path(), name)
	staged := filepath.Join(lsm, subject.Props["title"].Staged)
	require.NoError(t, os.MkdirAll(staged, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(staged, "segment-1.db"), []byte("not a segment"), 0o600))
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordMerged(subject)))
	recordsDir := filepath.Join(lsm, migrationsDir, migrationRecordsDirName)
	require.NoError(t, os.Chmod(recordsDir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(recordsDir, 0o700)) })

	_, err = idx.initShard(ctx, name, class, nil, true, true)
	require.ErrorContains(t, err, "mirror stayed unarmed")
}
