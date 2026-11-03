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
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
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
			// The task can already be past its prepare, as when the node restarts mid-swap.
			payload := subjects[0].reindexPayload()
			swap := p.runShardSwapPhase(ctx, &payload, subjects[0].Key.UnitID, name, shard, tasks, logger)
			require.NotEmpty(t, swap.Errs)
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

// An unreadable record makes the store refuse the stamp, and a later load would
// rebuild the task from a fresh store, so the shard is held READONLY.
func TestShardLoadWhenAMirrorFailsToArmAndAnotherRecordIsUnreadable(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-arm-fails-unreadable")
	plantUnreadableRecord(t, fx.recordsDir)

	shard := fx.load(t)
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.Equal(t, statusReasonMirrorUnarmed, shard.GetStatusReason())
	require.EqualError(t, shard.PutObject(fx.ctx, createTestObjectWithText(fx.className, "late")),
		storagestate.ErrStatusReadOnlyWithReason(statusReasonMirrorUnarmed).Error())
	fx.requirePhasesRefused(t, shard, "is READONLY because its double-write mirror")
}

// A mirror that fails to arm while its stamp cannot be written holds the shard
// READONLY: it serves reads, refuses writes, and refuses the stale swap.
func TestShardLoadWhenAMirrorFailsToArmAndItsStampCannotBeWritten(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-double-fault")
	fx.breakDisk(t)

	shard := fx.load(t)
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.Equal(t, statusReasonMirrorUnarmed, shard.GetStatusReason())
	found, err := shard.ObjectByID(fx.ctx, fx.written.ID(), nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, found, "reads are served")
	require.EqualError(t, shard.PutObject(fx.ctx, createTestObjectWithText(fx.className, "late")),
		storagestate.ErrStatusReadOnlyWithReason(statusReasonMirrorUnarmed).Error())

	fx.requirePhasesRefused(t, shard, "is READONLY because its double-write mirror")
}

// Every path that can lift READONLY first retries the recovery a double fault left undone.
func TestLiftingADoubleFaultReadOnlyShardRetriesRecovery(t *testing.T) {
	type lift func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error)
	lifts := []struct {
		name string
		lift lift
		// A path that only lifts its own read-only reason never lifts this one.
		neverLifts bool
		want       storagestate.Status
	}{
		{name: "manual status update", lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusReady.String(), 0)
		}},
		{name: "manual shutdown status", want: storagestate.StatusShutdown, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusShutdown.String(), 0)
		}},
		{name: "vector index config change", lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonVectorIndexUpdate)
		}},
		{name: "disk pressure recovery", neverLifts: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			require.True(t, fx.idx.db.markShardReady(fx.name, shard))
			return shard, nil
		}},
		{name: "restart", lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			require.NoError(t, shard.Shutdown(context.Background()))
			return fx.load(t), nil
		}},
	}
	disks := []struct {
		name string
		heal func(t *testing.T, fx *doubleFaultFixture)
		// What the retry gets done: "armed", "stamped", or "" when the disk still refuses.
		recovers string
	}{
		{name: "mirror arms", recovers: "armed", heal: func(t *testing.T, fx *doubleFaultFixture) {
			fx.healRecords(t)
			require.NoError(t, os.Remove(fx.corrupt))
		}},
		{name: "only the stamp lands", recovers: "stamped", heal: func(t *testing.T, fx *doubleFaultFixture) {
			fx.healRecords(t)
		}},
		{name: "disk still broken", heal: func(*testing.T, *doubleFaultFixture) {}},
	}
	for _, l := range lifts {
		for _, d := range disks {
			t.Run(l.name+"/"+d.name, func(t *testing.T) {
				fx := newDoubleFaultFixture(t, "tenant-lift")
				fx.breakDisk(t)
				shard := fx.load(t)
				require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())

				d.heal(t, fx)
				shard, err := l.lift(t, fx, shard)
				t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })

				rec, ok := shard.migrationRecords.Get(fx.subject.Key)
				require.True(t, ok)
				want := l.want
				if want == "" {
					want = storagestate.StatusReady
				}
				switch {
				case l.neverLifts:
					require.NoError(t, err)
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					require.Equal(t, statusReasonMirrorUnarmed, shard.GetStatusReason())
				case d.recovers == "armed":
					require.NoError(t, err)
					require.Equal(t, want, shard.GetStatus())
					require.False(t, rec.Subject().Unmirrored)
					require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors())
					fx.requireSwapProceeds(t, shard)
				case d.recovers == "stamped":
					require.NoError(t, err)
					require.Equal(t, want, shard.GetStatus())
					require.True(t, rec.Subject().Unmirrored)
					fx.requirePhasesRefused(t, shard, "no double-write mirror armed")
				default:
					if l.name != "restart" {
						require.ErrorContains(t, err, "the disk is not accepting writes")
					}
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					require.False(t, rec.Subject().Unmirrored)
					fx.requirePhasesRefused(t, shard, "is READONLY because its double-write mirror")
				}
			})
		}
	}
}

// A lift waits on the disk outside the status lock, so a stuck disk call stalls
// neither status reads nor a second lift, and the wait ends with the shard.
func TestLiftingAHeldShardDoesNotWaitOnAStuckDisk(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-stuck-disk")
	fx.breakDisk(t)
	shard := fx.load(t)
	// Opening a pipe with no writer blocks in the kernel, like a hung network volume.
	require.NoError(t, os.Remove(fx.corrupt))
	require.NoError(t, syscall.Mkfifo(fx.corrupt, 0o600))
	t.Cleanup(func() {
		w, err := os.OpenFile(fx.corrupt, os.O_WRONLY, 0)
		require.NoError(t, err)
		require.NoError(t, w.Close())
		require.Eventually(t, func() bool {
			if !shard.liftRetry.TryLock() {
				return false
			}
			shard.liftRetry.Unlock()
			return true
		}, 10*time.Second, 10*time.Millisecond, "the retry finishes once the disk answers")
	})

	lifts := make(chan error, 2)
	for range 2 {
		enterrors.GoWrapper(func() {
			lifts <- shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate)
		}, fx.logger)
	}
	require.ErrorContains(t, receiveWithin(t, lifts), "a recovery retry is already running")
	status := make(chan storagestate.Status, 1)
	enterrors.GoWrapper(func() { status <- shard.GetStatus() }, fx.logger)
	require.Equal(t, storagestate.StatusReadOnly, receiveWithin(t, status))

	shard.shutCtxCancel(errors.New("dropped"))
	require.ErrorIs(t, receiveWithin(t, lifts), context.Canceled)
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
}

func receiveWithin[T any](t *testing.T, ch <-chan T) T {
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("blocked on the stuck disk")
		var zero T
		return zero
	}
}

// doubleFaultFixture is a shard holding one written object and one merged
// migration whose staged segment cannot be opened, so its mirror fails to arm.
type doubleFaultFixture struct {
	ctx        context.Context
	name       string
	className  string
	class      *models.Class
	idx        *Index
	provider   *ReindexProvider
	logger     logrus.FieldLogger
	desc       distributedtask.TaskDescriptor
	subject    MigrationSubject
	task       *ShardReindexTaskGeneric
	written    *storobj.Object
	corrupt    string
	recordsDir string
}

func newDoubleFaultFixture(t *testing.T, name string) *doubleFaultFixture {
	ctx := testCtx()
	className := "LazyRecoveryDoubleFault_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	t.Cleanup(func() { _ = shd.Shutdown(context.Background()) })

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)
	fx := &doubleFaultFixture{
		ctx: ctx, name: name, className: className, class: class, idx: idx, provider: p, logger: logger,
		desc: distributedtask.TaskDescriptor{ID: "T", Version: 3},
	}

	shard := fx.load(t)
	fx.written = createTestObjectWithText(className, "written before the fault")
	require.NoError(t, shard.PutObject(ctx, fx.written))
	require.NoError(t, shard.Shutdown(context.Background()))

	fx.subject = testMigrationSubject(fx.desc.Version, StrategyCodeSearchableRetokenize, "title")
	fx.subject.TaskID, fx.subject.Collection = fx.desc.ID, className
	fx.subject.Key.UnitID = testMigrationUnitFor(idx, name)
	built, err := buildRecoveryTasks(fx.subject, name, logger, nil)
	require.NoError(t, err)
	fx.task = built[0]
	fx.subject.Props["title"] = MigrationPropertyDirs{
		Staged:    fx.task.ingestBucketName("title"),
		Canonical: fx.subject.Props["title"].Canonical,
		Sidecar:   fx.task.reindexBucketName("title"),
	}
	lsm := shardPathLSM(idx.path(), name)
	staged := filepath.Join(lsm, fx.subject.Props["title"].Staged)
	require.NoError(t, os.MkdirAll(staged, 0o777))
	fx.corrupt = filepath.Join(staged, "segment-1.db")
	require.NoError(t, os.WriteFile(fx.corrupt, []byte("not a segment"), 0o600))
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordMerged(fx.subject)))
	fx.recordsDir = filepath.Join(lsm, migrationsDir, migrationRecordsDirName)
	return fx
}

func (fx *doubleFaultFixture) load(t *testing.T) *Shard {
	loaded, err := fx.idx.initShard(fx.ctx, fx.name, fx.class, nil, true, true)
	require.NoError(t, err)
	fx.idx.shards.Store(fx.name, loaded)
	return loaded.(*Shard)
}

// breakDisk makes the records dir refuse the unmirrored stamp.
func (fx *doubleFaultFixture) breakDisk(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the read-only records dir")
	}
	require.NoError(t, os.Chmod(fx.recordsDir, 0o500))
	t.Cleanup(func() { fx.healRecords(t) })
}

func (fx *doubleFaultFixture) healRecords(t *testing.T) {
	require.NoError(t, os.Chmod(fx.recordsDir, 0o700))
}

func (fx *doubleFaultFixture) requirePhasesRefused(t *testing.T, shard *Shard, want string) {
	tasks := []*ShardReindexTaskGeneric{fx.task}
	prepOK, prep := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, false, fx.logger)
	require.False(t, prepOK)
	require.Len(t, prep.Errs, 1)
	require.Contains(t, prep.Errs[0], want)
	payload := fx.subject.reindexPayload()
	swap := fx.provider.runShardSwapPhase(fx.ctx, &payload, fx.subject.Key.UnitID, fx.name, shard, tasks, fx.logger)
	require.NotEmpty(t, swap.Errs)
	require.Contains(t, swap.Errs[0], want)
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.FlipDecided(), "nothing flipped")
}

func (fx *doubleFaultFixture) requireSwapProceeds(t *testing.T, shard *Shard) {
	tasks := fx.provider.cachedReindexTasks(fx.desc, fx.subject.Key.UnitID)
	require.Len(t, tasks, 1, "the swap reuses the task whose mirror is armed")
	prepOK, prep := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, false, fx.logger)
	require.True(t, prepOK, "%v", prep.Errs)
	payload := fx.subject.reindexPayload()
	swap := fx.provider.runShardSwapPhase(fx.ctx, &payload, fx.subject.Key.UnitID, fx.name, shard, tasks, fx.logger)
	require.Empty(t, swap.Errs)
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.True(t, rec.FlipDecided())
}
