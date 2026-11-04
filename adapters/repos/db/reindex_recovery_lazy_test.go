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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Recovery runs when a shard loads, not at boot, so a shard that never loads costs nothing.
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
	// The writer accepts this record, but recovery cannot build a task from it.
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

// The shard keeps serving when a mirror fails to arm, and the stale migration fails at prepare.
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

			// Both halves of one unit, of which only the searchable one fails to arm.
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
			// An unopenable segment fails the arm the way a failing disk would.
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

// A mirror that fails to arm while its stamp cannot be written holds the shard READONLY, which serves
// reads, refuses writes, and holds back the stale swap. An unreadable record refuses the stamp too.
func TestShardLoadWhenAMirrorFailsToArmAndItsStampCannotBeWritten(t *testing.T) {
	for _, tt := range []struct {
		name  string
		fault func(t *testing.T, fx *doubleFaultFixture)
	}{
		{name: "records dir read-only", fault: func(t *testing.T, fx *doubleFaultFixture) { fx.breakDisk(t) }},
		{name: "another record unreadable", fault: func(t *testing.T, fx *doubleFaultFixture) {
			plantUnreadableRecord(t, fx.recordsDir)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fx := newDoubleFaultFixture(t, "tenant-double-fault")
			tt.fault(t, fx)

			shard := fx.load(t)
			require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
			require.Equal(t, statusReasonMirrorUnarmed, shard.GetStatusReason())
			found, err := shard.ObjectByID(fx.ctx, fx.written.ID(), nil, additional.Properties{})
			require.NoError(t, err)
			require.NotNil(t, found, "reads are served")
			require.EqualError(t, shard.PutObject(fx.ctx, createTestObjectWithText(fx.className, "late")),
				storagestate.ErrStatusReadOnlyWithReason(statusReasonMirrorUnarmed).Error())

			fx.requirePhasesRefused(t, shard, errShardHeldReadOnly.Error(), true)

			// Only the lift's retry clears the hold. The status gate refuses a write that skips the retry.
			require.Error(t, shard.UpdateStatusIf(func(ShardStatus) bool { return true },
				storagestate.StatusReady.String(), statusReasonManualUpdate))
			require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
			// A step arms nothing either, even once the disk has healed.
			fx.healRecords(t)
			require.NoError(t, os.Remove(fx.corrupt))
			require.ErrorIs(t, fx.task.RunReindexOnlyOnShard(fx.ctx, shard), errShardHeldReadOnly)
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
		})
	}
}

// Every path that can lift READONLY first retries the recovery a double fault left undone.
func TestLiftingADoubleFaultReadOnlyShardRetriesRecovery(t *testing.T) {
	type lift func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error)
	lifts := []struct {
		name string
		lift lift
		// A path that only lifts its own read-only reason never lifts this one.
		neverLifts bool
		// The other disks add nothing the manual rows do not already cover.
		armsOnly bool
		want     storagestate.Status
	}{
		{name: "manual status update", lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusReady.String(), 0)
		}},
		{name: "manual indexing status", armsOnly: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusIndexing.String(), 0)
		}},
		{name: "manual shutdown status", armsOnly: true, want: storagestate.StatusShutdown, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusShutdown.String(), 0)
		}},
		{name: "disk pressure recovery", neverLifts: true, armsOnly: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			require.True(t, fx.idx.db.markShardReady(fx.name, shard))
			return shard, nil
		}},
		{name: "restart", armsOnly: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			require.NoError(t, shard.Shutdown(context.Background()))
			return fx.load(t), nil
		}},
	}
	disks := []struct {
		name string
		heal func(t *testing.T, fx *doubleFaultFixture)
		// "armed", "stamped", or "" when the disk still refuses both.
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
			if l.armsOnly && d.recovers != "armed" {
				continue
			}
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
					fx.requirePhasesRefused(t, shard, "no double-write mirror armed", false)
				default:
					require.ErrorContains(t, err, "cannot be stamped Unmirrored")
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					require.False(t, rec.Subject().Unmirrored)
					fx.requirePhasesRefused(t, shard, errShardHeldReadOnly.Error(), true)
				}
			})
		}
	}
}

// A lift whose retry opens a bucket and then fails leaves that bucket as READONLY as the shard.
func TestAFailedLiftLeavesTheBucketsItOpenedReadOnly(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-lift-buckets")
	lsm := shardPathLSM(fx.idx.path(), fx.name)
	require.NoError(t, NewMigrationRecordStore(lsm, fx.logger).Put(NewMigrationRecordIterated(fx.subject)))
	sidecar := filepath.Join(lsm, fx.subject.Props["title"].Sidecar)
	require.NoError(t, os.MkdirAll(sidecar, 0o777))
	require.NoError(t, os.Rename(fx.corrupt, filepath.Join(sidecar, "segment-1.db")))
	fx.breakDisk(t)
	shard := fx.load(t)

	// Now the sidecar opens and the staged bucket fails after it.
	require.NoError(t, os.Rename(filepath.Join(sidecar, "segment-1.db"), fx.corrupt))
	require.ErrorContains(t, shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate),
		"cannot be stamped Unmirrored")
	opened := shard.store.Bucket(fx.subject.Props["title"].Sidecar)
	require.NotNil(t, opened)
	require.Equal(t, storagestate.StatusReadOnly, opened.GetStatus())
}

// A lift that arms only some held migrations keeps holding just the others.
func TestAPartlyFailedLiftHoldsOnlyTheUnarmedMigrations(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-lift-partly")
	lsm := shardPathLSM(fx.idx.path(), fx.name)
	other := testMigrationSubject(fx.desc.Version, StrategyCodeFilterableRetokenize, "title")
	other.TaskID, other.Collection, other.Key.UnitID = fx.desc.ID, fx.className, fx.subject.Key.UnitID
	built, err := buildRecoveryTasks(other, fx.name, fx.logger, nil)
	require.NoError(t, err)
	other.Props["title"] = MigrationPropertyDirs{
		Staged: built[0].ingestBucketName("title"), Canonical: other.Props["title"].Canonical,
		Sidecar: built[0].reindexBucketName("title"),
	}
	staged := filepath.Join(lsm, other.Props["title"].Staged)
	require.NoError(t, os.MkdirAll(staged, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(staged, "segment-1.db"), []byte("not a segment"), 0o600))
	require.NoError(t, NewMigrationRecordStore(lsm, fx.logger).Put(NewMigrationRecordMerged(other)))
	fx.breakDisk(t)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	require.Len(t, shard.migrationMirrors.heldUnarmed(), 2)

	require.NoError(t, os.Remove(fx.corrupt))
	require.Error(t, shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate))
	held := shard.migrationMirrors.heldUnarmed()
	require.Len(t, held, 1)
	require.Equal(t, other.Key, held[0].migrationRecordKey())
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
	// Opening without blocking fails until the arm reads the pipe, so a regression fails instead of hanging.
	answer := func() bool {
		w, err := os.OpenFile(fx.corrupt, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return false
		}
		return w.Close() == nil
	}
	t.Cleanup(func() { answer() })

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
	require.Error(t, receiveWithin(t, lifts))
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	shutdown := make(chan error, 1)
	enterrors.GoWrapper(func() { shutdown <- shard.Shutdown(context.Background()) }, fx.logger)
	require.ErrorIs(t, receiveWithin(t, shutdown), errShardStillInUse, "the arm holds the shard")

	require.Eventually(t, answer, 10*time.Second, 10*time.Millisecond, "the arm parks on the segment")
	require.Eventually(t, shard.shut.Load, 10*time.Second, 10*time.Millisecond,
		"the shutdown completes once the arm ends")
}

// A lift that stops waiting must not end the arm. A stamp written because a caller gave up
// would fail a healthy migration for good.
func TestALiftThatStopsWaitingStillArmsTheMirror(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-lift-gives-up")
	reg := prometheus.NewRegistry()
	fx.idx.bucketLoadLimiter = loadlimiter.NewLoadLimiter(reg, "buckets", 1)
	fx.breakDisk(t)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	fx.healRecords(t)
	require.NoError(t, os.Remove(fx.corrupt))

	// A busy limiter parks the arm, and the caller stops waiting only once it is parked.
	require.NoError(t, fx.idx.bucketLoadLimiter.Acquire(fx.ctx))
	lift := make(chan error, 1)
	enterrors.GoWrapper(func() {
		lift <- shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate)
	}, fx.logger)
	require.Eventually(t, func() bool {
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, f := range families {
			if f.GetName() == "buckets_waiting_for_permit_to_load" {
				return f.GetMetric()[0].GetGauge().GetValue() == 1
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the arm parks on the limiter")
	shard.shutCtxCancel(errors.New("dropped"))
	require.Error(t, receiveWithin(t, lift))
	fx.idx.bucketLoadLimiter.Release()

	require.Eventually(t, func() bool { return len(shard.migrationMirrors.heldUnarmed()) == 0 },
		10*time.Second, 10*time.Millisecond, "the arm releases the hold")
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored, "a caller giving up must not stamp the migration")
	require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors())
	for name, b := range shard.store.GetBucketsByName() {
		require.Equalf(t, storagestate.StatusReadOnly, b.GetStatus(), "bucket %s opened after the caller left", name)
	}
}

// An arm that ends before the lift writes the status keeps the steps waiting
// until that write: the shard is still READONLY until then.
func TestAHeldStepWaitsUntilTheLiftWritesTheStatus(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-lift-late-arm")
	lsm := shardPathLSM(fx.idx.path(), fx.name)
	require.NoError(t, NewMigrationRecordStore(lsm, fx.logger).Put(NewMigrationRecordIterated(fx.subject)))
	fx.breakDisk(t)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	fx.healRecords(t)
	require.NoError(t, os.Remove(fx.corrupt))

	shard.statusLock.Lock()
	unlock := sync.OnceFunc(shard.statusLock.Unlock)
	t.Cleanup(unlock)
	lift := make(chan error, 1)
	enterrors.GoWrapper(func() {
		lift <- shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate)
	}, fx.logger)
	require.Eventually(t, func() bool { return len(shard.migrationMirrors.heldUnarmed()) == 0 },
		10*time.Second, 10*time.Millisecond, "the arm ends")
	fx.requirePhasesRefused(t, shard, errShardHeldReadOnly.Error(), true)
	unlock()

	require.NoError(t, receiveWithin(t, lift))
	require.Equal(t, storagestate.StatusReady, shard.GetStatus())
	fx.requireSwapProceeds(t, shard)
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

// A step that meets the hold waits instead of failing its migration, and the
// scheduler runs it again on a later tick once the shard is lifted.
func TestAHeldShardsMigrationResumesOnALaterTickAfterTheLift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status distributedtask.TaskStatus
		// The migration whose step meets the hold.
		payload func(fx *doubleFaultFixture) ReindexTaskPayload
		unit    distributedtask.UnitStatus
	}{
		{
			name: "prepare of the held migration", status: distributedtask.TaskStatusPreparing,
			unit:    distributedtask.UnitStatusCompleted,
			payload: func(fx *doubleFaultFixture) ReindexTaskPayload { return fx.subject.reindexPayload() },
		},
		{
			name: "repair unit on the held shard", status: distributedtask.TaskStatusStarted,
			unit: distributedtask.UnitStatusPending,
			payload: func(fx *doubleFaultFixture) ReindexTaskPayload {
				return ReindexTaskPayload{
					MigrationType: ReindexTypeRepairFilterable, Collection: fx.className, Properties: []string{"title"},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newDoubleFaultFixture(t, "tenant-held-step")
			fx.breakDisk(t)
			shard := fx.load(t)
			fx.idx.db.indexLock.Lock()
			fx.idx.db.indices[indexID(schema.ClassName(fx.className))] = fx.idx
			fx.idx.db.indexLock.Unlock()
			fx.provider.concurrency = func() int { return 1 }

			unitID, node := fx.subject.Key.UnitID, fx.idx.getSchema.NodeName()
			payload := tc.payload(fx)
			payload.UnitToShard, payload.UnitToNode = map[string]string{unitID: fx.name}, map[string]string{unitID: node}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: fx.desc, Payload: raw, Status: tc.status,
				NeedsPreparationBarrier: true,
				Units: map[string]*distributedtask.Unit{unitID: {
					ID: unitID, GroupID: fx.name, NodeID: node, Status: tc.unit,
				}},
			}
			outcome := &heldStepOutcome{task: task, done: make(chan string, 4)}
			sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
				CompletionRecorder: outcome, AckRecorder: outcome,
				TaskLister: outcome,
				Providers:  map[string]distributedtask.Provider{ReindexNamespace: fx.provider},
				Logger:     fx.logger, LocalNode: node, TickInterval: 20 * time.Millisecond,
			})
			require.NoError(t, sched.Start(fx.ctx))
			t.Cleanup(sched.Close)

			require.Never(t, func() bool { return len(outcome.done) > 0 }, time.Second, 20*time.Millisecond,
				"the step must wait while the shard is held")
			fx.healRecords(t)
			require.NoError(t, os.Remove(fx.corrupt))
			require.NoError(t, shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate))
			require.Equal(t, "succeeded", receiveWithin(t, outcome.done))
		})
	}
}

// heldStepOutcome reports the first verdict on the step: a prepare ack or a unit result.
type heldStepOutcome struct {
	task *distributedtask.Task
	done chan string
}

func (o *heldStepOutcome) verdict(ok bool, msg string) {
	if ok {
		msg = "succeeded"
	}
	o.done <- msg
}

func (o *heldStepOutcome) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{ReindexNamespace: {o.task}}, nil
}

func (o *heldStepOutcome) RecordDistributedTaskPreparationCompleteAck(_ context.Context, _, _ string, _ uint64, _ string, ok bool, msg string) error {
	o.verdict(ok, msg)
	return nil
}

func (o *heldStepOutcome) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, _, msg string) error {
	o.verdict(false, msg)
	return nil
}

func (o *heldStepOutcome) RecordDistributedTaskUnitCompletion(context.Context, string, string, uint64, string, string) error {
	o.verdict(true, "")
	return nil
}

func (o *heldStepOutcome) RecordDistributedTaskPostCompletionAck(context.Context, string, string, uint64, string, bool, string) error {
	return nil
}

func (o *heldStepOutcome) UpdateDistributedTaskUnitProgress(context.Context, string, string, uint64, string, string, float32) error {
	return nil
}

// Shards that load at once share one recovery reindexer, which must keep every unit.
func TestShardsLoadingAtOnceShareOneRecoveryReindexer(t *testing.T) {
	p, recovery := newBareRecovery()
	names := make([]string, 8)
	var wg sync.WaitGroup
	for i := range names {
		names[i] = fmt.Sprintf("tenant-%d", i)
		shard := bareShardWithMergedRecord(t, names[i])
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			recovery.RecoverMigrations(shard)
		}, recovery.logger)
	}
	wg.Wait()
	for _, name := range names {
		require.Len(t, p.cachedReindexTasks(bareRecoveryTask, MigrationUnitID(name, "node1")), 1, name)
	}
}

// A shard that loads after its migration ended still arms it, but must not
// cache its tasks again, because nothing would ever clear them.
func TestALoadAfterAMigrationEndedCachesNothing(t *testing.T) {
	p, recovery := newBareRecovery()
	require.Len(t, recovery.RecoverMigrations(bareShardWithMergedRecord(t, "tenant-a")), 1)
	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		TaskDescriptor: bareRecoveryTask, Status: distributedtask.TaskStatusCancelled,
	}))

	require.Len(t, recovery.RecoverMigrations(bareShardWithMergedRecord(t, "tenant-b")), 1, "still armed")
	require.Empty(t, p.cachedReindexTasks(bareRecoveryTask, MigrationUnitID("tenant-b", "node1")))
}

var bareRecoveryTask = distributedtask.TaskDescriptor{ID: "T", Version: 1}

func newBareRecovery() (*ReindexProvider, *MigrationRecoveryReindexer) {
	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(nil, nil, nil, logger, "node1", nil, testCtx())
	return p, NewMigrationRecoveryReindexer(true, logger, nil, p)
}

func bareShardWithMergedRecord(t *testing.T, name string) *Shard {
	logger, _ := test.NewNullLogger()
	subject := testMigrationSubject(bareRecoveryTask.Version, StrategyCodeSearchableRetokenize, "title")
	subject.TaskID, subject.Collection = bareRecoveryTask.ID, "Books"
	subject.Key.UnitID = MigrationUnitID(name, "node1")
	lsm := t.TempDir()
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordMerged(subject)))
	store := NewMigrationRecordStore(lsm, logger)
	require.NoError(t, store.Load())
	return &Shard{name: name, migrationRecords: store}
}

// A client that gives up on a lazy load must not stamp a healthy migration
// Unmirrored for good.
func TestACancelledShardLoadStillArmsTheMirror(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-cancelled-load")
	require.NoError(t, os.Remove(fx.corrupt))
	ctx, cancel := context.WithCancel(fx.ctx)
	// The client disconnects once every other load step is done.
	fx.idx.logger.(*logrus.Logger).AddHook(cancelOnLog{prefix: "Completed loading shard", cancel: cancel})

	shard, err := NewShard(ctx, nil, fx.name, fx.idx, fx.class, fx.idx.centralJobQueue, fx.idx.scheduler,
		fx.idx.indexCheckpoints, fx.idx.shardReindexer, false, fx.idx.bitmapBufPool, monitoring.ShardRegistrationEager)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	require.Error(t, ctx.Err())
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored)
	require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors())
}

type cancelOnLog struct {
	prefix string
	cancel context.CancelFunc
}

func (h cancelOnLog) Levels() []logrus.Level { return logrus.AllLevels }

func (h cancelOnLog) Fire(e *logrus.Entry) error {
	if strings.HasPrefix(e.Message, h.prefix) {
		h.cancel()
	}
	return nil
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

// waits says whether the scheduler retries the step on a later tick instead of failing the task.
func (fx *doubleFaultFixture) requirePhasesRefused(t *testing.T, shard *Shard, want string, waits bool) {
	tasks := []*ShardReindexTaskGeneric{fx.task}
	prepOK, prep := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, false, fx.logger)
	require.False(t, prepOK)
	require.Len(t, prep.Errs, 1)
	require.Contains(t, prep.Errs[0], want)
	require.Equal(t, waits, prep.Transient)
	if waits {
		rehydrateOK, rehydrate := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, true, fx.logger)
		require.False(t, rehydrateOK)
		require.Len(t, rehydrate.Errs, 1)
		require.Contains(t, rehydrate.Errs[0], errShardHeldReadOnly.Error())
		require.True(t, rehydrate.Transient)
	}
	payload := fx.subject.reindexPayload()
	swap := fx.provider.runShardSwapPhase(fx.ctx, &payload, fx.subject.Key.UnitID, fx.name, shard, tasks, fx.logger)
	require.NotEmpty(t, swap.Errs)
	require.Contains(t, swap.Errs[0], want)
	require.Equal(t, waits, swap.Transient)
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
