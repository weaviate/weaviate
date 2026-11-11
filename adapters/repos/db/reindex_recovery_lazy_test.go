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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// A shard load arms its in-flight migration and caches the task, and a reload reuses that task.
func TestAShardLoadArmsAndCachesItsMigration(t *testing.T) {
	const name = "tenant-warm"
	ctx := testCtx()
	className := "LazyRecovery_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	recovery := NewMigrationRecoveryReindexer(logger, nil, p)
	idx.shardReindexer = recovery

	desc := distributedtask.TaskDescriptor{ID: "T", Version: 3}
	subject := testMigrationSubject(desc.Version, StrategyCodeSearchableRetokenize, "title")
	subject.TaskID, subject.Collection = desc.ID, className
	subject.Key.UnitID = testMigrationUnitFor(idx, name)
	lsm := shardPathLSM(idx.path(), name)
	for _, dir := range []string{subject.Props["title"].Staged, subject.Props["title"].Sidecar} {
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
	}
	require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordIterated(subject)))

	lazy := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(name, lazy)
	t.Cleanup(func() { require.NoError(t, lazy.Shutdown(context.Background())) })

	require.NoError(t, lazy.Load(ctx))
	require.NotZero(t, lazy.shard.migrationMirrors.ArmedMigrationMirrors(),
		"a flip the next promotion acts on needs its writes mirrored from the first write")
	cached := p.cachedReindexTasks(desc, subject.Key.UnitID)
	require.Len(t, cached, 1, "the swap must reuse the task whose mirror is armed")
	reloaded := recovery.RecoverMigrations(lazy.shard)
	require.Len(t, reloaded, 1)
	require.Same(t, cached[0], reloaded[0], "a reload must re-arm the instance the provider swaps")

	reread := NewMigrationRecordStore(lsm, logger)
	require.NoError(t, reread.Load())
	rec, ok := reread.Get(subject.Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored, "an armed mirror leaves nothing unmirrored")
}

// Recovery stamps an unbuildable decided flip before the load reconciler runs, so the staged copy is not promoted.
func TestShardLoadStampsAnUnarmableFlipBeforeTheReconcilerPromotesIt(t *testing.T) {
	const name = "tenant-unbuildable"
	ctx := testCtx()
	className := "LazyRecoveryStamp_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(logger, nil, p)

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
			idx.shardReindexer = NewMigrationRecoveryReindexer(logger, nil, p)
			installCaughtUpTaskList(ctx, idx.db)

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

			for _, subject := range subjects {
				rec, ok := shard.migrationRecords.Get(subject.Key)
				require.True(t, ok)
				require.True(t, rec.Subject().Unmirrored, "%s: the write above is missing from the staged copy", subject.Key.StrategyCode)
			}
			require.Empty(t, p.cachedReindexTasks(desc, subjects[0].Key.UnitID),
				"a cached sibling alone would swap half the unit")

			// Once the fault clears, the staged copy is openable but still stale.
			require.NoError(t, os.Remove(corrupt))
			prepOK, prep := p.runShardPrepPhase(ctx, subjects[0].Key.UnitID, shard, tasks, false, logger)
			require.False(t, prepOK, "a failed prepare fails the task before any replica swaps")
			require.Len(t, prep.Errs, len(tasks), "both halves are stamped")
			for _, e := range prep.Errs {
				require.Contains(t, e, "no double-write mirror armed")
			}
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
		// unitFails says a unit fails its migration instead of waiting on the hold.
		unitFails bool
	}{
		{name: "records dir read-only", fault: func(t *testing.T, fx *doubleFaultFixture) { fx.breakDisk(t) }},
		{name: "another record unreadable", unitFails: true, fault: func(t *testing.T, fx *doubleFaultFixture) {
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

			fx.requireStepsWait(t)
			unitID := fx.subject.Key.UnitID
			repair := ReindexTaskPayload{
				MigrationType: ReindexTypeRepairFilterable, Collection: fx.className,
				Properties: []string{"title"}, UnitToShard: map[string]string{unitID: fx.name},
			}
			outcome := &heldStepOutcome{done: make(chan string, 1)}
			fx.provider.processOneUnit(fx.ctx, &distributedtask.Task{TaskDescriptor: fx.desc}, &repair, fx.idx, unitID, outcome)
			require.Equal(t, tt.unitFails, len(outcome.done) == 1)

			// Only the lift's retry clears the hold. The status gate refuses a write that skips the retry.
			require.Error(t, shard.UpdateStatusIf(func(ShardStatus) bool { return true },
				storagestate.StatusReady.String(), statusReasonManualUpdate))
			require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
		})
	}
}

// Every path that can lift READONLY first retries the recovery a double fault left undone.
func TestLiftingADoubleFaultReadOnlyShardRetriesRecovery(t *testing.T) {
	type lift func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error)
	lifts := []struct {
		name string
		lift lift
		// A status write starts the recovery retry and is refused. A retry that works lifts the shard.
		retries bool
		// Only the manual status update row runs the disks that do not arm.
		armsOnly bool
	}{
		{name: "manual status update", retries: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusReady.String(), 0)
		}},
		{name: "manual shutdown status", retries: true, armsOnly: true, lift: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) (*Shard, error) {
			return shard, fx.idx.IncomingUpdateShardStatus(fx.ctx, fx.name, storagestate.StatusShutdown.String(), 0)
		}},
		// The only row that asserts a held shard's Shutdown returns no error; the other rows discard it in cleanup.
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
				lift := func() (err error) {
					shard, err = l.lift(t, fx, shard)
					return err
				}
				var err error
				if l.retries {
					err = liftAndAwaitRetry(t, fx, shard, lift)
				} else {
					err = lift()
				}
				t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })

				rec, ok := shard.migrationRecords.Get(fx.subject.Key)
				require.True(t, ok)
				switch d.recovers {
				case "armed":
					require.NoError(t, err)
					require.Equal(t, storagestate.StatusReady, shard.GetStatus())
					require.False(t, rec.Subject().Unmirrored)
					require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors())
					fx.requireSwapProceeds(t, shard)
				case "stamped":
					require.NoError(t, err)
					require.Equal(t, storagestate.StatusReady, shard.GetStatus())
					require.True(t, rec.Subject().Unmirrored)
					fx.requirePhasesRefused(t, shard, "no double-write mirror armed")
				default:
					require.ErrorContains(t, err, "stamp Unmirrored the migrations whose mirror failed to arm")
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					require.False(t, rec.Subject().Unmirrored)
					fx.requireStepsWait(t)
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
	require.ErrorContains(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)),
		"stamp Unmirrored the migrations whose mirror failed to arm")
	opened := shard.store.Bucket(fx.subject.Props["title"].Sidecar)
	require.NotNil(t, opened)
	require.Equal(t, storagestate.StatusReadOnly, opened.GetStatus())
}

// A lift that arms or stamps only some held migrations keeps holding just the others.
func TestAPartlyFailedLiftHoldsOnlyTheUnarmedMigrations(t *testing.T) {
	for _, tt := range []struct {
		name string
		heal func(t *testing.T, fx *doubleFaultFixture, shard *Shard, other MigrationSubject)
	}{
		{name: "one mirror arms", heal: func(t *testing.T, fx *doubleFaultFixture, _ *Shard, _ MigrationSubject) {
			require.NoError(t, os.Remove(fx.corrupt))
		}},
		{name: "one stamp lands", heal: func(t *testing.T, fx *doubleFaultFixture, shard *Shard, other MigrationSubject) {
			fx.healRecords(t)
			// A directory at the other record's path fails only that rewrite.
			path := shard.migrationRecords.path(other.Key)
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.Mkdir(path, 0o700))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fx := newDoubleFaultFixture(t, "tenant-lift-partly")
			other, _ := fx.addHalf(t, StrategyCodeFilterableRetokenize, "U", fx.desc.Version+1, true)
			fx.breakDisk(t)
			logs := test.NewLocal(fx.idx.logger.(*logrus.Logger))
			shard := fx.load(t)
			t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
			require.Len(t, shard.migrationMirrors.heldUnarmed(), 2)
			holdLines := entriesAbout(logs, "its reindex migrations wait")
			require.Len(t, holdLines, 1, "one Error line per hold, naming the shard")
			require.Equal(t, logrus.ErrorLevel, holdLines[0].Level)
			require.Equal(t, shard.ID(), holdLines[0].Data["shard"])

			tt.heal(t, fx, shard, other)
			require.Error(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)))
			held := shard.migrationMirrors.heldUnarmed()
			require.Len(t, held, 1)
			require.Equal(t, other.Key, held[0].migrationRecordKey())
			unseal, sealed := fx.provider.SealLocalUnit(fx.desc, fx.subject.Key.UnitID)
			require.True(t, sealed, "the lift released every unit claim it took")
			unseal()
		})
	}
}

// The swap checks every half of a unit first, so a prepared half is not swapped beside a stamped one.
func TestASwapRefusesTheHealthyHalfOfAStampedMigration(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-swap-stamped-half")
	half, _ := fx.addHalf(t, StrategyCodeFilterableRetokenize, fx.desc.ID, fx.desc.Version, false)
	require.NoError(t, os.Remove(fx.corrupt))
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	tasks := fx.provider.cachedReindexTasks(fx.desc, fx.subject.Key.UnitID)
	require.Len(t, tasks, 2, "both halves armed")
	prepOK, prep := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, false, fx.logger)
	require.True(t, prepOK, "%v", prep.Errs)

	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	stamped, ok := migrationRecordStampedUnmirrored(rec)
	require.True(t, ok)
	require.NoError(t, shard.migrationRecords.Put(stamped))
	payload := fx.subject.reindexPayload()
	swap := fx.provider.runShardSwapPhase(fx.ctx, &payload, fx.subject.Key.UnitID, fx.name, shard, tasks, fx.logger)
	require.NotEmpty(t, swap.Errs)
	rec, ok = shard.migrationRecords.Get(half.Key)
	require.True(t, ok)
	require.False(t, rec.FlipDecided(), "the prepared half must not swap")
}

// An unreadable record fails an uncached prepare or swap on a held shard instead of letting it wait,
// because without its record the unit could read as finished.
func TestAnUncachedStepFailsOnAHeldShardWithAnUnreadableRecord(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-held-unreadable")
	plantUnreadableRecord(t, fx.recordsDir)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	require.NotEmpty(t, shard.migrationMirrors.heldUnarmed())
	fx.provider.evictReindexTasks(fx.desc, fx.subject.Key.UnitID)

	payload := fx.subject.reindexPayload()
	payload.UnitToShard = map[string]string{fx.subject.Key.UnitID: fx.name}
	err := fx.provider.runPerUnitPhase(&distributedtask.Task{TaskDescriptor: fx.desc}, &payload,
		[]string{fx.subject.Key.UnitID}, fx.idx, fx.logger, "group-completion", "starting", false,
		func(string, ShardLike, []*ShardReindexTaskGeneric, bool) phaseResult { return phaseResult{} })
	require.ErrorContains(t, err, "migration record fault(s) on this shard")
	require.NotErrorIs(t, err, context.Canceled, "the step fails instead of waiting")
}

// A migration with a stamped half fails its swap, so no half of it is armed, and only a stamp
// write that succeeds lets it leave the hold.
func TestAMigrationWithAStampedHalfIsNeverRearmed(t *testing.T) {
	for _, tt := range []struct {
		name              string
		filterableHalf    bool
		searchableCorrupt bool
	}{
		{name: "only the unstamped half fails to arm", filterableHalf: true},
		{name: "the stamp's rewrite is refused", searchableCorrupt: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fx := newDoubleFaultFixture(t, "tenant-stamped-half")
			lsm := shardPathLSM(fx.idx.path(), fx.name)
			halves := []MigrationSubject{fx.subject}
			corrupt := []string{fx.corrupt}
			if tt.filterableHalf {
				half, segment := fx.addHalf(t, StrategyCodeFilterableRetokenize, fx.desc.ID, fx.desc.Version, true)
				halves, corrupt = append(halves, half), append(corrupt, segment)
			}
			if !tt.searchableCorrupt {
				require.NoError(t, os.Remove(fx.corrupt))
				corrupt = corrupt[1:]
			}
			stamped := fx.subject
			stamped.Unmirrored = true
			require.NoError(t, NewMigrationRecordStore(lsm, fx.logger).Put(NewMigrationRecordMerged(stamped)))
			fx.breakDisk(t)

			shard := fx.load(t)
			t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
			require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
			require.Len(t, shard.migrationMirrors.heldUnarmed(), len(halves))

			fx.healRecords(t)
			for _, segment := range corrupt {
				require.NoError(t, os.Remove(segment))
			}
			require.NoError(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)))
			require.Equal(t, storagestate.StatusReady, shard.GetStatus())
			require.Empty(t, shard.migrationMirrors.heldUnarmed())
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
			for _, half := range halves {
				rec, ok := shard.migrationRecords.Get(half.Key)
				require.True(t, ok)
				require.True(t, rec.Subject().Unmirrored, "%s", half.Key.StrategyCode)
				require.False(t, rec.FlipDecided(), "%s", half.Key.StrategyCode)
			}
			fx.requirePhasesRefused(t, shard, "no double-write mirror armed")
		})
	}
}

// A lift only starts the recovery retry, because it can run inside a RAFT apply, so a stuck disk call
// stalls neither the lift, status reads nor a second lift; the shard's shutdown waits for the arm.
func TestLiftingAHeldShardDoesNotWaitOnAStuckDisk(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-stuck-disk")
	fx.breakDisk(t)
	shard := fx.load(t)
	// Opening a pipe with no writer blocks in the kernel, like a hung network volume.
	require.NoError(t, os.Remove(fx.corrupt))
	require.NoError(t, syscall.Mkfifo(fx.corrupt, 0o600))
	// Opening without blocking fails until the arm opens the pipe, so a regression fails instead of hanging.
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
		enterrors.GoWrapper(func() { lifts <- liftReady(shard)() }, fx.logger)
	}
	require.Eventually(t, func() bool { return len(lifts) == 2 }, time.Second, 10*time.Millisecond,
		"both lifts answer while the arm is parked")
	answers := (<-lifts).Error() + "\n" + (<-lifts).Error()
	require.Contains(t, answers, "a recovery retry started")
	require.Contains(t, answers, "a recovery retry is already running")
	status := make(chan storagestate.Status, 1)
	enterrors.GoWrapper(func() { status <- shard.GetStatus() }, fx.logger)
	require.Equal(t, storagestate.StatusReadOnly, receiveWithin(t, status))

	shutdown := make(chan error, 1)
	enterrors.GoWrapper(func() { shutdown <- shard.Shutdown(context.Background()) }, fx.logger)
	require.ErrorIs(t, receiveWithin(t, shutdown), errShardStillInUse, "the arm holds the shard")

	require.Eventually(t, answer, 10*time.Second, 10*time.Millisecond, "the arm parks on the segment")
	require.Eventually(t, shard.shut.Load, 10*time.Second, 10*time.Millisecond,
		"the shutdown completes once the arm ends")
}

// newParkedLift heals a held shard and starts a lift whose arm parks on a busy limiter.
func newParkedLift(t *testing.T, name string) (*doubleFaultFixture, *Shard, <-chan error) {
	t.Helper()
	fx := newDoubleFaultFixture(t, name)
	reg := prometheus.NewRegistry()
	fx.idx.bucketLoadLimiter = loadlimiter.NewLoadLimiter(reg, "buckets", 1)
	fx.breakDisk(t)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	fx.healRecords(t)
	require.NoError(t, os.Remove(fx.corrupt))

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
	return fx, shard, lift
}

// The lift's arm is detached from the shard's context, so a drop that cancels that context
// mid-arm neither stops the arm nor makes it stamp the migration Unmirrored.
func TestADropDuringTheLiftsArmStillArmsTheMirror(t *testing.T) {
	fx, shard, lift := newParkedLift(t, "tenant-lift-gives-up")
	shard.shutCtxCancel(errors.New("dropped"))
	require.Error(t, receiveWithin(t, lift))
	fx.idx.bucketLoadLimiter.Release()

	require.Eventually(t, func() bool { return len(shard.migrationMirrors.heldUnarmed()) == 0 },
		10*time.Second, 10*time.Millisecond, "the arm settles the held set")
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored, "a caller giving up must not stamp the migration")
	require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors())
}

// Cleanup of a migration cancelled during a lift waits for the arm, so no mirror outlives its record.
func TestCleanupOfACancelledMigrationWaitsForTheLiftsArm(t *testing.T) {
	fx, shard, lift := newParkedLift(t, "tenant-lift-cleanup")
	cleaned := make(chan error, 1)
	enterrors.GoWrapper(func() {
		unseal, err := fx.provider.SealLocalTaskDrain(fx.ctx, fx.desc)
		if err != nil {
			cleaned <- err
			return
		}
		defer unseal()
		task := &distributedtask.Task{TaskDescriptor: fx.desc, Status: distributedtask.TaskStatusCancelled}
		cleaned <- fx.idx.discardTaskRecordsOn(fx.ctx, fx.name, task)
	}, fx.logger)
	require.Never(t, func() bool { return len(cleaned) > 0 }, time.Second, 10*time.Millisecond,
		"cleanup waits for the arm's unit claim")
	fx.idx.bucketLoadLimiter.Release()

	require.ErrorContains(t, receiveWithin(t, lift), "a recovery retry started")
	require.NoError(t, receiveWithin(t, cleaned))
	_, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.False(t, ok)
	require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
	require.NoDirExists(t, filepath.Join(shardPathLSM(fx.idx.path(), fx.name), fx.subject.Props["title"].Staged))
}

func TestALiftSkipsAMigrationATeardownHolds(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-lift-sealed")
	fx.breakDisk(t)
	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	fx.healRecords(t)
	require.NoError(t, os.Remove(fx.corrupt))

	unseal, sealed := fx.provider.SealLocalUnit(fx.desc, fx.subject.Key.UnitID)
	require.True(t, sealed)
	defer unseal()
	require.ErrorContains(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)), "a teardown holds the unit")
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
	require.Len(t, shard.migrationMirrors.heldUnarmed(), 1)
}

// A second lift that read the held set before the first lift's late arm settled it must not arm
// again: a failing second arm would stamp a migration the first one armed.
func TestASecondLiftDoesNotRearmWhatTheFirstSettled(t *testing.T) {
	fx, shard, liftA := newParkedLift(t, "tenant-lift-second")
	staged := filepath.Join(shardPathLSM(fx.idx.path(), fx.name), fx.subject.Props["title"].Staged)

	// Holding shutdownLock parks lift B between its first read of the held set and liftRetry.
	shard.shutdownLock.Lock()
	unlock := sync.OnceFunc(shard.shutdownLock.Unlock)
	t.Cleanup(unlock)
	liftB := make(chan error, 1)
	enterrors.GoWrapper(func() {
		liftB <- shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate)
	}, fx.logger)
	require.Eventually(t, func() bool {
		// The dump of every goroutine outgrows a fixed buffer when the whole package runs.
		buf := make([]byte, 1<<20)
		for n := runtime.Stack(buf, true); ; n = runtime.Stack(buf, true) {
			if n < len(buf) {
				return strings.Contains(string(buf[:n]), "(*Shard).preventShutdown")
			}
			buf = make([]byte, 2*len(buf))
		}
	}, 10*time.Second, 10*time.Millisecond, "lift B parks on shutdownLock after reading the held set")

	fx.idx.bucketLoadLimiter.Release()
	require.ErrorContains(t, receiveWithin(t, liftA), "a recovery retry started")
	// Lift A's retry must be over, so lift B takes liftRetry and reaches the arm.
	awaitRetry(t, shard)
	// stat fails on a self-referencing symlink, so a second arm would fail and stamp.
	require.NoError(t, os.Rename(staged, staged+".moved"))
	require.NoError(t, os.Symlink(filepath.Base(staged), staged))
	unlock()
	require.ErrorContains(t, receiveWithin(t, liftB), "a recovery retry started")
	awaitRetry(t, shard)

	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored)
	require.Len(t, fx.provider.cachedReindexTasks(fx.desc, fx.subject.Key.UnitID), 1)
}

// A retry that works leaves a shard READONLY for another reason, and its steps wait until that reason clears.
func TestARetryThatWorksLeavesAShardReadOnlyForAnotherReason(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reason  string
		during  func(t *testing.T, fx *doubleFaultFixture, shard *Shard)
		settled func(t *testing.T, fx *doubleFaultFixture, shard *Shard)
		clear   func(t *testing.T, fx *doubleFaultFixture, shard *Shard)
	}{
		{
			name: "resource pressure", reason: statusReasonResourcePressure,
			during: func(_ *testing.T, fx *doubleFaultFixture, _ *Shard) { fx.idx.db.resourceScanState.transition.Lock() },
			settled: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) {
				require.Eventually(t, func() bool { return len(shard.migrationMirrors.heldUnarmed()) == 0 }, 10*time.Second, 10*time.Millisecond)
				require.Equal(t, statusReasonMirrorUnarmed, shard.GetStatusReason(), "the lift waits for the scan's transition")
				fx.idx.db.resourceScanState.isReadOnly.Store(true)
				fx.idx.db.resourceScanState.transition.Unlock()
			},
			clear: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) {
				fx.idx.db.setReadOnlyFlag(false)
				require.True(t, fx.idx.db.markShardReady(fx.name, shard))
			},
		},
		{
			name: "manual READONLY during the retry", reason: statusReasonManualUpdate,
			during: func(t *testing.T, _ *doubleFaultFixture, shard *Shard) {
				require.NoError(t, shard.SetStatusReadonly(statusReasonManualUpdate))
			},
			clear: func(t *testing.T, _ *doubleFaultFixture, shard *Shard) { require.NoError(t, liftReady(shard)()) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx, shard, lift := newParkedLift(t, "tenant-lift-other-reason")
			tc.during(t, fx, shard)
			fx.idx.bucketLoadLimiter.Release()
			require.ErrorContains(t, receiveWithin(t, lift), "a recovery retry started")
			if tc.settled != nil {
				tc.settled(t, fx, shard)
			}
			awaitRetry(t, shard)

			require.Empty(t, shard.migrationMirrors.heldUnarmed())
			require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
			require.Equal(t, tc.reason, shard.GetStatusReason())
			fx.requireStepsWait(t)

			tc.clear(t, fx, shard)
			require.Equal(t, storagestate.StatusReady, shard.GetStatus())
			fx.requireSwapProceeds(t, shard)
		})
	}
}

func TestTheHeldShardGaugeCountsAShardUntilItsHoldEnds(t *testing.T) {
	held := func() float64 { return testutil.ToFloat64(monitoring.GetMetrics().ReindexHeldShards) }
	for _, tc := range []struct {
		name string
		end  func(t *testing.T, fx *doubleFaultFixture, shard *Shard)
	}{
		{name: "lift", end: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) {
			fx.healRecords(t)
			require.NoError(t, os.Remove(fx.corrupt))
			require.NoError(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)))
		}},
		{name: "shutdown", end: func(t *testing.T, _ *doubleFaultFixture, shard *Shard) {
			require.NoError(t, shard.Shutdown(context.Background()))
		}},
		{name: "drop", end: func(t *testing.T, _ *doubleFaultFixture, shard *Shard) {
			require.NoError(t, shard.drop(true))
		}},
		// A drop proceeds while a retry may still run, and that retry can hold the shard again.
		{name: "drop, then a late arm's hold", end: func(t *testing.T, fx *doubleFaultFixture, shard *Shard) {
			require.NoError(t, shard.drop(true))
			shard.migrationMirrors.holdUnarmed([]*ShardReindexTaskGeneric{fx.task})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newDoubleFaultFixture(t, "tenant-held-gauge")
			fx.breakDisk(t)
			before := held()
			shard := fx.load(t)
			require.Equal(t, before+1, held())
			tc.end(t, fx, shard)
			require.Equal(t, before, held())
		})
	}
}

func liftReady(shard *Shard) func() error {
	return func() error { return shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate) }
}

// liftAndAwaitRetry lifts, which refuses at once and starts the recovery retry, and returns the failure
// the retry logs before it ends.
func liftAndAwaitRetry(t *testing.T, fx *doubleFaultFixture, shard *Shard, lift func() error) error {
	t.Helper()
	logs := test.NewLocal(fx.idx.logger.(*logrus.Logger))
	require.ErrorContains(t, lift(), "a recovery retry started")
	awaitRetry(t, shard)
	for _, e := range logs.AllEntries() {
		if e.Level == logrus.ErrorLevel {
			return errors.New(e.Message)
		}
	}
	return nil
}

func awaitRetry(t *testing.T, shard *Shard) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !shard.liftRetry.TryLock() {
			return false
		}
		shard.liftRetry.Unlock()
		return true
	}, 10*time.Second, 10*time.Millisecond, "the recovery retry ends")
}

func receiveWithin[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("no value within 10s")
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
			task.IndexGroups()
			outcome := &heldStepOutcome{task: task, done: make(chan string, 4)}
			fx.logger.(*logrus.Logger).SetLevel(logrus.DebugLevel)
			logs := test.NewLocal(fx.logger.(*logrus.Logger))
			sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
				CompletionRecorder: outcome, AckRecorder: outcome,
				TaskLister: outcome,
				Providers:  map[string]distributedtask.Provider{ReindexNamespace: fx.provider},
				Logger:     fx.logger, LocalNode: node, TickInterval: 200 * time.Millisecond,
			})
			require.NoError(t, sched.Start(fx.ctx))
			t.Cleanup(sched.Close)

			// Each wake relaunches a waiting step, as unit events elsewhere in the cluster do.
			require.Never(t, func() bool { sched.Wake(); return len(outcome.done) > 0 }, time.Second, 20*time.Millisecond,
				"the step must wait while the shard is held")
			retries, holdWaits, infoOrAbove := 0, 0, 0
			for _, e := range logs.AllEntries() {
				if e.Level <= logrus.InfoLevel {
					infoOrAbove++
				}
				if e.Data["shard"] == fx.name && e.Data["unit"] == unitID &&
					strings.Contains(e.Message, "waits because its shard is held READONLY") {
					holdWaits++
				}
				if e.Data["groupID"] == fx.name {
					retries++
					require.Greaterf(t, e.Level, logrus.InfoLevel, "per-tick line: %s", e.Message)
					require.Contains(t, e.Message, "did not complete and is retryable")
				}
			}
			require.Equal(t, tc.status != distributedtask.TaskStatusStarted, retries > 0)
			require.Positive(t, holdWaits, "the step waits on the hold")
			// Two sampled windows, the unsampled first start line, and the unit's own Warn budget.
			require.LessOrEqual(t, infoOrAbove, 2*5+1+maxReportedErrors)
			fx.healRecords(t)
			require.NoError(t, os.Remove(fx.corrupt))
			require.NoError(t, liftAndAwaitRetry(t, fx, shard, liftReady(shard)))
			require.Equal(t, "succeeded", receiveWithin(t, outcome.done))
		})
	}
}

// heldStepOutcome sends to done each verdict the scheduler records for the step: a prepare ack, a swap ack or a unit result.
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

func (o *heldStepOutcome) RecordDistributedTaskPreparationCompleteAck(_ context.Context, _, _ string, _ uint64, _, _ string, ok bool, msg string) error {
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

func (o *heldStepOutcome) RecordDistributedTaskPostCompletionAck(_ context.Context, _, _ string, _ uint64, _, _ string, ok bool, msg string) error {
	o.verdict(ok, msg)
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
	return p, NewMigrationRecoveryReindexer(logger, nil, p)
}

func bareShardWithMergedRecord(t *testing.T, name string) *Shard {
	t.Helper()
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

// A load whose client gives up still arms a healthy migration instead of stamping it Unmirrored.
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

// installCaughtUpTaskList makes the node's applied task list readable and decide no swap, so an
// unarmable record may be stamped; an unreachable leader keeps the cluster pass from acting.
func installCaughtUpTaskList(ctx context.Context, database *DB) {
	database.SetMigrationTaskSources(ctx,
		func(string) ([]distributedtask.TaskStatusEntry, bool) { return nil, true },
		func(context.Context) ([]*distributedtask.Task, error) { return nil, errors.New("leader unreachable") })
}

func newDoubleFaultFixture(t *testing.T, name string) *doubleFaultFixture {
	t.Helper()
	ctx := testCtx()
	className := "LazyRecoveryDoubleFault_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	t.Cleanup(func() { _ = shd.Shutdown(context.Background()) })

	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(logger, nil, p)
	installCaughtUpTaskList(ctx, idx.db)
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

// addHalf records a merged migration on the fixture's property and returns its staged segment,
// which fails the arm when corrupt.
func (fx *doubleFaultFixture) addHalf(t *testing.T, code MigrationStrategyCode, taskID string, version uint64, corrupt bool,
) (MigrationSubject, string) {
	t.Helper()
	lsm := shardPathLSM(fx.idx.path(), fx.name)
	subject := testMigrationSubject(version, code, "title")
	subject.TaskID, subject.Collection, subject.Key.UnitID = taskID, fx.className, fx.subject.Key.UnitID
	built, err := buildRecoveryTasks(subject, fx.name, fx.logger, nil)
	require.NoError(t, err)
	subject.Props["title"] = MigrationPropertyDirs{
		Staged: built[0].ingestBucketName("title"), Canonical: subject.Props["title"].Canonical,
		Sidecar: built[0].reindexBucketName("title"),
	}
	staged := filepath.Join(lsm, subject.Props["title"].Staged)
	require.NoError(t, os.MkdirAll(staged, 0o777))
	segment := filepath.Join(staged, "segment-1.db")
	if corrupt {
		require.NoError(t, os.WriteFile(segment, []byte("not a segment"), 0o600))
	}
	require.NoError(t, NewMigrationRecordStore(lsm, fx.logger).Put(NewMigrationRecordMerged(subject)))
	return subject, segment
}

func (fx *doubleFaultFixture) load(t *testing.T) *Shard {
	t.Helper()
	loaded, err := fx.idx.initShard(fx.ctx, fx.name, fx.class, nil, true, true)
	require.NoError(t, err)
	fx.idx.shards.Store(fx.name, loaded)
	return loaded.(*Shard)
}

// breakDisk makes the records dir refuse the unmirrored stamp.
func (fx *doubleFaultFixture) breakDisk(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores the read-only records dir")
	}
	require.NoError(t, os.Chmod(fx.recordsDir, 0o500))
	t.Cleanup(func() { fx.healRecords(t) })
}

func (fx *doubleFaultFixture) healRecords(t *testing.T) {
	t.Helper()
	require.NoError(t, os.Chmod(fx.recordsDir, 0o700))
}

func (fx *doubleFaultFixture) requirePhasesRefused(t *testing.T, shard *Shard, want string) {
	t.Helper()
	tasks := []*ShardReindexTaskGeneric{fx.task}
	prepOK, prep := fx.provider.runShardPrepPhase(fx.ctx, fx.subject.Key.UnitID, shard, tasks, false, fx.logger)
	require.False(t, prepOK)
	require.Len(t, prep.Errs, 1)
	require.Contains(t, prep.Errs[0], want)
	require.False(t, prep.Transient)
	payload := fx.subject.reindexPayload()
	swap := fx.provider.runShardSwapPhase(fx.ctx, &payload, fx.subject.Key.UnitID, fx.name, shard, tasks, fx.logger)
	require.NotEmpty(t, swap.Errs)
	require.Contains(t, swap.Errs[0], want)
	require.False(t, swap.Transient)
	rec, ok := shard.migrationRecords.Get(fx.subject.Key)
	require.True(t, ok)
	require.False(t, rec.FlipDecided(), "nothing flipped")
}

// requireStepsWait runs prepare and swap through the unit entry: on a held shard neither runs,
// and the scheduler retries both on a later tick.
func (fx *doubleFaultFixture) requireStepsWait(t *testing.T) {
	t.Helper()
	payload := fx.subject.reindexPayload()
	payload.UnitToShard = map[string]string{fx.subject.Key.UnitID: fx.name}
	for _, callback := range []string{"group-completion", "swap-requested"} {
		ran := make(chan struct{}, 1)
		err := fx.provider.runPerUnitPhase(&distributedtask.Task{TaskDescriptor: fx.desc}, &payload,
			[]string{fx.subject.Key.UnitID}, fx.idx, fx.logger, callback, "starting", callback == "swap-requested",
			func(string, ShardLike, []*ShardReindexTaskGeneric, bool) phaseResult {
				ran <- struct{}{}
				return phaseResult{}
			})
		require.ErrorIs(t, err, context.Canceled, callback)
		require.ErrorContains(t, err, "is held READONLY", callback)
		require.Empty(t, ran, "%s ran on a held shard", callback)
	}
	require.Len(t, fx.provider.cachedReindexTasks(fx.desc, fx.subject.Key.UnitID), 1, "the waiting unit keeps its cached task")
}

func (fx *doubleFaultFixture) requireSwapProceeds(t *testing.T, shard *Shard) {
	t.Helper()
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

// Pins that a tenant set COLD while its swap is owed loses no write taken
// after it is reactivated. A mirror that fails to arm at the reload holds the
// shard READONLY rather than stamping the record, since a stamp would refuse
// the owed swap for good; once lifted, the late swap holds the write.
func TestWritesAfterReactivatingATenantThatOwesASwapSurviveTheSwap(t *testing.T) {
	for _, tc := range []struct {
		name string
		// The load runs before this node's RAFT has caught up, so no task list is readable yet.
		raftBehind bool
		// This node acked its PREP, and the swap is decided only after the load.
		decidedAfterLoad bool
		// The task then ends with the swap still undecided, so nothing is owed.
		endsUndecided bool
		// An earlier load stamped the record, which already refuses the swap.
		stampedBefore bool
		// The task ended FAILED rather than CANCELLED before the owed swap ran.
		failed bool
	}{
		{name: "the mirror fails to arm"},
		{name: "the mirror fails to arm, and the task failed", failed: true},
		{name: "the mirror fails to arm before RAFT has caught up", raftBehind: true},
		{name: "the mirror fails to arm after this node acked, before the swap is decided", decidedAfterLoad: true},
		{name: "a held lift stamps once the acked task ends undecided", decidedAfterLoad: true, endsUndecided: true},
		{name: "a record stamped earlier is not held for its decided swap", stampedBefore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const prop, node = "title", "node1"
			ctx := testCtx()
			className := "ColdHot_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			class.Properties[0].IndexFilterable, class.Properties[0].IndexSearchable = boolPtr(true), boolPtr(true)
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			registerIndex(idx, className)
			require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "seed")))

			logger, _ := test.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
			idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())
			idx.shardReindexer = NewMigrationRecoveryReindexer(logger, nil, p)

			unitID, group := shard.migrationUnit(), shard.Name()
			payload := ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{prop},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
				UnitToShard: map[string]string{unitID: group}, UnitToNode: map[string]string{unitID: node},
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := func(status distributedtask.TaskStatus) *distributedtask.Task {
				task := &distributedtask.Task{
					Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
					Status: status, Payload: raw, NeedsPreparationBarrier: true,
					SwapDecidedGroups: map[string]time.Time{group: time.Now()}, SwapsOwed: map[string][]string{group: {node}},
					Units: map[string]*distributedtask.Unit{unitID: {ID: unitID, GroupID: group, NodeID: node}},
				}
				task.IndexGroups()
				return task
			}
			p.processOneUnit(ctx, task(distributedtask.TaskStatusStarted), &payload, idx, unitID, newFakeRecorder())
			require.NoError(t, p.OnGroupCompleted(task(distributedtask.TaskStatusPreparing), group, []string{unitID}))
			cancelled := task(distributedtask.TaskStatusCancelled)
			atLoad := cancelled
			if tc.decidedAfterLoad {
				atLoad = task(distributedtask.TaskStatusPreparing)
				atLoad.SwapDecidedGroups, atLoad.SwapsOwed = nil, nil
				atLoad.PreparedGroups = map[string]map[string]distributedtask.PostCompletionAck{group: {node: {Success: true}}}
			}
			if tc.raftBehind {
				idx.db.SetMigrationTaskSources(ctx,
					func(string) ([]distributedtask.TaskStatusEntry, bool) { return nil, false },
					func(context.Context) ([]*distributedtask.Task, error) { return nil, errors.New("RAFT is behind") })
			} else {
				installTestMigrationTaskSources(ctx, idx.db, nil, atLoad)
			}
			if !tc.decidedAfterLoad {
				require.NoError(t, p.OnTaskCompleted(cancelled))
			}

			mig := &Migrator{db: idx.db, logger: logger}
			setStatus := func(status string) {
				require.NoError(t, mig.UpdateTenants(ctx, class,
					[]*schemaUC.UpdateTenantPayload{{Name: group, Status: status}}, false))
			}
			setStatus(models.TenantActivityStatusCOLD)
			restore := corruptIngestSegments(t, shard.pathLSM())
			if tc.stampedBefore {
				store := NewMigrationRecordStoreForUnit(shard.pathLSM(), unitID, logger)
				require.NoError(t, store.Load())
				require.NotEmpty(t, store.Records())
				for _, rec := range store.Records() {
					stamped, ok := migrationRecordStampedUnmirrored(rec)
					require.True(t, ok)
					require.NoError(t, store.Put(stamped))
				}
			}
			setStatus(models.TenantActivityStatusHOT)
			reloaded, err := unwrapShard(ctx, idx.shards.Load(group))
			require.NoError(t, err)
			defer reloaded.Shutdown(ctx)
			if tc.stampedBefore {
				require.Equal(t, storagestate.StatusReady, reloaded.GetStatus(), "holding cannot save a swap the stamp refuses")
				return
			}
			for _, rec := range reloaded.migrationRecords.Records() {
				require.False(t, rec.Subject().Unmirrored, "a stamp would refuse the owed swap for good")
			}
			require.Equal(t, storagestate.StatusReadOnly, reloaded.GetStatus())
			if tc.endsUndecided {
				// In place, so the lift runs before the periodic cluster pass discards the ended run.
				atLoad.Status = distributedtask.TaskStatusCancelled
				require.ErrorContains(t, liftReady(reloaded)(), "a recovery retry started")
				awaitRetry(t, reloaded)
				require.Equal(t, storagestate.StatusReady, reloaded.GetStatus())
				require.NotEmpty(t, reloaded.migrationRecords.Records())
				for _, rec := range reloaded.migrationRecords.Records() {
					require.True(t, rec.Subject().Unmirrored, "an ended task owes no swap, so the lift stamps")
				}
				return
			}
			if tc.raftBehind || tc.decidedAfterLoad {
				installTestMigrationTaskSources(ctx, idx.db, nil, cancelled)
			}
			restore()
			require.ErrorContains(t, liftReady(reloaded)(), "a recovery retry started")
			awaitRetry(t, reloaded)
			require.Equal(t, storagestate.StatusReady, reloaded.GetStatus())
			require.NotEmpty(t, p.cachedReindexTasks(cancelled.TaskDescriptor, unitID),
				"the owed swap reuses the instances the reload armed, even once the task has ended")
			written := createTestObjectWithText(className, "afterhot")
			require.NoError(t, reloaded.PutObject(ctx, written))
			ended := task(distributedtask.TaskStatusCancelled)
			if tc.failed {
				ended.Status = distributedtask.TaskStatusFailed
			}
			require.NoError(t, p.OnSwapRequested(ended, group, []string{unitID}))
			for _, rec := range reloaded.migrationRecords.Records() {
				require.True(t, rec.FlipDecided(), "the decided swap runs after the task ended")
			}

			filtered, _, err := reloaded.ObjectSearch(ctx, 10, propEqualsFilter(className, prop, "afterhot"), nil, nil, nil, additional.Properties{}, nil)
			require.NoError(t, err)
			require.Contains(t, objectIDs(filtered), written.ID().String(), "the filter finds the write")
			ranked, _, err := reloaded.ObjectSearch(ctx, 10, nil,
				&searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: "afterhot"},
				nil, nil, additional.Properties{}, []string{prop})
			require.NoError(t, err)
			require.Contains(t, objectIDs(ranked), written.ID().String(), "and so does BM25")
		})
	}
}

// corruptIngestSegments puts an unreadable segment in every staged bucket, so
// the next load cannot arm the mirror, and returns what puts them back.
func corruptIngestSegments(t *testing.T, lsm string) func() {
	t.Helper()
	entries, err := os.ReadDir(lsm)
	require.NoError(t, err)
	var restores []func()
	for _, e := range entries {
		if !e.IsDir() || !strings.Contains(e.Name(), "_ingest") {
			continue
		}
		path := filepath.Join(lsm, e.Name(), "segment-1.db")
		original, readErr := os.ReadFile(path)
		existed := readErr == nil
		require.True(t, existed || errors.Is(readErr, os.ErrNotExist), "read %s: %v", path, readErr)
		require.NoError(t, os.WriteFile(path, []byte("not a segment"), 0o600))
		restores = append(restores, func() {
			if existed {
				require.NoError(t, os.WriteFile(path, original, 0o600))
			} else {
				require.NoError(t, os.Remove(path))
			}
		})
	}
	require.NotEmpty(t, restores, "a staged bucket to corrupt")
	return func() {
		for _, r := range restores {
			r()
		}
	}
}

// A committed swap whose shard is held on its own migration fails the task
// instead of keeping it SWAPPING, so the collection can be deleted; the swap
// stays owed.
func TestAHeldCommittedSwapFailsTheTaskAndKeepsItsDebt(t *testing.T) {
	fx := newDoubleFaultFixture(t, "tenant-held-commit")
	node, unitID := fx.idx.getSchema.NodeName(), fx.subject.Key.UnitID
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  fx.logger,
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
	})
	payload := fx.subject.reindexPayload()
	payload.UnitToShard, payload.UnitToNode = map[string]string{unitID: fx.name}, map[string]string{unitID: node}
	restoreSwappingTask(t, mgr, fx.desc, payload, unitID, fx.name, node)
	// The node's applied task list decides the swap, so the unarmable record cannot be stamped.
	fx.idx.db.SetMigrationTaskSources(fx.ctx,
		func(shard string) ([]distributedtask.TaskStatusEntry, bool) {
			return mgr.LocalTaskStatuses(ReindexNamespace, shard), true
		},
		func(context.Context) ([]*distributedtask.Task, error) { return nil, errors.New("leader unreachable") })

	shard := fx.load(t)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.Len(t, shard.migrationMirrors.heldUnarmed(), 1)
	registerIndex(fx.idx, fx.className)

	sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
		CompletionRecorder: &heldStepOutcome{done: make(chan string, 16)},
		AckRecorder:        managerAckRecorder{mgr},
		TaskLister:         mgr,
		Providers:          map[string]distributedtask.Provider{ReindexNamespace: fx.provider},
		Logger:             fx.logger, LocalNode: node, TickInterval: 50 * time.Millisecond,
	})
	require.NoError(t, sched.Start(fx.ctx))
	t.Cleanup(sched.Close)

	task := func() *distributedtask.Task {
		tasks, _ := mgr.ListDistributedTasks(fx.ctx)
		return tasks[ReindexNamespace][0]
	}
	require.Eventually(t, func() bool { sched.Wake(); return task().Status == distributedtask.TaskStatusFailed },
		10*time.Second, 20*time.Millisecond, "the held swap's failure ack fails the task")
	require.Contains(t, task().Error, "held READONLY")
	require.Equal(t, []string{node}, task().SwapsOwed[fx.name], "the swap stays owed")
	require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
	require.NoError(t, mgr.CheckClassMutation(fx.className))
	require.NoError(t, mgr.CheckTenantMutation(fx.className, []string{fx.name}))
}

func restoreSwappingTask(t *testing.T, mgr *distributedtask.Manager, desc distributedtask.TaskDescriptor,
	payload ReindexTaskPayload, unitID, group, node string,
) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	type taskWithUnits struct {
		*distributedtask.Task
		Units map[string]*distributedtask.Unit `json:"units"`
	}
	snapshot, err := json.Marshal(map[string]map[string][]taskWithUnits{"tasks": {ReindexNamespace: {{
		Task: &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: desc, Payload: raw,
			Status: distributedtask.TaskStatusSwapping, NeedsPreparationBarrier: true,
			SwapDecidedGroups: map[string]time.Time{group: time.Now()},
			SwapsOwed:         map[string][]string{group: {node}},
		},
		Units: map[string]*distributedtask.Unit{unitID: {
			ID: unitID, GroupID: group, NodeID: node, Status: distributedtask.UnitStatusCompleted,
		}},
	}}}})
	require.NoError(t, err)
	require.NoError(t, mgr.Restore(snapshot))
}

// managerAckRecorder applies the scheduler's acks to the FSM, as a RAFT apply would.
type managerAckRecorder struct{ mgr *distributedtask.Manager }

func (r managerAckRecorder) RecordDistributedTaskPostCompletionAck(_ context.Context, namespace, taskID string,
	version uint64, groupID, nodeID string, success bool, errMsg string,
) error {
	sub, err := json.Marshal(&api.RecordDistributedTaskPostCompletionAckRequest{
		Namespace: namespace, Id: taskID, Version: version, GroupId: groupID, NodeId: nodeID,
		Success: success, Error: errMsg, AckedAtUnixMillis: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return r.mgr.RecordPostCompletionAck(&api.ApplyRequest{SubCommand: sub})
}

func (r managerAckRecorder) RecordDistributedTaskPreparationCompleteAck(context.Context, string, string, uint64, string, string, bool, string) error {
	return errors.New("no PREP ack expected")
}
