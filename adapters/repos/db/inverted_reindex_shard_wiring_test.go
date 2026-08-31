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
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The registry grants when it has nobody to ask and refuses only on a real
// refusal, so a deployment that never wires a seal provider up can still
// reclaim a migration's directories.
func TestMigrationUnitSealsDisposition(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "Books:change-tokenization:title:ab12", Version: 42}
	const unitID = "shard-1__node-0"

	tests := []struct {
		name       string
		builder    ReindexUnitSealBuilder
		wantSealed bool
	}{
		{name: "no builder installed", wantSealed: true},
		{
			name:       "a builder that produces no seal",
			builder:    func() ReindexUnitSeal { return nil },
			wantSealed: true,
		},
		{
			name: "an installed seal refuses while a worker still holds the unit",
			builder: func() ReindexUnitSeal {
				return func(d distributedtask.TaskDescriptor, u string) (func(), bool) {
					require.Equal(t, desc, d, "the seal must answer for the unit the teardown named")
					require.Equal(t, unitID, u)
					return nil, false
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seals migrationUnitSeals
			seals.Install(tt.builder)

			release, sealed := seals.SealUnit(desc, unitID)
			require.Equal(t, tt.wantSealed, sealed)
			if tt.wantSealed {
				require.NotNil(t, release, "a granted seal must hand back a release the teardown can call")
				release()
			}
		})
	}
}

func installTestMigrationTaskSources(ctx context.Context, database *DB, leaderErr error,
	tasks ...*distributedtask.Task,
) {
	database.SetMigrationTaskSources(ctx,
		func() ([]*distributedtask.Task, bool) { return tasks, true },
		func(context.Context) ([]*distributedtask.Task, error) {
			if leaderErr != nil {
				return nil, leaderErr
			}
			return tasks, nil
		})
}

func TestReconcileWithClusterWithholdsWhereItCannotAct(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name         string
		shuttingDown bool
		leaderErr    error
		wantSurvives bool
	}{
		{
			name: "a shard that is staying decides the migration the cluster abandoned",
		},
		{
			name:         "a shard on its way out is left to its next activation",
			shuttingDown: true,
			wantSurvives: true,
		},
		{
			name:         "an unreachable leader decides nothing at all",
			leaderErr:    errors.New("leader unreachable"),
			wantSurvives: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringShutdownGuard_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propName}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			if tt.shuttingDown {
				shard.shutdownRequested.Store(true)
				defer shard.shutdownRequested.Store(false)
			}

			require.NotNil(t, idx.db, "the test shard fixture has to wire idx.db")
			idx.db.indices[indexID(idx.Config.ClassName)] = idx

			installTestMigrationTaskSources(ctx, idx.db, tt.leaderErr, &distributedtask.Task{
				Namespace: ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{
					ID: subject.TaskID, Version: subject.Key.TaskVersion,
				},
				Status: distributedtask.TaskStatusCancelled,
			})

			assert.Equal(t, tt.wantSurvives, dirExists(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.Equal(t, tt.wantSurvives, present, "the migration record")
		})
	}
}

func TestReconcileWithoutADatabaseHandle(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name string
		rec  func(MigrationSubject) MigrationRecord
	}{
		{
			name: "merged",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringNoDBHandle_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(tt.rec(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			handle := idx.db
			idx.db = nil
			defer func() { idx.db = handle }()

			require.NotPanics(t, func() { shard.reconcileMigrationRecords(ctx, class) })

			assert.True(t, dirExists(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.True(t, present, "the migration record")
		})
	}
}

// The properties a predecessor still owns must keep serving while one is retired.
func TestShutdownStagedBucketsClosesOnlyTheNamedProperty(t *testing.T) {
	const propA, propB = "title", "author"

	tests := []struct {
		name  string
		prop  string
		other string
	}{
		{name: "retiring the first property", prop: propA, other: propB},
		{name: "retiring the second property", prop: propB, other: propA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringStagedScope_" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propA, propB}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, propA, propB)

			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, dir,
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
				require.NotNil(t, shard.store.Bucket(dir), "the fixture has to open %q", dir)
			}
			require.NoError(t, shard.migrationRecords.Put(
				NewMigrationRecordIterating(subject, MigrationCheckpoint{})))

			require.NoError(t, shard.ShutdownStagedBuckets(ctx, subject.Key, tt.prop))

			assert.Nil(t, shard.store.Bucket(subject.Props[tt.prop].Staged),
				"the staged bucket of the retired property")
			assert.Nil(t, shard.store.Bucket(subject.Props[tt.prop].Sidecar),
				"the sidecar bucket of the retired property")
			assert.NotNil(t, shard.store.Bucket(subject.Props[tt.other].Staged),
				"the staged bucket of the property still in use")
			assert.NotNil(t, shard.store.Bucket(subject.Props[tt.other].Sidecar),
				"the sidecar bucket of the property still in use")
		})
	}
}

// Both accessors read l.shard, which the loader writes under l.mutex. Taking
// the same mutex on the read side is what makes the pair safe, and dropping it
// makes this test fail under -race.
func TestLazyLoadShardMigrationAccessorsLockAgainstTheLoader(t *testing.T) {
	tests := []struct {
		name string
		read func(*LazyLoadShard) bool
		want string
	}{
		{
			name: "record store",
			read: func(l *LazyLoadShard) bool { return l.migrationRecordStore() != nil },
			want: "a loaded shard reconciles its records at init, so the accessor has to see a store",
		},
		{
			name: "mirror registry",
			read: func(l *LazyLoadShard) bool { return l.migrationMirrorRegistry() != nil },
			want: "a loaded shard owns a mirror registry, so the accessor has to see one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const tenant = "accessor-reader"

			ctx := testCtx()
			className := "WiringAccessorRace_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())

			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			// The reader is already spinning when the load starts and keeps
			// spinning until it has returned, so the write to l.shard lands
			// inside the window the reader is reading in. No timing assumption
			// sits between the two.
			spinning, loadDone := make(chan struct{}), make(chan struct{})
			var readers sync.WaitGroup
			readers.Add(1)
			go func() {
				defer readers.Done()
				close(spinning)
				for {
					tt.read(cold)
					select {
					case <-loadDone:
						return
					default:
					}
				}
			}()

			<-spinning
			loadErr := cold.Load(ctx)
			close(loadDone)
			readers.Wait()

			require.NoError(t, loadErr)
			require.True(t, tt.read(cold), tt.want)
		})
	}
}

// The whole safety argument for this PR is that a shard load decides nothing.
// The record pass runs on every load, but it is wired with no task source, so
// the only disposition it can reach is to leave the record standing. A load
// has to hand back the record, and every directory it names, as it found them
// — even when the schema already shows the migration's effect, which is the
// one reading that would otherwise license a commit.
func TestAShardLoadLeavesAnUndecidedRecordExactlyAsItFoundIt(t *testing.T) {
	ctx := testCtx()
	className := "WiringInertLoad_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	class.Properties[0].Tokenization = models.PropertyTokenizationLowercase

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	subject.Key.UnitID = shard.migrationUnit()
	require.Equal(t, models.PropertyTokenizationLowercase, subject.TargetTokenization,
		"fixture: the schema has to already show this migration's effect")
	require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))

	named := append(migrationOwnedDirs(subject), subject.Props["title"].Canonical)
	for _, dir := range named {
		require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
	}
	tracker := filepath.Join(shard.pathLSM(), migrationsDir, subject.TrackerDir)
	require.NoError(t, os.MkdirAll(tracker, 0o777))

	shard.reconcileMigrationRecords(ctx, class)

	rec, ok := shard.migrationRecords.Get(subject.Key)
	require.True(t, ok, "a load must not retire a record it has no task list to decide against")
	require.Equal(t, MigrationStateMerged, rec.State(),
		"and it must not move it: no disposition on this build is reachable from a shard load")
	for _, dir := range named {
		require.DirExists(t, filepath.Join(shard.pathLSM(), dir),
			"every directory the record names survives a load that decided nothing")
	}
	require.DirExists(t, tracker, "and so does its tracker directory")
}

// The counters answer two different operator questions, and the call site is
// the only place that decides which count goes where. Both move on every
// faulted load, so a swap of the two arguments shows up nowhere unless a test
// drives a real load and gives the two counts different values.
func TestAShardLoadCountsWedgedAndUnreadableRecordsInTheirOwnCounters(t *testing.T) {
	ctx := testCtx()
	className := "WiringRecordCounters_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	class.Properties[0].Tokenization = models.PropertyTokenizationLowercase

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	// One record the load cannot place: its rebuilt data is gone, so the pass
	// tries to restart it, and the freeze the unreadable files raise refuses
	// that write.
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	subject.Key.UnitID = shard.migrationUnit()
	require.NoError(t, shard.migrationRecords.Put(
		NewMigrationRecordIterating(subject, MigrationCheckpoint{})))

	// Two files, not one: equal counts would survive the swap this test exists
	// to catch.
	records := shard.migrationRecords.Dir()
	for _, version := range []uint64{7, 8} {
		key := testMigrationSubject(version, StrategyCodeSearchableRetokenize, "title").Key
		key.UnitID = shard.migrationUnit()
		require.NoError(t, os.WriteFile(filepath.Join(records, key.fileName()),
			[]byte("a record this build cannot decode"), 0o600))
	}

	// Deltas, not absolutes: the counters are node-wide and every shard load in
	// this package adds to the same two.
	m := monitoring.GetMetrics()
	wedgedBefore := testutil.ToFloat64(m.MigrationRecordsWedged)
	notUnderstoodBefore := testutil.ToFloat64(m.MigrationRecordsNotUnderstood)

	shard.reconcileMigrationRecords(ctx, class)

	require.Equal(t, float64(1), testutil.ToFloat64(m.MigrationRecordsWedged)-wedgedBefore,
		"the one record this load left standing belongs in the wedged counter")
	require.Equal(t, float64(2), testutil.ToFloat64(m.MigrationRecordsNotUnderstood)-notUnderstoodBefore,
		"and the two files it could not read belong in the other")
}

// The store sets aside every record whose unit is not this shard's own, so the
// shard has to arrive at the exact ID the submit path assigns. Both sides are
// stated here rather than read back out of migrationUnit(), which is what every
// other fixture does: a divergence would set aside every record on every shard,
// and that reads as a node that quietly stopped reconciling, not as a fault.
func TestAShardNamesItsOwnUnitTheWayTheSubmitPathDoes(t *testing.T) {
	require.Equal(t, "shard-1__node-0", MigrationUnitID("shard-1", "node-0"),
		"the unit ID is the shard name, two underscores, then the node name")

	ctx := testCtx()
	className := "WiringUnitID_" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	require.Equal(t, shard.name+"__node1", shard.migrationUnit(),
		"and the shard composes it from its own name and the name of the node it runs on")
}
