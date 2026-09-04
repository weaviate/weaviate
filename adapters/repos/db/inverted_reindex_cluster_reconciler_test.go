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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	esync "github.com/weaviate/weaviate/entities/sync"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The pass walks every loaded shard, and a shard count is a tenant count. A
// node drain refuses all of them at once and the pass repeats every interval,
// so a line per shard would repeat the whole tenant list every minute.
func TestTheClusterPassSamplesItsPerShardRefusals(t *testing.T) {
	const shards = maxReportedErrors + 5

	logger, hook := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.NoError(t, store.Put(NewMigrationRecordMerged(
		testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"))))
	require.NoError(t, store.Load())
	require.True(t, store.HasUndecided(), "fixture: the pass only runs when something is undecided")

	idx := &Index{logger: logger, shardCreateLocks: esync.NewKeyRWLocker()}
	for i := 0; i < shards; i++ {
		name := fmt.Sprintf("shard-%02d", i)
		shard := NewMockShardLike(t)
		shard.EXPECT().migrationRecordStore().Return(store).Maybe()
		shard.EXPECT().Name().Return(name).Maybe()
		shard.EXPECT().preventShutdown().Return(func() {}, nil).Maybe()
		idx.shards.Store(name, shard)
	}

	db := &DB{logger: logger, indices: map[string]*Index{"Books": idx}}
	db.migrationCluster.db = db
	db.migrationCluster.local = func() ([]*distributedtask.Task, bool) { return nil, true }
	db.migrationCluster.cluster = func(context.Context) ([]*distributedtask.Task, error) {
		return nil, nil
	}

	db.migrationCluster.ReconcileLoaded(context.Background())

	var refusals int
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel {
			refusals++
		}
	}
	require.Equal(t, maxReportedErrors, refusals,
		"a mock shard is a shard the pass cannot resolve, and %d of them must not print %d lines", shards, shards)
}

// Every other fixture shard is a mock, which unwrapShard rejects before the
// walk takes a reference or reaches the pass body. The real shard's own arm is
// silent: every verdict is Leave until the cutover PR wires LocalTasks, so a
// second, unresolvable shard is what makes a running walk observable.
func TestTheClusterPassWalksARealLoadedShard(t *testing.T) {
	ctx := context.Background()
	logger, hook := test.NewNullLogger()

	class := newTestClassWithProps("ClusterPassWalk", []string{"title"})
	shd, realIdx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = logger })
	shard := shd.(*Shard)

	detectorIdx := &Index{logger: logger, shardCreateLocks: esync.NewKeyRWLocker()}
	detector := NewMockShardLike(t)
	detector.EXPECT().Name().Return("detector").Maybe()
	// The pass walks only the shards that have something undecided, so the
	// detector needs a record of its own to be reached at all. Empty to begin
	// with, so the settled-node assertion below still holds.
	detectorStore := NewMigrationRecordStore(t.TempDir(), logger)
	detector.EXPECT().migrationRecordStore().Return(detectorStore).Maybe()
	detector.EXPECT().preventShutdown().Return(func() {}, nil).Maybe()
	detectorIdx.shards.Store("detector", detector)

	// A loaded shard with nothing to decide. preventShutdown is deliberately
	// left unstubbed: the pass reconciles the shards the gate named and no
	// others, and an unexpected call here is what says it walked this one too.
	settled := NewMockShardLike(t)
	settled.EXPECT().Name().Return("settled").Maybe()
	settled.EXPECT().migrationRecordStore().Return(NewMigrationRecordStore(t.TempDir(), logger)).Maybe()
	detectorIdx.shards.Store("settled", settled)

	db := &DB{logger: logger, indices: map[string]*Index{"Real": realIdx, "Detector": detectorIdx}}
	db.migrationCluster.db = db
	leaderQueries := 0
	db.migrationCluster.cluster = func(context.Context) ([]*distributedtask.Task, error) {
		leaderQueries++
		return nil, nil
	}
	hook.Reset()

	// A settled node does not query the leader.
	db.migrationCluster.ReconcileLoaded(ctx)
	require.Zero(t, leaderQueries, "nothing is undecided, so the leader is not asked")

	// The gate answers for the whole node, so a shard holding a record this
	// build cannot read must not open it: nothing here can say what that record
	// decided, undecided sibling or not.
	require.NoError(t, detectorStore.Put(NewMigrationRecordMerged(
		testMigrationSubject(43, StrategyCodeEnableFilterable, "title"))))
	plantUnreadableRecord(t, detectorStore.Dir())
	require.NoError(t, detectorStore.Load())
	require.True(t, detectorStore.HasUndecided(), "fixture: the record it could read is undecided")
	db.migrationCluster.ReconcileLoaded(ctx)
	require.Zero(t, leaderQueries,
		"a shard this build cannot read every record of must not open the leader-query gate")
	require.NoError(t, os.Remove(filepath.Join(detectorStore.Dir(), "99_enable_searchable.json")))
	require.NoError(t, detectorStore.Load())
	hook.Reset()

	// A failed leader query walks no shard.
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	subject.Key.UnitID = shard.migrationUnit()
	require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
	require.NoError(t, shard.migrationRecords.Load())
	db.migrationCluster.cluster = func(context.Context) ([]*distributedtask.Task, error) {
		return nil, fmt.Errorf("no leader")
	}
	db.migrationCluster.ReconcileLoaded(ctx)
	require.Len(t, storeLinesAt(hook, logrus.WarnLevel), 1)
	require.Contains(t, storeLinesAt(hook, logrus.WarnLevel)[0], "deciding nothing this pass")
	require.Empty(t, storeLinesAt(hook, logrus.ErrorLevel),
		"an unreachable leader stops the pass before it walks anything")
	hook.Reset()

	// A real loaded shard is resolved, reaches the pass body, and its reference
	// is released.
	db.migrationCluster.cluster = func(context.Context) ([]*distributedtask.Task, error) { return nil, nil }
	db.migrationCluster.ReconcileLoaded(ctx)
	refusals := storeLinesAt(hook, logrus.ErrorLevel)
	require.Len(t, refusals, 1, "the walk ran, and only the shard it cannot resolve refused")
	require.Contains(t, refusals[0], "could not be resolved")
	require.Zero(t, shard.inUseCounter.Load(), "the walk released the reference it took")
	require.Empty(t, storeLinesAt(hook, logrus.InfoLevel), "a skipped shard says so, and none was skipped")
	rec, ok := shard.migrationRecords.Get(subject.Key)
	require.True(t, ok)
	require.Equal(t, MigrationStateMerged, rec.State(),
		"this build decides nothing: the cutover PR is what makes this record move")
	hook.Reset()

	// A shard whose shutdown began is skipped and sampled.
	shard.shutdownRequested.Store(true)
	db.migrationCluster.ReconcileLoaded(ctx)
	shard.shutdownRequested.Store(false)
	skipped := storeLinesAt(hook, logrus.InfoLevel)
	require.Len(t, skipped, 1)
	require.Contains(t, skipped[0], "shutting down")
	require.Zero(t, shard.inUseCounter.Load(), "a refused preventShutdown takes no reference")
}

// "No tasks" and "no answer" license opposite dispositions of the same record.
func TestUninstalledTaskSourcesDecideNothing(t *testing.T) {
	var r migrationClusterReconciler

	tasks, readable := r.LocalTasks()
	require.Nil(t, tasks)
	require.False(t, readable,
		"an uninstalled local view must not read as readable-and-empty")

	cluster, err := r.clusterTasksBounded(context.Background())
	require.Error(t, err,
		"an uninstalled cluster source must not read as an authoritative empty task list")
	require.Nil(t, cluster)
}

// The pass collects a shard name and resolves it a moment later, so a tenant
// torn down in between has to read as gone. Resolving the shard the walk itself
// handed over instead calls Load, which has no shutdown flag to refuse: it
// rebuilds the tenant outside the shard map, where nothing can ever shut it
// down and reactivation fails until the node restarts.
func TestTheClusterPassDoesNotRebuildAShardTornDownUnderIt(t *testing.T) {
	const tenant = "torn-down-under-the-pass"

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := newTestClassWithProps("ClusterPassTeardown", []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = logger })
	defer hot.Shutdown(context.Background())

	cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	require.NoError(t, cold.Load(ctx))
	idx.shards.Store(tenant, cold)
	t.Cleanup(func() {
		if cold.isLoaded() {
			_ = cold.Shutdown(context.Background())
		}
	})

	require.Contains(t, loadedShardNames(idx), tenant,
		"fixture: the walk collects the tenant while it is still up")

	// What a tenant deactivation does, landing after the name was collected.
	require.NoError(t, cold.Shutdown(ctx))
	require.False(t, cold.isLoaded(), "fixture: the tenant is down before the pass resolves it")

	db := &DB{logger: logger, indices: map[string]*Index{class.Class: idx}}
	db.migrationCluster.db = db
	unresolved, shuttingDown := db.migrationCluster.samplers()

	db.migrationCluster.reconcileShard(ctx, idx, tenant, nil, unresolved, shuttingDown)

	require.False(t, cold.isLoaded(),
		"the pass must leave a deactivated tenant down, not rebuild it outside the shard map")
}

// One record no pass can advance drove a leader query and a walk of every
// loaded shard, once a minute, for the life of the process, with nothing to
// show for it. The wedge outlives the reconciler that found it, so the store
// can answer that this shard has nothing left to decide.
func TestAWedgedRecordStopsCountingAsUndecided(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	require.NoError(t, store.Put(NewMigrationRecordIterating(subject, MigrationCheckpoint{})))
	require.True(t, store.HasUndecided(), "fixture: a record before the flip is undecided")

	store.MarkWedged(subject.Key)
	require.False(t, store.HasUndecided(),
		"a record nothing here can move must stop driving the periodic pass")

	require.NoError(t, store.Load())
	require.True(t, store.HasUndecided(),
		"a load re-derives the wedge, so the pass that follows it decides the record again")
}
