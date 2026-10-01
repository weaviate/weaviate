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
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Boot recovery reads every record as a migration to re-arm, so a cancelled
// task's records have to be gone from every shard this node holds by the time
// its cleanup returns; a restart in between would rebuild the cancelled task.
func TestTerminalCleanupSettlesTheTasksRecordsOnTheShardsItHolds(t *testing.T) {
	const (
		unloadedB = "unloaded-b"
		loadedC   = "loaded-c"
	)
	ctx := testCtx()
	className := "TerminalCleanupRecords_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title", "body"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	loadedA := shd.(*Shard)
	defer loadedA.Shutdown(context.Background())

	lazyShard := func(name string) *LazyLoadShard {
		lazy := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
		idx.shards.Store(name, lazy)
		t.Cleanup(func() {
			if lazy.isLoaded() {
				require.NoError(t, lazy.Shutdown(context.Background()))
			}
		})
		return lazy
	}
	subjectOf := func(taskID string, version uint64, shardName, prop string) MigrationSubject {
		subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, prop)
		subject.TaskID, subject.Key.UnitID = taskID, testMigrationUnitFor(idx, shardName)
		return subject
	}

	require.NoError(t, loadedA.migrationRecords.Put(NewMigrationRecordIterated(subjectOf("T", 1, loadedA.Name(), "title"))))
	require.NoError(t, loadedA.migrationRecords.Put(NewMigrationRecordIterated(subjectOf("L", 2, loadedA.Name(), "body"))))

	logger, _ := test.NewNullLogger()
	lazyShard(unloadedB)
	lsmB := shardPathLSM(idx.path(), unloadedB)
	require.NoError(t, os.MkdirAll(lsmB, 0o777))
	require.NoError(t, NewMigrationRecordStore(lsmB, logger).Put(NewMigrationRecordMerged(subjectOf("T", 1, unloadedB, "title"))))

	c := lazyShard(loadedC)
	require.NoError(t, c.Load(ctx))
	swapped := subjectOf("T", 1, loadedC, "title")
	require.NoError(t, c.shard.migrationRecords.Put(NewMigrationRecordSwapped(swapped, swapped.Properties(),
		map[string]string{"title": swapped.Props["title"].Canonical})))

	registerIndex(idx, className)
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())

	p.autoCleanupAfterTerminal(testTask("T", 1, distributedtask.TaskStatusCancelled), &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"a": loadedA.Name(), "b": unloadedB, "c": loadedC},
	}, logger)

	recovered, err := DiscoverInFlightReindexTasks(idx.Config.RootPath, logger, nil)
	require.NoError(t, err)
	var rebuilt []string
	for _, rr := range recovered {
		rebuilt = append(rebuilt, rr.Descriptor.ID+" on "+rr.ShardName)
	}
	require.ElementsMatch(t, []string{"L on " + loadedA.Name(), "T on " + loadedC}, rebuilt,
		"only another task's record and a flip the cancel came too late for may survive the cleanup")
}

// The walk folds shards by max, so the outcome order is what makes a later
// clean shard unable to hide one that kept its records.
func TestTerminalCleanupReportsTheWorstShardNotTheLast(t *testing.T) {
	require.Greater(t, CleanupSweepFailed, CleanupSweepUnknown, "knowing state is left outranks not knowing")
	require.Greater(t, CleanupSweepUnknown, CleanupSweepDropped, "an unvisited shard outranks a collection going away")
	require.Greater(t, CleanupSweepDropped, CleanupSweepClean)

	idx, failing := shardWithAnUnreadableRecordStore(t)
	logger, _ := test.NewNullLogger()
	p := NewReindexProvider(&DB{indices: map[string]*Index{indexID(idx.Config.ClassName): idx}},
		nil, nil, logger, "n1", nil, testCtx())

	for _, shards := range [][]string{{failing, "absent-tenant"}, {"absent-tenant", failing}} {
		outcome, _ := p.discardTaskRecords(testCtx(), testTask("T", 1, distributedtask.TaskStatusCancelled),
			string(idx.Config.ClassName), shards)
		require.Equal(t, CleanupSweepFailed, outcome, "shards %v", shards)
	}
}
