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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var lateSwapDesc = distributedtask.TaskDescriptor{ID: "R1", Version: 1}

type lateSwapFixture struct {
	p       *ReindexProvider
	shard   *Shard
	idx     *Index
	unit    string
	payload *ReindexTaskPayload
	hook    *logrustest.Hook
}

func newLateSwapFixture(t *testing.T, ctx context.Context) lateSwapFixture {
	className := "LateSwap_" + uuid.NewString()[:8]
	shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	registerIndex(idx, className)
	unit := shard.migrationUnit()
	logger, hook := logrustest.NewNullLogger()
	return lateSwapFixture{
		p: NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx), shard: shard, idx: idx, unit: unit, hook: hook,
		payload: &ReindexTaskPayload{
			Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
			TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
			UnitToShard: map[string]string{unit: shard.Name()},
		},
	}
}

func (f lateSwapFixture) resolve(ctx context.Context) phaseUnitResolution {
	return f.p.resolveUnitForPhase(ctx, &distributedtask.Task{TaskDescriptor: lateSwapDesc}, f.payload, f.unit, f.idx, f.p.logger)
}

// Pins that a stopped run's swap retry, cached or rebuilt after a restart,
// pays its debt once a newer run of the same migration retired its record,
// and that an unreadable record still fails it.
func TestSwapRetryOfARunANewerRunSuperseded(t *testing.T) {
	for _, tc := range []struct{ cached, unreadable bool }{
		{false, false}, {false, true}, {true, false}, {true, true},
	} {
		cached, unreadable := tc.cached, tc.unreadable
		t.Run(fmt.Sprintf("cached=%t/unreadable=%t", cached, unreadable), func(t *testing.T) {
			ctx := testCtx()
			f := newLateSwapFixture(t, ctx)
			// Never run, so no record: as after the newer run retired it.
			if cached {
				tasks, err := f.p.createReindexTasks(lateSwapDesc, f.unit, f.payload)
				require.NoError(t, err)
				f.p.cacheReindexTasks(lateSwapDesc, f.unit, tasks)
			}
			if unreadable {
				plantUnreadableRecord(t, f.shard.migrationRecords.Dir())
				require.NoError(t, f.shard.migrationRecords.Load())
			}

			got := f.resolve(ctx)
			require.Equal(t, unreadable, len(got.Errs) > 0, "an unreadable record is never read as gone")
			require.Equal(t, !unreadable, got.Skip, "a retired record leaves nothing of the run in flight")
			for range maxReportedErrors {
				f.resolve(ctx)
			}
			require.LessOrEqual(t, len(f.hook.AllEntries()), maxReportedErrors, "a retry per tick logs only sampled lines")
		})
	}
}

// Pins that a decided swap retried every tick logs each task's first attempt
// in full and its retries only within the node-wide sample.
func TestFailingSwapRetryLogsSampled(t *testing.T) {
	ctx := testCtx()
	f := newLateSwapFixture(t, ctx)
	tasks, err := f.p.createReindexTasks(lateSwapDesc, f.unit, f.payload)
	require.NoError(t, err)
	for _, task := range tasks {
		subject := task.migrationSubject(f.shard, f.payload.Properties, time.Now())
		require.NoError(t, f.shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
	}
	// Leave one sampled slot, so a first attempt that waited for one would go unlogged.
	for range maxReportedErrors - 1 {
		f.p.swapRetrySampler().WithSampling(func(logrus.FieldLogger) {})
	}
	// No staged bucket is open, so every attempt fails.
	for range 3 * maxReportedErrors {
		require.NotEmpty(t, f.p.runShardSwapPhase(ctx, f.payload, f.unit, f.shard.Name(), f.shard, tasks, f.p.logger).Errs)
	}

	count := func(msg string) (n int) {
		for _, e := range f.hook.AllEntries() {
			if e.Message == msg {
				n++
			}
		}
		return n
	}
	require.Equal(t, len(tasks), count("RunSwapOnShard: resuming from merged state, in-memory atomic swap"))
	retries := count("RunSwapOnShard: flip already decided; running OnMigrationComplete only")
	require.Positive(t, retries)
	require.LessOrEqual(t, retries, maxReportedErrors)
}

// Pins that a decided swap retried every tick, because a sibling unit keeps
// failing, logs the swapped unit's "swap complete" once.
func TestDecidedSwapRetryLogsSwapCompleteOnce(t *testing.T) {
	ctx, shard, idx, className := newRangeableFinalizeTestShard(t, "SwapRetry_")
	unit := shard.migrationUnit()
	rt, _ := newRangeableInMemoryTestTask(t, idx, className, filterableToRangeablePropName,
		unit, &atomic.Bool{}, &atomic.Int32{})
	require.NoError(t, rt.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, rt.RunPrepareOnShard(ctx, shard))
	registerIndex(idx, className)
	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
	p.cachePayload(lateSwapDesc, &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeEnableRangeable,
		Properties: []string{filterableToRangeablePropName},
		// The sibling's shard does not exist, so its swap fails on every tick.
		UnitToShard: map[string]string{unit: shard.Name(), "sibling": "missing"},
	})
	p.cacheReindexTasks(lateSwapDesc, unit, []*ShardReindexTaskGeneric{rt})
	task := withUnits(&distributedtask.Task{
		TaskDescriptor: lateSwapDesc, Status: distributedtask.TaskStatusFailed,
		NeedsPreparationBarrier: true, SwapDecidedGroups: map[string]time.Time{"": {}},
	}, map[string]*distributedtask.Unit{
		unit:      {ID: unit, NodeID: "node1", Status: distributedtask.UnitStatusCompleted},
		"sibling": {ID: "sibling", NodeID: "node1", Status: distributedtask.UnitStatusCompleted},
	})

	for range 3 * maxReportedErrors {
		require.Error(t, p.OnSwapRequested(task, "", task.LocalGroupUnitIDs("", "node1")))
	}
	complete := 0
	for _, e := range hook.AllEntries() {
		if e.Message == "reindex provider: swap complete" {
			complete++
		}
	}
	require.Equal(t, 1, complete)
}
