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
	"testing"
	"time"

	"github.com/google/uuid"
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

// Pins that a failing swap step returns its fault without logging it: a
// decided swap retries every tick, and the scheduler reports the fault sampled.
func TestFailingSwapStepReturnsItsFaultUnlogged(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, ctx context.Context, f lateSwapFixture) []string
	}{
		{"shard not loaded", func(t *testing.T, ctx context.Context, f lateSwapFixture) []string {
			f.payload.UnitToShard[f.unit] = "cold-tenant"
			return f.resolve(ctx).Errs
		}},
		{"shard cannot be unwrapped", func(t *testing.T, ctx context.Context, f lateSwapFixture) []string {
			f.idx.shards.Store("odd", NewMockShardLike(t))
			t.Cleanup(func() { f.idx.shards.LoadAndDelete("odd") })
			f.payload.UnitToShard[f.unit] = "odd"
			return f.resolve(ctx).Errs
		}},
		{"tasks cannot be built", func(t *testing.T, ctx context.Context, f lateSwapFixture) []string {
			f.payload.Properties = nil
			return f.resolve(ctx).Errs
		}},
		{"unmirrored record refuses the swap", func(t *testing.T, ctx context.Context, f lateSwapFixture) []string {
			tasks, err := f.p.createReindexTasks(lateSwapDesc, f.unit, f.payload)
			require.NoError(t, err)
			for _, task := range tasks {
				subject := task.migrationSubject(f.shard, f.payload.Properties, time.Now())
				subject.Unmirrored = true
				require.NoError(t, f.shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
			}
			return f.p.runShardSwapPhase(ctx, f.payload, f.unit, f.shard.Name(), f.shard, tasks, f.p.logger).Errs
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			f := newLateSwapFixture(t, ctx)
			require.NotEmpty(t, tc.run(t, ctx, f))
			require.Empty(t, f.hook.AllEntries())
		})
	}
}
