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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A commit verdict on a record whose rebuild never finished can never be acted
// on, so an unmatched pass re-asks the leader every minute forever.
func TestACommitVerdictOnAnUnfinishedRebuildTerminates(t *testing.T) {
	const taskID = "Books:change-tokenization:title:ab12"

	tests := []struct {
		name   string
		record func(MigrationSubject) MigrationRecord
	}{
		{
			name:   "iterating",
			record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
		},
		{
			name:   "iterated",
			record: func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			// Effect in the schema, owning task gone: a finished migration once
			// the task TTL has removed it.
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title")
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TaskID = taskID
			f.mkdirs("property_title__g42_ingest", "property_title__s42_reindex", "property_title")
			f.put(tt.record(subject))
			require.NoError(t, f.store.Load())
			require.True(t, f.store.HasUndecided())

			r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
			r.ReconcileWithClusterTasks(context.Background(), []*distributedtask.Task{})

			require.Equal(t, 1, r.WedgedCount())
			require.NotEmpty(t, f.errorLines("the cluster reports it committed"))
			require.False(t, f.store.HasUndecided(),
				"a record no verdict can settle must stop driving the leader query")

			// The flip never ran, so the canonical bucket is the only complete copy.
			require.True(t, f.exists("property_title"))
			require.Equal(t, "property_title", f.contentOf("property_title"))
			state, present := f.state(subject.Key)
			require.True(t, present)
			require.Equal(t, tt.record(subject).State(), state)

			// A second pass says nothing more.
			before := len(f.errorLines("the cluster reports it committed"))
			r2 := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
			r2.ReconcileWithClusterTasks(context.Background(), []*distributedtask.Task{})
			require.Equal(t, before, len(f.errorLines("the cluster reports it committed")),
				"one line for the record, not one per pass")
		})
	}
}

// A record a task verdict wedges is never committed, so its mirror would only
// copy writes nobody reads. Another migration's mirror keeps running.
func TestATaskVerdictWedgeStopsOnlyThatRecordsMirror(t *testing.T) {
	tests := []struct {
		name          string
		migrationType ReindexMigrationType
		record        func(MigrationSubject) MigrationRecord
	}{
		{
			name:          "the leader's list no longer holds a migration the schema cannot show",
			migrationType: ReindexTypeRepairRangeable,
			record:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
		{
			name:          "the cluster committed a migration this replica never finished",
			migrationType: ReindexTypeChangeTokenization,
			record:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, "title", "body")
			wedging := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")
			wedging.MigrationType = tt.migrationType
			running := testMigrationSubject(50, StrategyCodeSearchableRetokenize, "title")
			running.TaskID = "running"
			f.tasks = []*distributedtask.Task{testTask(running.TaskID, 50, distributedtask.TaskStatusStarted)}
			f.put(tt.record(wedging))
			f.put(NewMigrationRecordIterated(running))
			require.NoError(t, f.store.Load())

			newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps()).
				ReconcileWithClusterTasks(context.Background(), f.tasks)

			require.True(t, f.store.Wedged(wedging.Key), "fixture: the pass has to wedge the record")
			require.ElementsMatch(t, []migrationMirrorKey{{wedging.Key, "title"}, {wedging.Key, "body"}}, f.disarmed)
			require.ElementsMatch(t, migrationOwnedDirs(wedging), f.buckets.closed)
		})
	}
}

// A worker can outlive its task (a cancel with a zero task TTL) and still swap
// its staged index in, so a pass that cannot take its seal must leave the
// mirror copying every write until then.
func TestARefusedWedgeKeepsTheWorkersMirror(t *testing.T) {
	const written = int64(777777)

	for _, tt := range []struct {
		name            string
		cancelTheWorker bool
	}{
		{name: "the user cancelled the worker", cancelTheWorker: true},
		{name: "the worker has not seen the cancel yet"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RefusedWedge_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)
			rangeable := true
			class.Properties[0].IndexRangeFilters = &rangeable
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			t.Cleanup(func() { shard.Shutdown(context.Background()) })
			for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, filterableToRangeablePropName, shard.migrationUnit())
			task.setMigrationIdentity(distributedtask.TaskDescriptor{ID: "repair", Version: 1}, shard.migrationUnit(),
				&ReindexTaskPayload{MigrationType: ReindexTypeRepairRangeable, Collection: className})
			workerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			require.NoError(t, task.RunReindexOnlyOnShard(workerCtx, shard))
			require.NoError(t, task.RunPrepareOnShard(workerCtx, shard))
			if tt.cancelTheWorker {
				cancel()
			}

			r := shard.migrationReconciler(func() *models.Class { return class })
			r.deps.LocalTasks = func() ([]*distributedtask.Task, bool) { return nil, true }
			r.deps.SealUnit = func(distributedtask.TaskDescriptor, string) (func(), bool) { return nil, false }
			r.ReconcileWithClusterTasks(context.Background(), nil)

			require.NoError(t, shard.PutObject(context.Background(), &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:         strfmt.UUID(uuid.NewString()),
					Class:      className,
					Properties: map[string]interface{}{filterableToRangeablePropName: written},
				},
			}))
			swapErr := task.RunSwapOnShard(workerCtx, shard)
			if !tt.cancelTheWorker {
				require.NoError(t, swapErr)
			}

			rec, ok := task.migrationRecord(shard)
			require.True(t, ok)
			require.Equal(t, MigrationStateSwapped, rec.State())
			served := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
			require.NotNil(t, served)
			require.NotEmpty(t, readRangeableIDs(t, served, 0), "fixture: the corpus is served")
			require.Len(t, readRangeableIDs(t, served, written), 1, "the write made after the pass")
			require.Len(t, rangeableDocIDsAtLeast(t, served, 0), 26)
		})
	}
}

// A write moves the record on, so the next pass has something new to decide.
func TestAWriteClearsTheWedgeThatStoppedTheLeaderQuery(t *testing.T) {
	f := newReconcileFixture(t)
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.put(NewMigrationRecordIterated(subject))
	require.NoError(t, f.store.Load())

	f.store.MarkWedged(subject.Key)
	require.False(t, f.store.HasUndecided())

	require.NoError(t, f.store.Put(NewMigrationRecordMerged(subject)))
	require.True(t, f.store.HasUndecided())
}
