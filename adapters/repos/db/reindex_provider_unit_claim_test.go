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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func registerIndex(idx *Index, className string) {
	db := idx.db
	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	db.indices[indexID(entschema.ClassName(className))] = idx
}

func TestAPhaseHoldsItsUnitWhileItRuns(t *testing.T) {
	ctx := testCtx()
	className := "UnitHeld" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
	}
	desc := distributedtask.TaskDescriptor{ID: "T_hold", Version: 1}

	tasks, err := p.createReindexTasks(desc, "u1", payload)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	// Recovery seeds only tasks whose record is on disk.
	for _, task := range tasks {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, hot))
	}
	p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		desc: {"u1": tasks},
	})

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusSwapping, Payload: raw,
		Units: map[string]*distributedtask.Unit{"u1": {ID: "u1", Shard: hot.Name(), NodeID: "node1"}},
	}

	ran, sealedDuring := false, false
	require.NoError(t, p.runPerUnitPhase(task, payload, []string{"u1"}, idx, logger, "probe", "starting", false,
		func(unitID string, _ ShardLike, _ []*ShardReindexTaskGeneric, _ bool) phaseResult {
			ran = true
			if release, sealed := p.SealLocalUnit(desc, unitID); sealed {
				sealedDuring = true
				release()
			}
			return phaseResult{}
		}))

	require.True(t, ran, "the phase has to run for the claim to mean anything")
	require.False(t, sealedDuring)
	release, sealed := p.SealLocalUnit(desc, "u1")
	require.True(t, sealed, "the claim has to be released when the phase returns")
	release()
}

func TestASealedUnitNeverStartsItsIteration(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload ReindexTaskPayload
	}{
		{
			name: "semantic",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
				TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
			},
		},
		{
			name:    "format-only",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"title"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SealedUnit" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			defer hot.Shutdown(context.Background())
			registerIndex(idx, className)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

			payload := tt.payload
			payload.Collection = className
			desc := distributedtask.TaskDescriptor{ID: "T_sealed", Version: 1}

			release, sealed := p.SealLocalUnit(desc, "u1")
			require.True(t, sealed)
			defer release()

			rec := newFakeRecorder()
			require.True(t, p.processOneUnit(ctx, &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc,
				Status: distributedtask.TaskStatusStarted,
				Units:  map[string]*distributedtask.Unit{"u1": {ID: "u1", Shard: hot.Name(), NodeID: "node1"}},
			}, &payload, idx, "u1", rec), "a sealed unit is retried")

			require.Empty(t, rec.progress, "a sealed unit reports no progress: it never started")
			require.Empty(t, rec.completed)
			require.Empty(t, rec.failed)
		})
	}
}

// The contention line must be sampled: the handle retries every unit it
// could not start, and a unit count is a tenant count.
func TestUnitsHeldByATeardownReportAtASampledRate(t *testing.T) {
	ctx := testCtx()
	className := "UnitContention" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())
	registerIndex(idx, className)

	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

	const units = 40
	desc := distributedtask.TaskDescriptor{ID: "T_contended", Version: 1}
	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
		Units: map[string]*distributedtask.Unit{},
	}
	unitIDs := make([]string, 0, units)
	for i := 0; i < units; i++ {
		id := fmt.Sprintf("u%d__node1", i)
		unitIDs = append(unitIDs, id)
		task.Units[id] = &distributedtask.Unit{ID: id, Shard: hot.Name(), NodeID: "node1"}
	}

	// A teardown holds every unit, so the provider declines each in turn.
	for _, id := range unitIDs {
		_, sealed := p.SealLocalUnit(desc, id)
		require.True(t, sealed)
	}
	for _, id := range unitIDs {
		require.True(t, p.processOneUnit(ctx, task, payload, idx, id, nil))
	}

	held := 0
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "a teardown holds this unit") {
			held++
		}
	}
	require.Positive(t, held, "the contention is still reported")
	require.LessOrEqual(t, held, maxReportedErrors,
		"sampled per window, not emitted once per unit")
	require.Less(t, held, units)
}

func TestTheUnreadableRecordGuardNamesTheBadFilesUpToACap(t *testing.T) {
	tests := []struct {
		name      string
		bad       int
		wantNamed int
		wantMore  string
	}{
		{name: "every bad file fits the cap", bad: maxNamedUnreadableRecords, wantNamed: maxNamedUnreadableRecords},
		{name: "one past the cap", bad: maxNamedUnreadableRecords + 1, wantNamed: maxNamedUnreadableRecords, wantMore: "(and 1 more)"},
		{name: "more bad files than the cap", bad: maxNamedUnreadableRecords + 3, wantNamed: maxNamedUnreadableRecords, wantMore: "(and 3 more)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
			var files []string
			for i := range tc.bad {
				name := fmt.Sprintf("%02d_enable_searchable.json", i)
				require.NoError(t, os.WriteFile(filepath.Join(store.Dir(), name), []byte("{"), 0o600))
				files = append(files, name)
			}
			require.NoError(t, store.Load())

			fault := unreadableRecordFault(store)
			require.True(t, strings.HasPrefix(fault, fmt.Sprintf("%d migration record fault(s)", tc.bad)), fault)
			for i, name := range files {
				require.Equal(t, i < tc.wantNamed, strings.Contains(fault, `"`+name+`"`), fault)
			}
			if tc.wantMore != "" {
				require.Contains(t, fault, tc.wantMore)
			} else {
				require.NotContains(t, fault, "more)")
			}
		})
	}
}

// Pins that a stopped run's cached swap retry pays its debt once a newer run retired its record.
func TestSwapRetryOfARunANewerRunSuperseded(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unreadable=%t", unreadable), func(t *testing.T) {
			ctx := testCtx()
			className := "LateSwap_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
			registerIndex(idx, className)
			unit := shard.migrationUnit()
			logger, hook := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
			}
			desc := distributedtask.TaskDescriptor{ID: "R1", Version: 1}
			tasks, err := p.createReindexTasks(desc, unit, payload)
			require.NoError(t, err)
			p.cacheReindexTasks(desc, unit, tasks)
			if unreadable {
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
				require.NoError(t, shard.migrationRecords.Load())
			}
			resolve := func() phaseUnitResolution {
				return p.resolveUnitForPhase(ctx, &distributedtask.Task{TaskDescriptor: desc, Units: map[string]*distributedtask.Unit{
					unit: {ID: unit, Shard: shard.Name()},
				}}, payload, unit, idx, p.logger)
			}

			got := resolve()
			require.Equal(t, unreadable, len(got.Errs) > 0, "an unreadable record is never read as gone")
			require.Equal(t, !unreadable, got.Skip, "a retired record leaves nothing of the run in flight")
			for range maxReportedErrors {
				resolve()
			}
			require.LessOrEqual(t, len(hook.AllEntries()), maxReportedErrors, "a retry per tick logs only sampled lines")
		})
	}
}

func TestPayloadOfKeepsTheTaskListedUntilCleanup(t *testing.T) {
	p := NewReindexProvider(nil, nil, nil, logrus.New(), "node-0", nil, nil)
	raw, err := json.Marshal(ReindexTaskPayload{Collection: "Docs", MigrationType: ReindexTypeChangeTokenization})
	require.NoError(t, err)
	task := loadTasks(t, ReindexNamespace, &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
		Status:         distributedtask.TaskStatusFailed, Payload: raw,
	})[0]

	_, err = p.payloadOf(task)
	require.NoError(t, err)
	require.Equal(t, []distributedtask.TaskDescriptor{task.TaskDescriptor}, p.GetLocalTasks())

	require.NoError(t, p.CleanupTask(task.TaskDescriptor))
	require.Empty(t, p.GetLocalTasks())
}

// Each unit is bound to one node; another node's unit names a shard this node does not hold.
func TestStartTaskWorksOnlyThisNodesUnits(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): {}}}
	p := NewReindexProvider(db, nil, nil, logger, "node1", func() int { return 2 }, context.Background())
	rec := newFakeRecorder()
	p.SetCompletionRecorder(rec)
	raw, err := json.Marshal(ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeRepairFilterable, Properties: []string{"title"}})
	require.NoError(t, err)

	handle, err := p.StartTask(loadTasks(t, ReindexNamespace, &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
		Status:         distributedtask.TaskStatusStarted, Payload: raw,
		Units: map[string]*distributedtask.Unit{
			"s1__node1": {ID: "s1__node1", Shard: "s1", NodeID: "node1"},
			"s2__node2": {ID: "s2__node2", Shard: "s2", NodeID: "node2"},
		},
	})[0])
	require.NoError(t, err)
	<-handle.Done()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []string{"s1__node1"}, slices.Collect(maps.Keys(rec.failed)), "this node fails its own unit, whose shard it lacks")
}
