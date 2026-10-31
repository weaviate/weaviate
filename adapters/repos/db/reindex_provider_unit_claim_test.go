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
	"github.com/weaviate/weaviate/entities/additional"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func registerIndex(idx *Index, className string) {
	db := idx.db
	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	db.indices[indexID(entschema.ClassName(className))] = idx
}

func TestSwapPhaseLoadsTheShardBeforeClaimingItsUnit(t *testing.T) {
	ctx := testCtx()
	const (
		prop   = "title"
		node   = "node1"
		tenant = "cold-shard"
	)
	// The unit is the tenant shard's own identity; its record store sets any
	// other unit's records aside as foreign.
	unitID := MigrationUnitID(tenant, node)
	className := "SwapClaimOrder" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{prop})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())

	tenantLSM := shardPathLSM(idx.path(), tenant)
	subject := mkMigrationRecordFor(t, tenantLSM, StrategyCodeSearchableRetokenize, "T_swap", 1, unitID,
		ReindexTypeChangeTokenization, MigrationStateSwapped, prop)

	staged := subject.Props[prop].Staged
	canonical := "property_" + prop + "_searchable"
	require.NoError(t, os.MkdirAll(filepath.Join(tenantLSM, staged), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(tenantLSM, staged, "promoted.marker"), []byte(staged), 0o600))
	require.NoDirExists(t, filepath.Join(tenantLSM, canonical))

	cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, cold)
	defer func() {
		if cold.isLoaded() {
			require.NoError(t, cold.Shutdown(context.Background()))
		}
	}()

	registerIndex(idx, className)
	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
	idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())

	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{prop}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{unitID: tenant},
		UnitToNode:  map[string]string{unitID: node},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_swap", Version: 1}

	tasks, err := p.createReindexTasks(desc, unitID, payload)
	require.NoError(t, err)
	seeded := tasks[:1]
	require.Equal(t, StrategyCodeSearchableRetokenize, seeded[0].strategy.StrategyCode(),
		"only the searchable half has a record")
	p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		desc: {unitID: seeded},
	})
	require.False(t, cold.isLoaded(), "the shard has to still be cold when the phase starts")

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, p.OnSwapRequested(&distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusSwapping, NeedsPreparationBarrier: true, Payload: raw,
	}, "g1", []string{unitID}))

	got, err := os.ReadFile(filepath.Join(tenantLSM, canonical, "promoted.marker"))
	require.NoError(t, err, "the canonical name must hold the migrated data")
	require.Equal(t, staged, string(got))
	require.NoDirExists(t, filepath.Join(tenantLSM, staged),
		"promotion renames the staged directory onto the canonical name")
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
		UnitToShard: map[string]string{"u1": hot.Name()},
		UnitToNode:  map[string]string{"u1": "node1"},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_hold", Version: 1}

	tasks, err := p.createReindexTasks(desc, "u1", payload)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		desc: {"u1": tasks},
	})

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusSwapping, Payload: raw,
	}

	ran, sealedDuring := false, false
	require.NoError(t, p.runPerUnitPhase(task, payload, []string{"u1"}, idx, logger, "probe", false,
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
			payload.UnitToShard = map[string]string{"u1": hot.Name()}
			payload.UnitToNode = map[string]string{"u1": "node1"}
			desc := distributedtask.TaskDescriptor{ID: "T_sealed", Version: 1}

			release, sealed := p.SealLocalUnit(desc, "u1")
			require.True(t, sealed)
			defer release()

			rec := newFakeRecorder()
			p.processOneUnit(ctx, &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc,
				Status: distributedtask.TaskStatusStarted,
			}, &payload, idx, "u1", rec)

			require.Empty(t, rec.progress, "a sealed unit reports no progress: it never started")
			require.Empty(t, rec.completed)
			require.Empty(t, rec.failed)
		})
	}
}

// The contention line must be sampled: the scheduler retries every unit it
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
		UnitToShard: map[string]string{},
		UnitToNode:  map[string]string{},
	}
	unitIDs := make([]string, 0, units)
	for i := 0; i < units; i++ {
		id := fmt.Sprintf("u%d__node1", i)
		unitIDs = append(unitIDs, id)
		payload.UnitToShard[id] = hot.Name()
		payload.UnitToNode[id] = "node1"
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}

	// A teardown holds every unit, so the provider declines each in turn.
	for _, id := range unitIDs {
		_, sealed := p.SealLocalUnit(desc, id)
		require.True(t, sealed)
	}
	for _, id := range unitIDs {
		p.processOneUnit(ctx, task, payload, idx, id, nil)
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

// A boot with an unreadable record must not let a unit rerun the swap that deletes the canonical directory.
func TestAUnitDoesNotRunWhileAMigrationRecordIsUnreadable(t *testing.T) {
	bm25 := func(ctx context.Context, s *Shard, query string) int {
		found, _, err := s.ObjectSearch(ctx, 100, nil,
			&searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: query},
			nil, nil, additional.Properties{}, []string{"title"})
		require.NoError(t, err)
		return len(found)
	}
	// The write after the stamp reaches only the canonical directory.
	unmirrored := func(t *testing.T, ctx context.Context, s *Shard, boot func(ShardReindexerV3) *Shard) int {
		stamped, ok := migrationRecordStampedUnmirrored(s.migrationRecords.Records()[0])
		require.True(t, ok)
		require.NoError(t, s.migrationRecords.Put(stamped))
		require.NoError(t, s.Shutdown(ctx))
		u := boot(NewShardReindexerV3Noop())
		require.NoError(t, u.PutObject(ctx, createTestObjectWithText(s.index.Config.ClassName.String(), "betaword")))
		require.NoError(t, u.Shutdown(ctx))
		return 1
	}
	// A newer migration already promoted its data into the canonical directory.
	superseded := func(t *testing.T, ctx context.Context, s *Shard, _ func(ShardReindexerV3) *Shard) int {
		want := bm25(ctx, s, "alpha")
		older := s.migrationRecords.Records()[0].Subject()
		dirs := older.Props["title"]
		newer := older
		newer.Key.TaskVersion++
		newer.TaskID += "-newer"
		newer.Props = map[string]MigrationPropertyDirs{"title": {
			Staged: dirs.Canonical + "__newer_ingest", Sidecar: dirs.Canonical + "__newer_reindex", Canonical: dirs.Canonical,
		}}
		require.NoError(t, s.migrationRecords.Put(NewMigrationRecordPromoted(newer, []string{"title"},
			map[string]string{"title": dirs.Canonical})))
		require.NoError(t, s.Shutdown(ctx))
		canonical := filepath.Join(s.pathLSM(), dirs.Canonical)
		require.NoError(t, os.RemoveAll(canonical))
		require.NoError(t, os.CopyFS(canonical, os.DirFS(filepath.Join(s.pathLSM(), dirs.Staged))))
		return want
	}
	tests := []struct {
		name       string
		query      string
		unreadable bool
		arrange    func(t *testing.T, ctx context.Context, s *Shard, boot func(ShardReindexerV3) *Shard) int
	}{
		{"unmirrored swapped record, another record unreadable", "betaword", true, unmirrored},
		{"superseded swapped record, another record unreadable", "alpha", true, superseded},
		{"unmirrored swapped record, every record readable", "betaword", false, unmirrored},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Frozen_" + uuid.NewString()[:8]
			class := newRebuildSearchableTestClass(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeConvergenceTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			name := shard.Name()
			unit := testMigrationUnitFor(idx, name)
			newTask := func() *ShardReindexTaskGeneric {
				task, _ := newRebuildSearchableTask(t, idx, className, "title", unit)
				return task
			}
			task := newTask()
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))
			boot := func(reindexer ShardReindexerV3) *Shard {
				idx.shardReindexer = reindexer
				reloaded, err := idx.initShard(ctx, name, class, nil, true, true)
				require.NoError(t, err)
				idx.shards.Store(name, reloaded)
				return reloaded.(*Shard)
			}
			bad := filepath.Join(shard.migrationRecords.Dir(), "99_searchable_retokenize_"+shard.migrationUnit()+".json")
			if tc.unreadable {
				require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))
			}
			want := tc.arrange(t, ctx, shard, boot)

			frozen := boot(&testShardReindexer{task: newTask()})
			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeRebuildSearchable, Properties: []string{"title"},
				UnitToShard: map[string]string{unit: name}, UnitToNode: map[string]string{unit: "node1"},
			}
			rec := newFakeRecorder()
			p.processOneUnit(ctx, &distributedtask.Task{
				Namespace: ReindexNamespace, Status: distributedtask.TaskStatusStarted,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "test-rebuild-searchable", Version: 1},
			}, payload, idx, unit, rec)
			require.NoError(t, frozen.Shutdown(ctx))

			if tc.unreadable {
				require.NoError(t, os.Remove(bad))
			}
			clean := boot(&testShardReindexer{task: newTask()})
			defer clean.Shutdown(ctx)
			require.Equal(t, want, bm25(ctx, clean, tc.query))
			require.Equal(t, !tc.unreadable, slices.Contains(rec.completed, unit), rec.failed)
		})
	}
}
