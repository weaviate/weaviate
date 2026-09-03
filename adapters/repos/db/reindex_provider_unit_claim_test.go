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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
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
	// The unit is the tenant shard's own identity: its record store sets
	// records of any other unit aside as foreign on load.
	unitID := MigrationUnitID(tenant, node)
	className := "SwapClaimOrder" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{prop})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())

	tenantLSM := shardPathLSM(idx.path(), tenant)
	trackerDir := migrationDirWithProps(MigrationDirPrefixSearchableRetokenize, []string{prop}) + "_1"
	mkMigrationRecordFor(t, tenantLSM, trackerDir, "T_swap", 1, unitID,
		ReindexTypeChangeTokenization, MigrationStateSwapped, prop)

	staged := "property_" + prop + "__" + trackerDir + "_ingest"
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
		Properties: []string{prop}, TargetTokenization: "field", BucketStrategy: "MapCollection",
		UnitToShard: map[string]string{unitID: tenant},
		UnitToNode:  map[string]string{unitID: node},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_swap", Version: 1}

	seeded, err := p.createReindexTasks(desc, unitID, payload, tenantLSM, true)
	require.NoError(t, err)
	require.NotEmpty(t, seeded)
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
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: "MapCollection",
		UnitToShard: map[string]string{"u1": hot.Name()},
		UnitToNode:  map[string]string{"u1": "node1"},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_hold", Version: 1}

	concrete, err := unwrapShard(ctx, hot)
	require.NoError(t, err)
	tasks, err := p.createReindexTasks(desc, "u1", payload, concrete.pathLSM(), false)
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
				TargetTokenization: "field", BucketStrategy: "MapCollection",
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

// TestUnitsHeldByATeardownReportAtASampledRate pins the contention line to a
// sampled rate. The scheduler retries every unit it could not start, and a
// unit count is a tenant count, so one line per unit per tick has no end while
// the teardown runs.
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
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: "MapCollection",
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

	// A teardown holds every unit, which is what makes the provider decline
	// each of them in turn.
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
