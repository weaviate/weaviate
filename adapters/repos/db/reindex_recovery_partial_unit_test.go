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
	"strings"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type changeTokenizationUnit struct {
	shard   *Shard
	idx     *Index
	p       *ReindexProvider
	payload *ReindexTaskPayload
	desc    distributedtask.TaskDescriptor
	unitID  string
	tasks   []*ShardReindexTaskGeneric
}

func newChangeTokenizationUnit(t *testing.T, ctx context.Context) changeTokenizationUnit {
	className := "Unit" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })
	registerIndex(idx, className)
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "seed")))

	logger, _ := logrustest.NewNullLogger()
	unitID := shard.migrationUnit()
	u := changeTokenizationUnit{
		shard: shard, idx: idx, unitID: unitID,
		p: NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx),
		payload: &ReindexTaskPayload{
			Collection: className, MigrationType: ReindexTypeChangeTokenization,
			Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
			UnitToShard: map[string]string{unitID: shard.Name()},
			UnitToNode:  map[string]string{unitID: "node1"},
		},
		desc: distributedtask.TaskDescriptor{ID: "T_" + className, Version: 1},
	}
	var err error
	u.tasks, err = u.p.createReindexTasks(u.desc, unitID, u.payload)
	require.NoError(t, err)
	require.Len(t, u.tasks, 2, "change-tokenization on a property with both indexes runs two halves")
	return u
}

func (u changeTokenizationUnit) process(t *testing.T, ctx context.Context) *fakeRecorder {
	raw, err := json.Marshal(u.payload)
	require.NoError(t, err)
	rec := newFakeRecorder()
	u.p.processOneUnit(ctx, &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: u.desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}, u.payload, u.idx, u.unitID, rec, &keyDocColumnDrops{})
	return rec
}

// Running only the seeded half would flip the schema for both, so the
// never-started half is built from the task payload and the unit runs whole.
func TestAPartiallySeededUnitRebuildsItsNeverStartedHalf(t *testing.T) {
	ctx := testCtx()
	u := newChangeTokenizationUnit(t, ctx)

	// Only the searchable half carried a record when the node came back.
	u.p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		u.desc: {u.unitID: u.tasks[:1]},
	})

	rec := u.process(t, ctx)

	require.Empty(t, rec.failed,
		"a never-started half is built from the task payload instead of failing the unit")
	require.Contains(t, rec.completed, u.unitID)
	cached := u.p.cachedReindexTasks(u.desc, u.unitID)
	require.Len(t, cached, 2, "the cache holds both halves, so the swap phase covers them both")
	require.Same(t, u.tasks[0], cached[0], "the seeded half keeps the instance its mirror is armed on")
}

func TestAMigrationStampedUnmirroredFailsBeforeItIterates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		unit      bool
		rehydrate bool
	}{
		{name: "the unit", unit: true},
		{name: "prep's rehydrate", rehydrate: true},
		{name: "prepare's resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			u := newChangeTokenizationUnit(t, ctx)
			for _, task := range u.tasks {
				require.NoError(t, startOnShard(ctx, task, u.shard))
			}
			rec, ok := u.shard.migrationRecords.Get(u.tasks[0].migrationRecordKey())
			require.True(t, ok)
			stamped := rec.Subject()
			stamped.Unmirrored = true
			require.NoError(t, u.shard.migrationRecords.Put(NewMigrationRecordIterating(stamped, MigrationCheckpoint{})))

			var refusal string
			if tc.unit {
				refusal = u.process(t, ctx).failed[u.unitID]
			} else {
				logger, _ := logrustest.NewNullLogger()
				ok, out := u.p.runShardPrepPhase(ctx, u.unitID, u.shard, u.tasks, tc.rehydrate, logger)
				require.False(t, ok)
				refusal = strings.Join(out.Errs, "; ")
			}
			require.Contains(t, refusal, "no double-write mirror armed")
			rec, ok = u.shard.migrationRecords.Get(u.tasks[0].migrationRecordKey())
			require.True(t, ok)
			require.Equal(t, MigrationStateIterating, rec.State(), "the stamped migration is refused before it iterates")
		})
	}
}
