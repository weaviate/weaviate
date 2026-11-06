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
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Pins that a tenant set COLD while its swap is owed loses no write taken
// after it is reactivated: the reload arms the mirror, and the late swap
// installs a bucket that holds the write.
func TestWritesAfterReactivatingATenantThatOwesASwapSurviveTheSwap(t *testing.T) {
	const prop, node = "title", "node1"
	ctx := testCtx()
	className := "ColdHot_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{prop})
	class.Properties[0].IndexFilterable, class.Properties[0].IndexSearchable = boolPtr(true), boolPtr(true)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
	shard := shd.(*Shard)
	registerIndex(idx, className)
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "seed")))

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
	idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())
	recovery := NewReindexRecoveryHook(true, logger, nil)
	recovery.AttachProvider(p)
	idx.shardReindexer = recovery

	unitID, group := shard.migrationUnit(), shard.Name()
	payload := ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{prop},
		TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{unitID: group}, UnitToNode: map[string]string{unitID: node},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := func(status distributedtask.TaskStatus) *distributedtask.Task {
		return withUnits(&distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
			Status: status, Payload: raw, NeedsPreparationBarrier: true,
			SwapDecidedGroups: map[string]time.Time{group: time.Now()}, SwapsOwed: map[string][]string{group: {node}},
		}, map[string]*distributedtask.Unit{unitID: {ID: unitID, GroupID: group, NodeID: node}})
	}
	p.processOneUnit(ctx, task(distributedtask.TaskStatusStarted), &payload, idx, unitID, newFakeRecorder())
	require.NoError(t, p.OnGroupCompleted(task(distributedtask.TaskStatusPreparing), group, []string{unitID}))
	cancelled := task(distributedtask.TaskStatusCancelled)
	installTestMigrationTaskSources(ctx, idx.db, nil, cancelled)
	require.NoError(t, p.OnTaskCompleted(cancelled))

	mig := &Migrator{db: idx.db, logger: logger}
	for _, status := range []string{models.TenantActivityStatusCOLD, models.TenantActivityStatusHOT} {
		require.NoError(t, mig.UpdateTenants(ctx, class,
			[]*schemaUC.UpdateTenantPayload{{Name: group, Status: status}}, false))
	}
	reloaded, err := unwrapShard(ctx, idx.shards.Load(group))
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	written := createTestObjectWithText(className, "afterhot")
	require.NoError(t, reloaded.PutObject(ctx, written))
	require.NoError(t, p.OnSwapRequested(cancelled, group, []string{unitID}))

	filtered, _, err := reloaded.ObjectSearch(ctx, 10, propEqualsFilter(className, prop, "afterhot"), nil, nil, nil, additional.Properties{}, nil)
	require.NoError(t, err)
	require.Contains(t, objectIDs(filtered), written.ID().String(), "the filter finds the write")
	ranked, _, err := reloaded.ObjectSearch(ctx, 10, nil,
		&searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: "afterhot"},
		nil, nil, additional.Properties{}, []string{prop})
	require.NoError(t, err)
	require.Contains(t, objectIDs(ranked), written.ID().String(), "and so does BM25")
}
