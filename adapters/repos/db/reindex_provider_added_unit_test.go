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
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestAUnitAddedByTheTaskAddApplyRunsToItsSwap(t *testing.T) {
	ctx := testCtx()
	const node = "node1"
	className := "AddedUnit" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())
	registerIndex(idx, className)
	for _, obj := range makeConvergenceTestObjects(t, 25, className) {
		require.NoError(t, shd.PutObject(ctx, obj))
	}

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, node, func() int { return 1 }, ctx)
	rec := newFakeRecorder()
	p.SetCompletionRecorder(rec)

	// The payload maps were built before the tenant existed, so they list only another tenant's unit.
	raw, err := json.Marshal(&ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
		TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{"other__node2": "other"}, UnitToNode: map[string]string{"other__node2": "node2"},
	})
	require.NoError(t, err)
	group, unitID := shd.Name(), MigrationUnitID(shd.Name(), node)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_added", Version: 1},
		Status: distributedtask.TaskStatusStarted, NeedsPreparationBarrier: true, Payload: raw,
		Units: map[string]*distributedtask.Unit{
			unitID:         {ID: unitID, GroupID: group},
			"other__node2": {ID: "other__node2", GroupID: "other"},
		},
	}

	handle, err := p.StartTask(task)
	require.NoError(t, err)
	<-handle.Done()
	require.Empty(t, rec.failed)
	require.Equal(t, []string{unitID}, rec.completed, "this node runs the unit")

	task.Status = distributedtask.TaskStatusPreparing
	require.NoError(t, p.OnGroupCompleted(task, group, []string{unitID}))
	task.Status = distributedtask.TaskStatusSwapping
	task.SwapDecidedGroups = map[string]time.Time{group: {}}
	require.NoError(t, p.OnSwapRequested(task, group, []string{unitID}))

	states := migrationRecordStates(t, shd.(*Shard).pathLSM())
	require.NotEmpty(t, states)
	for _, state := range states {
		require.Equal(t, MigrationStateSwapped, state)
	}
}
