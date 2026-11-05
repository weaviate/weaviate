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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins that a stopped run's cached swap retry pays its debt once a newer run
// of the same migration retired its record, and that an unreadable record
// still fails it.
func TestSwapRetryOfARunANewerRunSuperseded(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unreadable=%t", unreadable), func(t *testing.T) {
			ctx := testCtx()
			className := "LateSwap_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			registerIndex(idx, className)
			for _, obj := range makeConvergenceTestObjects(t, 10, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			node, unit, group := idx.getSchema.NodeName(), shard.migrationUnit(), shard.Name()
			p := NewReindexProvider(idx.db, nil, nil, logrus.New(), node, nil, ctx)
			idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationField,
				BucketStrategy:     shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title")).Strategy(),
				UnitToShard:        map[string]string{unit: group}, UnitToNode: map[string]string{unit: node},
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			prepared := func(version uint64, status distributedtask.TaskStatus) (*distributedtask.Task, []*ShardReindexTaskGeneric) {
				desc := distributedtask.TaskDescriptor{ID: fmt.Sprintf("R%d", version), Version: version}
				tasks, err := p.createReindexTasks(desc, unit, payload)
				require.NoError(t, err)
				for _, task := range tasks {
					require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
					require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				}
				return withUnits(&distributedtask.Task{
					Namespace: ReindexNamespace, TaskDescriptor: desc, Status: status, NeedsPreparationBarrier: true,
					Payload: raw, SwapDecidedGroups: map[string]time.Time{group: {}},
				}, map[string]*distributedtask.Unit{unit: {ID: unit, GroupID: group, NodeID: node}}), tasks
			}

			run1, tasks1 := prepared(1, distributedtask.TaskStatusFailed)
			// What run 1's failed retry leaves behind.
			p.cacheReindexTasks(run1.TaskDescriptor, unit, tasks1)
			run2, _ := prepared(2, distributedtask.TaskStatusSwapping)
			installTestMigrationTaskSources(ctx, idx.db, nil, run1, run2)
			require.NoError(t, p.OnSwapRequested(run2, group, []string{unit}))
			if unreadable {
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
				require.NoError(t, shard.migrationRecords.Load())
			}

			err = p.OnSwapRequested(run1, group, []string{unit})
			if unreadable {
				require.Error(t, err, "an unreadable record is never read as gone")
				return
			}
			require.NoError(t, err, "run 2 retired run 1's record, so nothing of run 1 is left in flight")
		})
	}
}
