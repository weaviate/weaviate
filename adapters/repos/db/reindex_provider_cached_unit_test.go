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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A restart between a tokenization change's two swaps, with the unswapped half's
// record unreadable, must not ack the swap: the schema would flip over an unrebuilt
// filterable bucket.
func TestASwapRefusesARecoveredUnitMissingAHalfWhileARecordIsUnreadable(t *testing.T) {
	tests := []struct {
		name        string
		swapBoth    bool
		wantRefusal bool
	}{
		{"searchable swapped, filterable record unreadable", false, true},
		{"both swapped, an unrelated record unreadable", true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CachedUnit" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for i := 0; i < 20; i++ {
				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "alpha beta")))
			}
			name := shard.Name()
			unit := testMigrationUnitFor(idx, name)
			registerIndex(idx, className)
			logger, _ := logrustest.NewNullLogger()
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization,
				Properties: []string{"title"}, TargetTokenization: models.PropertyTokenizationField,
				BucketStrategy: lsmkv.StrategyMapCollection,
				UnitToShard:    map[string]string{unit: name},
				UnitToNode:     map[string]string{unit: "node1"},
			}
			desc := distributedtask.TaskDescriptor{ID: "T_cached_unit", Version: 1}
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
			tasks, err := p.createReindexTasks(desc, unit, payload)
			require.NoError(t, err)
			require.Len(t, tasks, 2)
			for _, task := range tasks {
				require.NoError(t, startOnShard(ctx, task, shard))
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			}
			require.NoError(t, tasks[0].RunSwapOnShard(ctx, shard))
			if tc.swapBoth {
				require.NoError(t, tasks[1].RunSwapOnShard(ctx, shard))
			}
			require.NoError(t, shard.Shutdown(ctx))
			if tc.swapBoth {
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
			} else {
				filterable := filepath.Join(shard.migrationRecords.Dir(), tasks[1].migrationRecordKey().fileName())
				require.NoError(t, os.WriteFile(filterable, []byte("{"), 0o600))
			}

			recovered, err := DiscoverInFlightReindexTasks(idx.db.config.RootPath, true, logger, nil)
			require.NoError(t, err)
			idx.shardReindexer = NewShardReindexerV3FromRecovered(recovered, logger)
			reloaded, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(name, reloaded)
			defer reloaded.Shutdown(ctx)
			restarted := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
			SeedReindexProviderFromRecovery(restarted, recovered)
			require.NotEmpty(t, restarted.cachedReindexTasks(desc, unit))

			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			swapErr := restarted.OnSwapRequested(&distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc,
				Status: distributedtask.TaskStatusSwapping, NeedsPreparationBarrier: true, Payload: raw,
			}, "g1", []string{unit})
			if tc.wantRefusal {
				require.ErrorContains(t, swapErr, "could not be read")
			} else {
				require.NoError(t, swapErr)
			}
		})
	}
}
