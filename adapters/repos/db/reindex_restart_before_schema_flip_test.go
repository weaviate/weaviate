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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A replica that swapped its buckets to the new tokenization and then restarted
// before the cluster-wide schema flip must still answer like one that did not.
func TestRestartBetweenSwapAndSchemaFlipKeepsQueriesCorrect(t *testing.T) {
	const (
		prop  = "title"
		node  = "node1"
		query = "alpha bravo"
	)

	tests := []struct {
		name string
		// The restart lost this node's swap ack, so the scheduler requests the swap again.
		reswap bool
	}{
		{name: "the swap ack landed before the restart"},
		{name: "the restart lost the swap ack", reswap: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RestartBeforeFlip_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			class.Properties[0].IndexFilterable = boolPtr(true)
			class.Properties[0].IndexSearchable = boolPtr(true)

			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				true, false, false)
			f := &migrationRestartFixture{idx: idx, class: class, shard: hot.(*Shard), shardName: hot.Name()}
			t.Cleanup(func() { f.shard.Shutdown(context.Background()) })
			registerIndex(idx, className)

			// Word tokenization matches both on "alpha bravo"; field only the exact one.
			exact := createTestObjectWithText(className, query)
			longer := createTestObjectWithText(className, query+" charlie")
			for _, obj := range []*storobj.Object{exact, longer} {
				require.NoError(t, f.shard.PutObject(ctx, obj))
			}

			unitID := f.shard.migrationUnit()
			payload := ReindexTaskPayload{
				Collection:         className,
				MigrationType:      ReindexTypeChangeTokenization,
				Properties:         []string{prop},
				TargetTokenization: models.PropertyTokenizationField,
				BucketStrategy:     lsmkv.StrategyMapCollection,
				UnitToShard:        map[string]string{unitID: f.shardName},
				UnitToNode:         map[string]string{unitID: node},
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := func(status distributedtask.TaskStatus) *distributedtask.Task {
				return &distributedtask.Task{
					Namespace:               ReindexNamespace,
					TaskDescriptor:          distributedtask.TaskDescriptor{ID: "T", Version: 1},
					Status:                  status,
					Payload:                 raw,
					NeedsPreparationBarrier: true,
					SwapDecidedGroups:       map[string]time.Time{"g1": time.Now()},
				}
			}

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
			recorder := newFakeRecorder()
			p.processOneUnit(ctx, task(distributedtask.TaskStatusStarted), &payload, idx, unitID, recorder)
			require.Equal(t, []string{unitID}, recorder.completed, "the unit must finish its reindex")
			require.NoError(t, p.OnGroupCompleted(task(distributedtask.TaskStatusPreparing), "g1", []string{unitID}))
			require.NoError(t, p.OnSwapRequested(task(distributedtask.TaskStatusSwapping), "g1", []string{unitID}))

			probe := func() map[string][]string {
				filtered, _, err := f.shard.ObjectSearch(ctx, 10, propEqualsFilter(className, prop, query),
					nil, nil, nil, additional.Properties{}, nil)
				require.NoError(t, err)
				ranked, _, err := f.shard.ObjectSearch(ctx, 10, nil,
					&searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: query},
					nil, nil, additional.Properties{}, []string{prop})
				require.NoError(t, err)
				return map[string][]string{"filter": objectIDs(filtered), "bm25": objectIDs(ranked)}
			}
			want := map[string][]string{
				"filter": {exact.ID().String()},
				"bm25":   {exact.ID().String()},
			}
			require.Equal(t, want, probe(),
				"a replica that swapped and did not restart answers with the new tokenization, or this test proves nothing")

			f.restart(t, ctx)
			if tc.reswap {
				p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
				require.NoError(t, p.OnSwapRequested(task(distributedtask.TaskStatusSwapping), "g1", []string{unitID}))
			}
			assert.Equal(t, want, probe(),
				"before the schema flip, the restarted replica must answer like one that did not restart")

			class.Properties[0].Tokenization = models.PropertyTokenizationField
			require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))
			assert.Equal(t, want, probe(), "after the schema flip, too")
		})
	}
}
