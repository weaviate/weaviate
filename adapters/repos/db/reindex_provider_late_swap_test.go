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
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			registerIndex(idx, className)
			unit := shard.migrationUnit()
			logger, hook := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
				UnitToShard: map[string]string{unit: shard.Name()},
			}
			desc := distributedtask.TaskDescriptor{ID: "R1", Version: 1}
			cached, err := p.createReindexTasks(desc, unit, payload)
			require.NoError(t, err)
			// Never run, so no record: as after the newer run retired it.
			p.cacheReindexTasks(desc, unit, cached)
			if unreadable {
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
				require.NoError(t, shard.migrationRecords.Load())
			}

			got := p.resolveUnitForPhase(ctx, &distributedtask.Task{TaskDescriptor: desc}, payload, unit, idx, logger)
			require.Equal(t, unreadable, len(got.Errs) > 0, "an unreadable record is never read as gone")
			require.Equal(t, !unreadable, got.Skip, "a retired record leaves nothing of the run in flight")
			for range maxReportedErrors {
				p.resolveUnitForPhase(ctx, &distributedtask.Task{TaskDescriptor: desc}, payload, unit, idx, logger)
			}
			var skipLines int
			for _, e := range hook.AllEntries() {
				if strings.Contains(e.Message, "migration records are gone") {
					skipLines++
				}
			}
			require.LessOrEqual(t, skipLines, maxReportedErrors, "a retry per tick logs a sampled skip line")
		})
	}
}
