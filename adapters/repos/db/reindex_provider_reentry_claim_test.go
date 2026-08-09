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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// reentryRecorder accepts progress and failure reports and counts them.
// processOneUnit reports progress before it reaches the guard, so a
// panicking stub would not survive either arm.
type reentryRecorder struct {
	failures int
}

func (r *reentryRecorder) RecordDistributedTaskUnitFailure(
	_ context.Context, _, _ string, _ uint64, _, _, _ string,
) error {
	r.failures++
	return nil
}

func (r *reentryRecorder) RecordDistributedTaskUnitCompletion(
	_ context.Context, _, _ string, _ uint64, _, _ string,
) error {
	return nil
}

func (r *reentryRecorder) UpdateDistributedTaskUnitProgress(
	_ context.Context, _, _ string, _ uint64, _, _ string, _ float32,
) error {
	return nil
}

// TestProcessOneUnit_ReEntryClaimAppliesToDeferredUnitsOnly is the
// consumer-side receipt for which units take the re-entry claim. The
// predicate and the map bookkeeping have their own tests; this one
// drives processOneUnit itself, because the branch changed the guard
// from "semantic" to "not inline" and nothing downstream of
// claimUnitIfDeferred was exercised.
//
// A deferred unit whose slot another worker already holds must return
// before it can create the next generation and clobber the cached tasks
// OnGroupCompleted swaps. An inline unit takes no claim, so the same
// pre-held slot must not stop it.
func TestProcessOneUnit_ReEntryClaimAppliesToDeferredUnitsOnly(t *testing.T) {
	tests := []struct {
		name          string
		migrationType ReindexMigrationType
		barrier       bool
		wantSkipped   bool
	}{
		{
			name:          "a deferred unit yields to the worker holding the slot",
			migrationType: ReindexTypeEnableRangeable,
			barrier:       true,
			wantSkipped:   true,
		},
		{
			name:          "an inline unit takes no claim and runs anyway",
			migrationType: ReindexTypeRepairRangeable,
			wantSkipped:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ReEntryClaim_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			p := &ReindexProvider{
				logger:        logger,
				localNode:     "node1",
				serverCtx:     ctx,
				activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{},
				reindexTasks:  map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{},
			}

			const unitID = "unit-0"
			task := &distributedtask.Task{
				Namespace:               ReindexNamespace,
				TaskDescriptor:          distributedtask.TaskDescriptor{ID: "task-1", Version: 1},
				NeedsPreparationBarrier: tc.barrier,
			}
			payload := &ReindexTaskPayload{
				MigrationType: tc.migrationType,
				Collection:    className,
				Properties:    []string{filterableToRangeablePropName},
				UnitToShard:   map[string]string{unitID: shard.Name()},
			}

			// Another worker is already inside this unit.
			require.True(t, p.claimActiveWorker(task.TaskDescriptor, unitID))

			p.processOneUnit(ctx, task, payload, idx, unitID, &reentryRecorder{})

			var skipped bool
			for _, entry := range hook.AllEntries() {
				if entry.Message == "reindex provider: skipping re-entered unit (concurrent worker)" {
					skipped = true
				}
			}
			require.Equal(t, tc.wantSkipped, skipped)

			// The tracker dir is the durable evidence of having got past
			// the guard: it is created by the first step behind it.
			_, err := os.Stat(filepath.Join(shard.pathLSM(), ".migrations"))
			if tc.wantSkipped {
				require.True(t, os.IsNotExist(err),
					"a unit that yielded must not have created a generation")
				return
			}
			require.NoError(t, err, "an inline unit must have run its migration")
		})
	}
}
