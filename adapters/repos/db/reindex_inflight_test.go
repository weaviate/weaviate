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
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// reindexTaskOn is a reindex task of collection in status, with one unit on shard.
func reindexTaskOn(t *testing.T, collection, shard string, status distributedtask.TaskStatus) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable, Collection: collection, Properties: []string{"foo"}})
	require.NoError(t, err)
	unitID := MigrationUnitID(shard, "node1")
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
		Payload:        raw,
		Status:         status,
		Units:          map[string]*distributedtask.Unit{unitID: {ID: unitID, Shard: shard, NodeID: "node1"}},
	}
}

// The check never sees the task's status, so the cancel it names has to
// carry the condition under which the API accepts one.
func TestReindexInFlightError(t *testing.T) {
	err := reindexInFlightError(`shard "shard1" (collection "MyClass")`)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), `shard "shard1" (collection "MyClass") has an active runtime-reindex task in DTM`)
	require.Contains(t, err.Error(), "reaches a terminal state")
	// A STARTED task with no working unit reads as "pending" on the GET.
	require.Contains(t, err.Error(), `status="pending"`)
	require.Contains(t, err.Error(), `status="indexing"`)
	require.Contains(t, err.Error(), "accepted while the task is STARTED or PREPARING")
	require.Contains(t, err.Error(), "409")
	require.Contains(t, err.Error(), "refuses for about a second, so retry")
}

// A halt for a backup or a replica move refuses while a task this node applied
// still builds on the shard.
func TestShard_HaltForTransfer_RefusesWhileReindexRunsHere(t *testing.T) {
	for _, tc := range []struct {
		name       string
		task       func(collection, shard string) *distributedtask.Task
		offloading bool
		disabled   bool
		wantRefuse bool
	}{
		{name: "a started task with a unit here", wantRefuse: true, task: func(c, s string) *distributedtask.Task {
			return reindexTaskOn(t, c, s, distributedtask.TaskStatusStarted)
		}},
		{name: "a status this build cannot classify", wantRefuse: true, task: func(c, s string) *distributedtask.Task {
			return reindexTaskOn(t, c, s, unknownFutureStatus)
		}},
		{name: "an ended task", task: func(c, s string) *distributedtask.Task {
			return reindexTaskOn(t, c, s, distributedtask.TaskStatusFinished)
		}},
		{name: "a started task on another shard", task: func(c, _ string) *distributedtask.Task {
			return reindexTaskOn(t, c, "other", distributedtask.TaskStatusStarted)
		}},
		{name: "a started task of another collection", task: func(_, s string) *distributedtask.Task {
			return reindexTaskOn(t, "Other", s, distributedtask.TaskStatusStarted)
		}},
		{name: "an offload", offloading: true, task: func(c, s string) *distributedtask.Task {
			return reindexTaskOn(t, c, s, distributedtask.TaskStatusStarted)
		}},
		{name: "runtime reindex disabled", disabled: true, task: func(c, s string) *distributedtask.Task {
			return reindexTaskOn(t, c, s, distributedtask.TaskStatusStarted)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shd, idx := testShard(t, ctx, "ShardHaltClass")
			idx.db.config.RuntimeReindexDisabled = tc.disabled
			idx.db.SetReindexTaskStateSource(managerHolding(t, ReindexNamespace, tc.task("ShardHaltClass", shd.Name())))

			err := shd.HaltForTransfer(ctx, tc.offloading, 100*time.Millisecond)
			if tc.wantRefuse {
				require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
				require.Contains(t, err.Error(), shd.Name())
				return
			}
			require.NoError(t, err)
			require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
		})
	}
}

// A replica move waits out a reindex on the shard, in both snapshot modes.
func TestReplicaSnapshotDefersForAReindexOnTheShard(t *testing.T) {
	for _, mode := range []struct {
		name         string
		withHardlink bool
	}{{name: "hardlink mode", withHardlink: true}, {name: "fallback halt-for-duration mode"}} {
		t.Run(mode.name, func(t *testing.T) {
			if !mode.withHardlink {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			}
			index, _ := newSharedHaltTestShard(t)
			putSharedHaltObject(t, index, strfmt.UUID("2b1d4bd0-3f52-4f0f-9f75-b0cb2e29b3a3"), 0)
			index.db.SetReindexTaskStateSource(managerHolding(t, ReindexNamespace,
				reindexTaskOn(t, "TestClass", "shard1", distributedtask.TaskStatusStarted)))

			_, err := index.IncomingCreateReplicaSnapshot(context.Background(), "shard1", "op-reindex")
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
				"the reindex sentinel must survive so the backup path keeps its own response")
			require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp,
				"the shard-busy sentinel is what writes the text a remote caller matches on")
			require.True(t, replication.IsReversibleRefusal(err))
		})
	}
}

// Index.asShardBusyIfReindexInFlight marks only a live-reindex refusal as ErrShardBusyStructuralOp, so a timed-out halt still counts against MaxErrors.
func TestAsShardBusyIfReindexInFlight_ReturnsOtherErrorsUnchanged(t *testing.T) {
	plain := errors.New("pause compaction: context deadline exceeded")
	require.Same(t, plain, (&Index{}).asShardBusyIfReindexInFlight(plain))
}
