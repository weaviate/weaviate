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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// cancelGateProvider builds the smallest ReindexProvider OnCancelApplied needs.
func cancelGateProvider(serverCtx context.Context) *ReindexProvider {
	logger, _ := logrustest.NewNullLogger()
	return &ReindexProvider{
		logger:                logger,
		serverCtx:             serverCtx,
		timings:               defaultReindexTimings(),
		payloads:              make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		cleanupInProgress:     make(map[reindexCleanupKey]int),
		submitInProgress:      make(map[reindexCleanupKey]int),
		cancelSeen:            make(map[string]int),
		cancelApplyGates:      make(map[distributedtask.TaskDescriptor]func()),
		cancelTeardownSettled: make(map[distributedtask.TaskDescriptor]time.Time),
	}
}

func cancelGateTask(t *testing.T, payload *ReindexTaskPayload) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	// A claimed unit: the cancel of a migration that had already started
	// rebuilding buckets, which is the only shape with sidecars to gate. A
	// cancel whose units never left PENDING is waived by
	// [cancelledWithoutClaimedUnits] and is covered by the rollback journey.
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 4},
		Namespace:      ReindexNamespace,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        raw,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
		},
	}
}

// The gate must be closed by the apply itself. This provider has no scheduler,
// no DB and no running task, so nothing else could have closed it.
func TestOnCancelAppliedClosesCleanupGate(t *testing.T) {
	const collection = "Movies"

	t.Run("named shards are gated immediately", func(t *testing.T) {
		serverCtx, cancelServer := context.WithCancel(context.Background())
		defer cancelServer()
		p := cancelGateProvider(serverCtx)

		task := cancelGateTask(t, &ReindexTaskPayload{
			Collection: collection,
			UnitToShard: map[string]string{
				"u1": "shard1",
				"u2": "shard2",
				// Two units on one shard must still gate it once.
				"u3": "shard2",
			},
		})

		require.False(t, p.IsCleanupInProgress(collection, "shard1"))

		p.OnCancelApplied(task)

		require.True(t, p.IsCleanupInProgress(collection, "shard1"),
			"the cancel apply must close the gate without a scheduler tick")
		require.True(t, p.IsCleanupInProgress(collection, "shard2"))
		require.True(t, p.AnyCleanupInProgressForCollection(collection),
			"the cluster-wide probe the cancel handler polls must see the gate too")
		require.False(t, p.IsCleanupInProgress("OtherCollection", "shard1"),
			"the gate is scoped to the cancelled task's collection")
	})

	t.Run("a payload without shards gates the whole collection", func(t *testing.T) {
		serverCtx, cancelServer := context.WithCancel(context.Background())
		defer cancelServer()
		p := cancelGateProvider(serverCtx)

		p.OnCancelApplied(cancelGateTask(t, &ReindexTaskPayload{Collection: collection}))

		require.True(t, p.IsCleanupInProgress(collection, "any-shard"),
			"with no shard names the gate has to cover every shard of the collection")
	})

	// The node handling the cancel polls each owner until it answers "gate
	// closed", and gives up with a degraded warning when the budget runs out.
	// A payload a newer node wrote that this one cannot fully decode must
	// therefore still latch the confirmation, or every cancel of such a task
	// burns the full per-owner budget and answers unconfirmed.
	t.Run("a payload the full decoder rejects still confirms the cancel", func(t *testing.T) {
		serverCtx, cancelServer := context.WithCancel(context.Background())
		defer cancelServer()
		p := cancelGateProvider(serverCtx)

		p.OnCancelApplied(&distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-3", Version: 1},
			Payload:        []byte(`{"collection":"Movies","unitToShard":"not-a-map"}`),
		})

		require.True(t, p.AnyCleanupInProgressForCollection(collection),
			"the node must not answer that it has seen no cancel for this collection")
		require.False(t, p.AnyCleanupInProgress(),
			"nothing names the shards, so the blocking gate stays open")
	})

	t.Run("an unparseable payload is a no-op", func(t *testing.T) {
		serverCtx, cancelServer := context.WithCancel(context.Background())
		defer cancelServer()
		p := cancelGateProvider(serverCtx)

		p.OnCancelApplied(&distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-2", Version: 1},
			Payload:        []byte("not json"),
		})

		require.False(t, p.AnyCleanupInProgress(),
			"nothing identifies the shards to gate, so nothing may be gated")
	})

	// The no-claimed-units waiver exists for one shape only: the cancel the
	// submit path manufactures for itself when a backup wins the slot and the
	// submit rolls back. Nothing wrote, so gating would fail the backup the
	// rollback exists to let win.
	//
	// A FAILED task is not that shape. It reaches this observer too, and the
	// units map it arrives with is not proof that nothing ran: a task can fail
	// before the unit updates its shard wrote have landed in the copy this node
	// sees. Waiving on unit state alone would leave those sidecars ungated for
	// the whole window this gate exists to cover.
	t.Run("a task with no claimed units is waived only when it was cancelled", func(t *testing.T) {
		unclaimedTask := func(status distributedtask.TaskStatus, units map[string]*distributedtask.Unit) *distributedtask.Task {
			raw, err := json.Marshal(&ReindexTaskPayload{
				Collection:  collection,
				UnitToShard: map[string]string{"u1": "shard1"},
			})
			require.NoError(t, err)
			return &distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-4", Version: 9},
				Namespace:      ReindexNamespace,
				Status:         status,
				Payload:        raw,
				Units:          units,
			}
		}
		pending := map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
		}

		tests := []struct {
			name   string
			status distributedtask.TaskStatus
			units  map[string]*distributedtask.Unit
			gated  bool
			why    string
		}{
			{
				name:   "cancelled with every unit still pending",
				status: distributedtask.TaskStatusCancelled,
				units:  pending,
				gated:  false,
				why:    "the submit rollback's own cancel must not gate the backup that beat it to the slot",
			},
			{
				name:   "failed with every unit still pending",
				status: distributedtask.TaskStatusFailed,
				units:  pending,
				gated:  true,
				why:    "a failed task's shards may carry sidecars this node's unit view does not show yet",
			},
			{
				name:   "failed with no units populated at all",
				status: distributedtask.TaskStatusFailed,
				units:  nil,
				gated:  true,
				why:    "an empty units map says nothing ran only if you assume it is complete",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				serverCtx, cancelServer := context.WithCancel(context.Background())
				defer cancelServer()
				p := cancelGateProvider(serverCtx)

				p.OnCancelApplied(unclaimedTask(tc.status, tc.units))

				require.Equal(t, tc.gated, p.IsCleanupInProgress(collection, "shard1"), tc.why)
				require.True(t, p.AnyCleanupInProgressForCollection(collection),
					"the confirmation latch fires either way; only the blocking gate is waived")
			})
		}
	})

	t.Run("the hold is released on server shutdown", func(t *testing.T) {
		// The hold is 15s of wall time, which no fast test can wait out. Server
		// shutdown is the other exit from the same select, so it proves the
		// release runs rather than leaking the registration.
		serverCtx, cancelServer := context.WithCancel(context.Background())
		p := cancelGateProvider(serverCtx)

		p.OnCancelApplied(cancelGateTask(t, &ReindexTaskPayload{
			Collection:  collection,
			UnitToShard: map[string]string{"u1": "shard1"},
		}))
		require.True(t, p.IsCleanupInProgress(collection, "shard1"))

		cancelServer()
		require.Eventually(t, func() bool {
			return !p.IsCleanupInProgress(collection, "shard1")
		}, 2*time.Second, 5*time.Millisecond,
			"the gate must reopen instead of staying closed for the rest of the process")
	})
}
