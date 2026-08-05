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

// cancelGateProvider builds the smallest ReindexProvider that OnCancelApplied
// needs: the cleanup registry, the payload cache it reads through, a logger for
// the reopen goroutine, and the server context that bounds the hold.
func cancelGateProvider(serverCtx context.Context) *ReindexProvider {
	logger, _ := logrustest.NewNullLogger()
	return &ReindexProvider{
		logger:            logger,
		serverCtx:         serverCtx,
		payloads:          make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		cleanupInProgress: make(map[reindexCleanupKey]int),
		submitInProgress:  make(map[reindexCleanupKey]int),
		cancelSeen:        make(map[string]int),
		cancelApplyGates:  make(map[distributedtask.TaskDescriptor]func()),
	}
}

func cancelGateTask(t *testing.T, payload *ReindexTaskPayload) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 4},
		Namespace:      ReindexNamespace,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        raw,
	}
}

// TestOnCancelAppliedClosesCleanupGate pins the reason OnCancelApplied exists:
// the node handling a cancel has to be able to tell its caller that no backup
// can start into a half-torn-down shard, and it only has a few seconds to learn
// that. Every other path that closes the cleanup gate is driven by the
// scheduler tick (one minute by default), so the gate must be closed by the
// apply itself.
//
// The provider here has no scheduler, no DB, and no running task: if the gate
// is closed after OnCancelApplied returns, nothing but the apply can have
// closed it.
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
