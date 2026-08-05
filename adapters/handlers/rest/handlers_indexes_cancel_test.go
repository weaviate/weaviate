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

package rest

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// cancelFixture builds the cancel handler over one STARTED task whose units
// live on remoteOwner, which is the only shape in which the cancel has an owner
// to wait for.
func cancelFixture(t *testing.T, prober reindexCleanupProber) (*indexesHandlers, *raceTaskService) {
	t.Helper()
	const (
		collection  = "Movies"
		remoteOwner = "node2"
		taskID      = "Movies:repair-filterable:title:ab3f"
	)

	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    collection,
		Properties:    []string{"title"},
		UnitToNode:    map[string]string{"u1": remoteOwner},
		UnitToShard:   map[string]string{"u1": "shard1"},
	})
	require.NoError(t, err)

	svc := &raceTaskService{tasks: []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}}

	var busy atomic.Bool
	h := submissionHandlers(t, svc, togglingProber{busy: &busy})
	h.reindexCleanup = prober
	return h, svc
}

// The cancel is answered only once every other owner confirms it closed its
// cleanup gate. Answering earlier hands the caller a "cancelled" that a backup
// starting in the same instant can still race on those nodes.
func TestCancelReindexTaskWaitsForOwnerCleanupGates(t *testing.T) {
	const (
		collection  = "Movies"
		remoteOwner = "node2"
	)

	prober := &scriptedCleanupProber{script: map[string][]cleanupAnswer{
		remoteOwner: {{up: false}, {up: true}},
	}}
	h, svc := cancelFixture(t, prober)

	responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "a cancel of a live task must be accepted, got %T", responder)
	require.Equal(t, "CANCELLED", accepted.Payload.Status)
	require.Len(t, svc.cancelled, 1, "the live task must have been cancelled")

	require.GreaterOrEqual(t, prober.callsFor(remoteOwner), 2,
		"the owner has to be asked, and re-asked, before the caller is told the cancel is done")
	require.Contains(t, prober.queried, remoteOwner+"/"+collection,
		"the owner must be asked about the collection being cancelled")
}

// A cancel with nothing to cancel must not probe anyone: there is no teardown
// for an owner to confirm.
func TestCancelReindexTaskNoOpDoesNotProbeOwners(t *testing.T) {
	prober := &scriptedCleanupProber{}
	h, _ := cancelFixture(t, prober)

	responder := h.cancelReindexTask(context.Background(), "Movies", "description", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "an idempotent cancel is a success, got %T", responder)
	require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
	require.Empty(t, prober.queried)
}
