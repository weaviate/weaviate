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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
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
	// A real provider, so the gates the cancel closes are the ones a backup
	// would consult.
	h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, "node1",
		func() int { return 1 }, context.Background()))
	return h, svc
}

// The gate the cancel closes must outlive its own cleanup: the owners are asked
// to confirm theirs after it, and answering with this node's gate already open
// hands the caller a "cancelled" a backup starting in that instant can race.
func TestCancelHoldsCleanupGateUntilTheHandlerAnswers(t *testing.T) {
	const (
		collection = "Movies"
		shard      = "shard1"
	)

	prober := &gateWatchCleanupProber{collection: collection}
	h, svc := cancelFixture(t, prober)
	prober.provider = h.appState.ReindexProvider.Load()

	responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "a cancel of a live task must be accepted, got %T", responder)
	require.Equal(t, "CANCELLED", accepted.Payload.Status)
	require.Len(t, svc.cancelled, 1)

	samples := prober.samples()
	require.NotEmpty(t, samples, "the owner must have been asked, or the gate is sampled nowhere")
	require.True(t, samples[0],
		"the gate must still be closed while the owners are asked to confirm theirs")
	require.False(t, h.appState.ReindexProvider.Load().IsCleanupInProgress(collection, shard),
		"the gate must be released once the handler answers")
}

// gateWatchCleanupProber samples the local backup gate at the point an owner is
// asked to confirm its own — the last thing the cancel does before answering.
type gateWatchCleanupProber struct {
	provider   *db.ReindexProvider
	collection string

	mu         sync.Mutex
	gateAtCall []bool
}

func (p *gateWatchCleanupProber) CleanupInProgress(_ context.Context, _, _ string) (bool, error) {
	p.mu.Lock()
	p.gateAtCall = append(p.gateAtCall, p.provider.IsCleanupInProgress(p.collection, "shard1"))
	p.mu.Unlock()
	return true, nil
}

func (p *gateWatchCleanupProber) samples() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.gateAtCall...)
}

// stubCleanupGateProvider drives the drain-timeout branch. A wedged worker
// cannot be built from this package: a running handle is only registered by
// ReindexProvider.StartTask, which needs a live index.
type stubCleanupGateProvider struct {
	drainErr  error
	released  atomic.Bool
	handoffs  int
	handedOff func()
}

func (p *stubCleanupGateProvider) DrainWithCleanupGate(
	context.Context, *db.ReindexTaskPayload, distributedtask.TaskDescriptor,
) (func(), error) {
	return func() { p.released.Store(true) }, p.drainErr
}

func (p *stubCleanupGateProvider) ReleaseCleanupGateOnWorkerExit(
	_ distributedtask.TaskDescriptor, release func(), _ logrus.FieldLogger,
) {
	p.handoffs++
	p.handedOff = release
}

// A drain that times out leaves the worker writing, which is the case the gate
// exists for: it has to be handed to the worker-exit watcher rather than
// released with the request.
func TestCancelDrainTimeoutHandsTheGateToTheWorkerExitWatcher(t *testing.T) {
	h, _ := cancelFixture(t, &scriptedCleanupProber{})
	provider := &stubCleanupGateProvider{drainErr: context.DeadlineExceeded}

	release := h.drainAndCleanupCancelledTask(context.Background(), provider,
		&distributedtask.Task{TaskDescriptor: distributedtask.TaskDescriptor{ID: "Movies:repair-filterable:title:ab3f", Version: 3}},
		&db.ReindexTaskPayload{MigrationType: db.ReindexTypeRepairFilterable, Collection: "Movies"},
		"Movies", "title", "filterable")

	require.Nil(t, release,
		"the caller must not be handed a release it would drop at its return, over a worker that is still writing")
	require.Equal(t, 1, provider.handoffs, "the gate must be handed to the worker-exit watcher")
	require.False(t, provider.released.Load(), "the gate must stay closed while the worker may still be writing")

	provider.handedOff()
	require.True(t, provider.released.Load(), "the watcher must have been handed the real release")
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
