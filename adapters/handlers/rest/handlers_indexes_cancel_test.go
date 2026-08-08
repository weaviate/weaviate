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
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
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

	drainDeadline    time.Time
	drainHasDeadline bool
}

func (p *stubCleanupGateProvider) DrainWithCleanupGate(
	ctx context.Context, _ *db.ReindexTaskPayload, _ distributedtask.TaskDescriptor,
) (func(), error) {
	p.drainDeadline, p.drainHasDeadline = ctx.Deadline()
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
		"Movies", "title", "filterable", true)

	require.Nil(t, release,
		"the caller must not be handed a release it would drop at its return, over a worker that is still writing")
	require.Equal(t, 1, provider.handoffs, "the gate must be handed to the worker-exit watcher")
	require.False(t, provider.released.Load(), "the gate must stay closed while the worker may still be writing")

	provider.handedOff()
	require.True(t, provider.released.Load(), "the watcher must have been handed the real release")
}

// panicOnMessageHook panics from a log line, which is how this test injects a
// panic into the window between the drain and the return. Which panic it is
// does not matter — what is under test is the unwind.
type panicOnMessageHook struct{ substr string }

func (h panicOnMessageHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h panicOnMessageHook) Fire(entry *logrus.Entry) error {
	if strings.Contains(entry.Message, h.substr) {
		panic("injected panic inside the guarded region")
	}
	return nil
}

// The gate release is handed to the caller by the return value, so a panic
// between the drain and that return leaves the caller with nothing to defer.
// net/http recovers and the process survives, but the collection's backups and
// restores stay refused until it is restarted.
func TestCancelReleasesTheCleanupGateWhenTheCleanupPanics(t *testing.T) {
	h, _ := cancelFixture(t, &scriptedCleanupProber{})
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.AddHook(panicOnMessageHook{substr: "cancel: drain complete"})
	h.appState.Logger = logger

	provider := &stubCleanupGateProvider{}

	require.Panics(t, func() {
		h.drainAndCleanupCancelledTask(context.Background(), provider,
			&distributedtask.Task{TaskDescriptor: distributedtask.TaskDescriptor{ID: "Movies:repair-filterable:title:ab3f", Version: 3}},
			&db.ReindexTaskPayload{MigrationType: db.ReindexTypeRepairFilterable, Collection: "Movies"},
			"Movies", "title", "filterable", true)
	})

	require.True(t, provider.released.Load(),
		"a panic past the return leaks the gate: the caller never receives the release, "+
			"so every backup and restore of this collection is refused for the rest of the process")
	require.Zero(t, provider.handoffs,
		"the drain succeeded, so the gate is not the worker-exit watcher's to release")
}

// The drain is detached from the request, so nothing upstream bounds it. Its own
// timeout is the only thing that stops a wedged worker from holding the
// goroutine open forever, and it must stay short enough that "let the next
// submit clean up" is still a fallback rather than a theory.
func TestCancelDrainRunsUnderItsOwnBound(t *testing.T) {
	h, _ := cancelFixture(t, &scriptedCleanupProber{})
	// Stops at the drain: the on-disk sweep past it needs a real DB.
	provider := &stubCleanupGateProvider{drainErr: context.DeadlineExceeded}

	start := time.Now()
	h.drainAndCleanupCancelledTask(context.Background(), provider,
		&distributedtask.Task{TaskDescriptor: distributedtask.TaskDescriptor{ID: "Movies:repair-filterable:title:ab3f", Version: 3}},
		&db.ReindexTaskPayload{MigrationType: db.ReindexTypeRepairFilterable, Collection: "Movies"},
		"Movies", "title", "filterable", true)

	require.True(t, provider.drainHasDeadline,
		"the drain is detached from the request, so without its own deadline a wedged worker holds the goroutine forever")
	assert.InDelta(t, (10 * time.Second).Seconds(), provider.drainDeadline.Sub(start).Seconds(), 1,
		"the drain must be capped at 10s, short enough for 'the next submit cleans up' to still apply")
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

// The tolerant cancel pass matches on the collection alone, so the property and
// index type it sweeps come from the request URL, not from the task. Logging
// the same "cleanup complete" line as the exact match tells an operator the
// disk is clean when the task's own sidecars may be untouched.
func TestCancelCleanupLogSaysWhenTheSweptTupleWasGuessed(t *testing.T) {
	tests := []struct {
		name            string
		payloadReadable bool
		wantMessage     string
	}{
		{"the task's own payload named the tuple", true, "cancel: on-disk cleanup complete"},
		{"the tuple came from the request URL", false, "cancel: on-disk cleanup complete for the property and index type in the request URL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, _ := cancelFixture(t, &scriptedCleanupProber{})
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h.appState.Logger = logger
			release := h.drainAndCleanupCancelledTask(context.Background(), &stubCleanupGateProvider{},
				&distributedtask.Task{TaskDescriptor: distributedtask.TaskDescriptor{ID: "Movies:repair-filterable:title:ab3f", Version: 3}},
				&db.ReindexTaskPayload{MigrationType: db.ReindexTypeRepairFilterable, Collection: "Movies"},
				"Movies", "title", "filterable", test.payloadReadable)
			require.NotNil(t, release)
			release()
			var completion string
			for _, entry := range hook.AllEntries() {
				if strings.HasPrefix(entry.Message, "cancel: on-disk cleanup complete") {
					completion = entry.Message
				}
			}
			require.NotEmpty(t, completion, "the sweep has to report its outcome")
			require.Contains(t, completion, test.wantMessage)
			if test.payloadReadable {
				// The unqualified line is the one an operator reads as authoritative.
				require.Equal(t, test.wantMessage, completion)
			}
		})
	}
}

// Every gate on the submission path says so when it fails open, and every one
// of those lines has to reach the handler's own logger — the sampler that
// rate-limits them is what carries it.
func TestSubmitReportsEveryGateItFailsOpen(t *testing.T) {
	tests := []struct {
		name string
		// unwire removes one dependency from a handler that is otherwise whole.
		unwire  func(h *indexesHandlers)
		wantLog string
	}{
		{
			name:    "no reindex provider, so the submit gate is a no-op",
			unwire:  func(h *indexesHandlers) { h.appState.ReindexProvider.Store(nil) },
			wantLog: "reindex provider is not wired",
		},
		{
			name:    "no backup activity probe, so no node is asked",
			unwire:  func(h *indexesHandlers) { h.backupActivity = nil },
			wantLog: "backup activity probe is not wired",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var busy atomic.Bool
			h := submissionHandlers(t, &raceTaskService{}, togglingProber{busy: &busy})
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h.appState.Logger = logger
			h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, logger, fixtureNode,
				func() int { return 1 }, context.Background()))
			test.unwire(h)

			require.IsType(t, &schema.SchemaObjectsIndexesUpdateAccepted{}, submitReindex(h),
				"the gate fails open, so the submission still goes through")

			var found bool
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, test.wantLog) {
					found = true
					require.Equal(t, logrus.WarnLevel, entry.Level)
				}
			}
			require.Truef(t, found, "an ungated submission has to be visible on the node's own logger")
		})
	}
}
