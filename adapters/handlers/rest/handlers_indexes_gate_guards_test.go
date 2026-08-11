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

// Guards on the reindex admission and cancel paths for when a question
// cannot be answered: unreachable leader, silent prober, old peer, or a
// cancel racing the task's own ending.

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/backup"
)

// -----------------------------------------------------------------------------
// The task listing is what rules a conflicting migration out.
// -----------------------------------------------------------------------------

// A failed listing hides any conflicting task, and the on-disk sweep behind
// it must not run on that guess.
func TestUpdateIndexRefusesWhenTheTaskListingFails(t *testing.T) {
	svc := &raceTaskService{listErr: errors.New("raft: leader not reachable")}
	h, provider := gatePriorityHandlers(t, svc)

	local := &localSlotProbe{provider: provider}
	h.localBackupActivity = local
	fanOut := &gateObservingProber{provider: provider}
	h.backupActivity = fanOut

	responder := submitReindex(h)

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "a conflict that cannot be ruled out must be refused with 503, got %T", responder)
	require.Contains(t, errorMessage(t, unavailable.Payload), "cannot list in-flight reindex tasks")
	require.Contains(t, errorMessage(t, unavailable.Payload), "raft: leader not reachable",
		"the operator needs to know which failure to chase")

	// Each assertion is a step of the destructive path; separated so a
	// regression names which one it reached.
	require.Emptyf(t, local.observed(),
		"the handler carried on past the failed listing and read this node's backup slots")
	require.Emptyf(t, fanOut.observed(),
		"the handler carried on past the failed listing, closed the collection's backup gate "+
			"and fanned out — the stale-state sweep behind it was reachable")
	require.Zerof(t, svc.adds,
		"a migration was committed while a conflicting one could not be ruled out")
	require.Equal(t, db.ReindexHoldNone, provider.HoldForShard("Movies", "shard1"),
		"the submit gate must never have been taken")
}

// -----------------------------------------------------------------------------
// The fan-out scan and the verdict it produces.
// -----------------------------------------------------------------------------

// perNodeProber answers each node from a script; it can also panic to
// simulate a prober that leaves its slot unwritten.
type perNodeProber struct {
	activity map[string]backup.NodeActivity
	errs     map[string]error
	panics   map[string]bool
}

func (p perNodeProber) NodeActivity(_ context.Context, node string) (backup.NodeActivity, error) {
	if p.panics[node] {
		panic("prober blew up on " + node)
	}
	if err := p.errs[node]; err != nil {
		return backup.NodeActivity{}, err
	}
	return p.activity[node], nil
}

// The scan's three verdicts and the refusal each warrants.
func TestScanBackupActivityVerdicts(t *testing.T) {
	const (
		busyNode        = "node1"
		unreachableNode = "node2"
	)
	nodes := []string{busyNode, unreachableNode}
	running := backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1"}

	tests := []struct {
		name            string
		prober          perNodeProber
		wantBusy        string
		wantUnreachable string
		// wantStatus is the HTTP status the scan warrants; 0 means no refusal
		// at all, so the submission is admitted.
		wantStatus int
		wantBody   string
	}{
		{
			name:       "one node holds a slot",
			prober:     perNodeProber{activity: map[string]backup.NodeActivity{busyNode: running}},
			wantBusy:   busyNode,
			wantStatus: 409,
			wantBody:   "reindex blocked: a backup is running in the cluster; retry after it finishes",
		},
		{
			name: "one node cannot be reached",
			prober: perNodeProber{errs: map[string]error{
				unreachableNode: errors.New("dial tcp: connection refused"),
			}},
			wantUnreachable: unreachableNode,
			wantStatus:      503,
			wantBody:        "reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers",
		},
		{
			// "busy" outranks "unreachable": wait for a backup to finish, not a
			// cluster that merely reads unhealthy.
			name: "a busy node and an unreachable one at the same time",
			prober: perNodeProber{
				activity: map[string]backup.NodeActivity{busyNode: running},
				errs:     map[string]error{unreachableNode: errors.New("dial tcp: connection refused")},
			},
			wantBusy:        busyNode,
			wantUnreachable: unreachableNode,
			wantStatus:      409,
			wantBody:        "reindex blocked: a backup is running in the cluster; retry after it finishes",
		},
		{
			// The zero value reads as "no backup running", so an unwritten
			// slot has to be a refusal.
			name:            "a prober that never reports",
			prober:          perNodeProber{panics: map[string]bool{unreachableNode: true}},
			wantUnreachable: unreachableNode,
			wantStatus:      503,
			wantBody:        "reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers",
		},
		{
			// Deliberate fail-open: an old peer has no probe endpoint, and
			// treating it as unreachable would 503 the whole rolling upgrade.
			name: "a peer too old to serve the probe",
			prober: perNodeProber{errs: map[string]error{
				unreachableNode: clients.ErrNodeActivityUnsupported,
			}},
			wantStatus: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()

			scan := scanBackupActivity(context.Background(), nodes, tc.prober, logger)

			require.Equal(t, tc.wantBusy, scan.BusyNode)
			require.Equal(t, tc.wantUnreachable, scan.UnreachableNode)

			responder := backupActivityResponder(&models.Principal{Username: "u1"}, scan)
			if tc.wantStatus == 0 {
				require.Nilf(t, responder, "this scan warrants no refusal, got %T", responder)
				return
			}
			switch tc.wantStatus {
			case 409:
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "expected 409, got %T", responder)
				require.Equal(t, tc.wantBody, errorMessage(t, conflict.Payload))
			default:
				unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
				require.Truef(t, ok, "expected 503, got %T", responder)
				require.Equal(t, tc.wantBody, errorMessage(t, unavailable.Payload))
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Waiting on one owner's cleanup gate.
// -----------------------------------------------------------------------------

// unsupportedCleanupProber is the peer running a build without the cleanup
// probe endpoint.
type unsupportedCleanupProber struct{}

func (unsupportedCleanupProber) CleanupInProgress(context.Context, string, string) (bool, error) {
	return false, clients.ErrReindexCleanupUnsupported
}

// An old peer never answers this probe; polling it must give up at once
// rather than burn the per-owner timeout budget.
func TestAwaitOneOwnerCleanupGateGivesUpAtOnceOnAnOldPeer(t *testing.T) {
	h, _ := gateHandlers(unsupportedCleanupProber{}, fixtureNode, "node2")

	start := time.Now()
	reason := h.awaitOneOwnerCleanupGate(context.Background(), "node2", "Movies")
	elapsed := time.Since(start)

	require.Equal(t, "node does not serve the cleanup probe", reason,
		"the cancel is answered anyway, so the reason is all the operator gets")
	require.Lessf(t, elapsed, reindexOwnerGateTimeout/2, "waiting on a peer that cannot answer "+
		"burns the whole %s budget per old owner", reindexOwnerGateTimeout)
}

// -----------------------------------------------------------------------------
// Cancelling a task that names no collection.
// -----------------------------------------------------------------------------

// A task naming no collection cannot address the on-disk sweep; running it
// would delete state belonging to whichever migration does own that tuple.
func TestCancelOfATaskNamingNoCollectionSkipsTheOnDiskSweep(t *testing.T) {
	svc := &raceTaskService{tasks: []*distributedtask.Task{
		unattributableTask("orphan", distributedtask.TaskStatusStarted),
	}}
	h := cancelHandlers(t, svc)
	logger, hook := logrustest.NewNullLogger()
	h.appState.Logger = logger

	responder := h.cancelReindexTask(context.Background(), "Movies", "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "cancel must be accepted, got %T", responder)
	require.Equal(t, "CANCELLED", accepted.Payload.Status)
	require.Len(t, svc.cancelled, 1, "the task holding every collection's gate must be cancelled")

	require.NotNil(t, entryWithMessage(hook, "skipping drain+cleanup"),
		"the skip is the decision under test; without the line it cannot be told from a path that never ran")
	require.Nilf(t, entryWithMessage(hook, "starting drain+cleanup"),
		"the sweep ran against the URL's tuple for a task that names none of it, so it deleted "+
			"on-disk state belonging to some other migration; entries were %q", entryMessages(hook))
}

// -----------------------------------------------------------------------------
// A cancel that lost the race with the task's own ending.
// -----------------------------------------------------------------------------

// DTM's rejection for an already-stopped task is identical to the one for a
// task past the cancellation point; only a fresh listing tells them apart.
func TestCancelThatRacedTheTasksOwnEnding(t *testing.T) {
	const (
		taskID     = "Movies:repair-filterable:title:ab3f"
		collection = "Movies"
	)
	rejection := func() error {
		return permanentRejection(distributedtask.ErrTaskNotRunning,
			"[dtm-perm/task-not-running] task reindex/"+taskID+"/3 is no longer running")
	}

	tests := []struct {
		name      string
		cancelErr error
		// settledAs is the status the task holds by the time the handler
		// re-lists, empty for a task whose status does not move.
		settledAs distributedtask.TaskStatus
		wantNoOp  bool
	}{
		{
			name:      "the task finished before the cancel landed",
			cancelErr: rejection(),
			settledAs: distributedtask.TaskStatusFinished,
			wantNoOp:  true,
		},
		{
			name:      "the task failed before the cancel landed",
			cancelErr: rejection(),
			settledAs: distributedtask.TaskStatusFailed,
			wantNoOp:  true,
		},
		{
			name:      "someone else cancelled it first",
			cancelErr: rejection(),
			settledAs: distributedtask.TaskStatusCancelled,
			wantNoOp:  true,
		},
		{
			// Same rejection, but a live task: NO_OP here would misreport a
			// migration that is still running.
			name:      "the task is still preparing",
			cancelErr: rejection(),
		},
		{
			// The error says nothing about why the cancel failed, so the
			// task's state is a guess, not a reading.
			name:      "the cancel failed for an unrelated reason",
			cancelErr: errors.New("raft: leader election in progress"),
			settledAs: distributedtask.TaskStatusFinished,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &scriptedRollbackService{
				tasks: []*distributedtask.Task{
					buildTask(t, taskID, distributedtask.TaskStatusPreparing, db.ReindexTaskPayload{
						MigrationType: db.ReindexTypeRepairFilterable,
						Collection:    collection,
						Properties:    []string{"title"},
					}, nil),
				},
				cancelErr:               tc.cancelErr,
				statusAfterFailedCancel: tc.settledAs,
			}
			h := cancelHandlers(t, svc)
			logger, hook := logrustest.NewNullLogger()
			h.appState.Logger = logger

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			if !tc.wantNoOp {
				_, ok := responder.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
				require.Truef(t, ok, "the task's state is unknown, so the failure must be reported, got %T", responder)
				return
			}

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "a task that stopped on its own leaves nothing to cancel, got %T", responder)
			require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
			require.NotNilf(t, audited(hook, "reindex_task_cancel_noop"),
				"a SIEM rule keys on audit_event; entries were %q", entryMessages(hook))
		})
	}
}

// -----------------------------------------------------------------------------
// Authorization on the cancel route.
// -----------------------------------------------------------------------------

// The kill-switch carve-out for cancel must sit after the authorization
// check, or it becomes an unauthenticated way to stop any migration.
func TestCancelRouteAuthorization(t *testing.T) {
	const collection = "Movies"
	principal := &models.Principal{Username: "u1"}

	tests := []struct {
		name string
		// authzErr is what the authorizer answers the submit-side check with.
		authzErr       func() error
		reindexEnabled bool
		wantForbidden  bool
	}{
		{
			name: "a denial while the feature is on",
			authzErr: func() error {
				return forbidden(principal, authorization.UPDATE, authorization.Collections(collection)[0])
			},
			reindexEnabled: true,
			wantForbidden:  true,
		},
		{
			// Pins the ordering: kill switch first would 400 without ever
			// consulting the authorizer.
			name: "a denial while the feature is off",
			authzErr: func() error {
				return forbidden(principal, authorization.UPDATE, authorization.Collections(collection)[0])
			},
			wantForbidden: true,
		},
		{
			name:           "an authorizer that cannot answer",
			authzErr:       func() error { return errAuthorizerUnavailable },
			reindexEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, svc := cancelFixture(t, confirmingCleanupProber{})
			h.appState.ServerConfig.Config.RuntimeReindexEnabled = tc.reindexEnabled
			authz := &recordingSubmitAuthorizer{err: tc.authzErr()}
			h.appState.Authorizer = authz

			responder := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
				HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
				ClassName:    collection,
				PropertyName: "title",
				Body:         &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Cancel: true}},
			}, principal)

			if tc.wantForbidden {
				_, ok := responder.(*schema.SchemaObjectsIndexesUpdateForbidden)
				require.Truef(t, ok, "a caller without update_collections must be refused with 403, got %T", responder)
			} else {
				_, ok := responder.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
				require.Truef(t, ok, "an authorizer that cannot answer must not admit the cancel, got %T", responder)
			}

			require.Equal(t, []string{authorization.UPDATE}, authz.verbs,
				"stopping a cluster-wide migration is an UPDATE on the collection, and the check "+
					"has to happen whatever the kill switch says")
			require.Empty(t, svc.cancelled,
				"a refused caller stopped the migration anyway")
			require.Len(t, svc.startedTasks(), 1, "the running migration must be left alone")
		})
	}
}

// -----------------------------------------------------------------------------
// Wiring.
// -----------------------------------------------------------------------------

// Each collaborator fails OPEN when nil, so a wiring regression must be
// reported — nothing else would catch it.
func TestNewIndexesHandlersReportsUnwiredGateCollaborators(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	// A cluster service and nothing else: the shape a wiring regression leaves.
	newIndexesHandlers(&state.State{
		ClusterService: &rCluster.Service{},
		Logger:         logger,
	})

	entry := entryWithMessage(hook, "gate collaborators are missing")
	require.NotNilf(t, entry,
		"a node with a cluster service and no probes runs reindex submissions unchecked "+
			"against backups, and says nothing about it; entries were %q", entryMessages(hook))
	require.Equal(t, logrus.ErrorLevel, entry.Level,
		"a disabled safety gate is not a warning about a fixture")
	require.ElementsMatch(t,
		[]string{
			"backupActivity (no peer is asked whether it holds a backup slot)",
			"localBackupActivity (this node's own slots are never read)",
			"cluster (there is no node list to fan the backup probe out over)",
		},
		entry.Data["unwired"],
		"the line has to name which collaborator is missing, or it cannot be acted on")
}
