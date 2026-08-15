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
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

const rolledBackTaskID = "Movies:change-tokenization:title:ab3f"

// scriptedCanceller answers each call from its script, so a row can say "the
// second attempt is the one that works" without timing anything.
type scriptedCanceller struct {
	tasks      []*distributedtask.Task
	listErrs   []error
	cancelErrs []error

	listCalls   int
	cancelCalls int
}

func (c *scriptedCanceller) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	attempt := c.listCalls
	c.listCalls++
	if attempt < len(c.listErrs) && c.listErrs[attempt] != nil {
		return nil, c.listErrs[attempt]
	}
	return map[string][]*distributedtask.Task{db.ReindexNamespace: c.tasks}, nil
}

func (c *scriptedCanceller) CancelDistributedTask(_ context.Context, _, _ string, _ uint64) error {
	attempt := c.cancelCalls
	c.cancelCalls++
	if attempt < len(c.cancelErrs) {
		return c.cancelErrs[attempt]
	}
	return nil
}

func rolledBackTask(t *testing.T, status distributedtask.TaskStatus) *distributedtask.Task {
	t.Helper()
	return buildTask(t, rolledBackTaskID, status,
		db.ReindexTaskPayload{MigrationType: db.ReindexTypeChangeTokenization, Collection: "Movies"}, nil)
}

// TestRollbackReindexSubmitOutcomes walks every way a rollback can end. The
// distinction that matters is whether it landed: a task still running after
// this has to be named to the caller, or their retry is refused for a task
// they were never told about.
func TestRollbackReindexSubmitOutcomes(t *testing.T) {
	transient := errors.New("raft leader election in progress")

	tests := []struct {
		name        string
		canceller   *scriptedCanceller
		wantOutcome rollbackOutcome
		wantLanded  bool
		wantLists   int
		wantCancels int
		wantLevel   logrus.Level
		wantEvent   string
	}{
		{
			name:        "cancelled on the first attempt",
			canceller:   &scriptedCanceller{},
			wantOutcome: rollbackCancelled,
			wantLanded:  true,
			wantLists:   1,
			wantCancels: 1,
			wantLevel:   logrus.InfoLevel,
			wantEvent:   "reindex_submit_rollback_cancelled",
		},
		{
			name:        "a listing that failed once, then worked",
			canceller:   &scriptedCanceller{listErrs: []error{transient}},
			wantOutcome: rollbackCancelledAfterRetry,
			wantLanded:  true,
			wantLists:   2,
			wantCancels: 1,
			wantLevel:   logrus.InfoLevel,
			wantEvent:   "reindex_submit_rollback_cancelled_after_retry",
		},
		{
			name:        "a cancel that failed once, then worked",
			canceller:   &scriptedCanceller{cancelErrs: []error{transient}},
			wantOutcome: rollbackCancelledAfterRetry,
			wantLanded:  true,
			wantLists:   2,
			wantCancels: 2,
			wantLevel:   logrus.InfoLevel,
			wantEvent:   "reindex_submit_rollback_cancelled_after_retry",
		},
		{
			name:        "the task is not in the store any more",
			canceller:   &scriptedCanceller{},
			wantOutcome: rollbackTaskGone,
			wantLanded:  true,
			wantLists:   1,
			wantLevel:   logrus.WarnLevel,
			wantEvent:   "reindex_submit_rollback_task_gone",
		},
		{
			name:        "the cancel found the task already deleted",
			canceller:   &scriptedCanceller{cancelErrs: []error{distributedtask.ErrTaskDoesNotExist}},
			wantOutcome: rollbackTaskGone,
			wantLanded:  true,
			wantLists:   1,
			wantCancels: 1,
			wantLevel:   logrus.WarnLevel,
			wantEvent:   "reindex_submit_rollback_task_gone",
		},
		{
			name:        "the cancel found the task already terminal",
			canceller:   &scriptedCanceller{cancelErrs: []error{distributedtask.ErrTaskNotRunning}},
			wantOutcome: rollbackTaskTerminal,
			wantLanded:  true,
			wantLists:   1,
			wantCancels: 1,
			wantLevel:   logrus.InfoLevel,
			wantEvent:   "reindex_submit_rollback_task_terminal",
		},
		{
			name:        "the task went terminal between the listing and the cancel",
			canceller:   &scriptedCanceller{},
			wantOutcome: rollbackTaskTerminal,
			wantLanded:  true,
			wantLists:   1,
			wantLevel:   logrus.InfoLevel,
			wantEvent:   "reindex_submit_rollback_task_terminal",
		},
		{
			name:        "the task is still coordinating its preparation",
			canceller:   &scriptedCanceller{},
			wantOutcome: rollbackRefused,
			wantLanded:  false,
			wantLists:   1,
			wantLevel:   logrus.ErrorLevel,
			wantEvent:   "reindex_submit_rollback_refused",
		},
		{
			name:        "the task store never answered",
			canceller:   &scriptedCanceller{listErrs: []error{transient, transient, transient}},
			wantOutcome: rollbackFailed,
			wantLanded:  false,
			wantLists:   submitRollbackAttempts,
			wantLevel:   logrus.ErrorLevel,
			wantEvent:   "reindex_submit_rollback_failed",
		},
		{
			name:        "the cancel never went through",
			canceller:   &scriptedCanceller{cancelErrs: []error{transient, transient, transient}},
			wantOutcome: rollbackFailed,
			wantLanded:  false,
			wantLists:   submitRollbackAttempts,
			wantCancels: submitRollbackAttempts,
			wantLevel:   logrus.ErrorLevel,
			wantEvent:   "reindex_submit_rollback_failed",
		},
	}

	// The status each row's task carries, keyed off the outcome it expects.
	statusFor := map[rollbackOutcome]distributedtask.TaskStatus{
		rollbackCancelled:           distributedtask.TaskStatusStarted,
		rollbackCancelledAfterRetry: distributedtask.TaskStatusStarted,
		rollbackTaskGone:            distributedtask.TaskStatusStarted,
		rollbackTaskTerminal:        distributedtask.TaskStatusStarted,
		rollbackRefused:             distributedtask.TaskStatusPreparing,
		rollbackFailed:              distributedtask.TaskStatusStarted,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "the task is not in the store any more":
				// tasks stays empty
			case "the task went terminal between the listing and the cancel":
				tt.canceller.tasks = []*distributedtask.Task{
					rolledBackTask(t, distributedtask.TaskStatusCancelled),
				}
			default:
				tt.canceller.tasks = []*distributedtask.Task{
					rolledBackTask(t, statusFor[tt.wantOutcome]),
				}
			}

			outcome, _ := rollbackReindexSubmit(context.Background(), tt.canceller, rolledBackTaskID)

			assert.Equal(t, tt.wantOutcome, outcome)
			assert.Equal(t, tt.wantLanded, outcome.landed())
			assert.Equal(t, tt.wantLists, tt.canceller.listCalls)
			assert.Equal(t, tt.wantCancels, tt.canceller.cancelCalls)

			event, level := outcome.auditEvent()
			assert.Equal(t, tt.wantEvent, event)
			assert.Equal(t, tt.wantLevel, level)

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h := &indexesHandlers{appState: &state.State{Logger: logger}}
			h.logRollbackOutcome("Movies", rolledBackTaskID, outcome, nil)

			require.Len(t, hook.AllEntries(), 1, "exactly one operator-facing line per rollback")
			assert.Equal(t, tt.wantLevel, hook.AllEntries()[0].Level)
			assert.Equal(t, tt.wantEvent, hook.AllEntries()[0].Data["audit_event"])
		})
	}
}

// The retries exist to ride out a leader election, so they have to be spaced
// far enough apart to reach a different leader and few enough to answer the
// caller who is still waiting.
func TestRollbackReindexSubmitRetryBudget(t *testing.T) {
	transient := errors.New("raft leader election in progress")
	canceller := &scriptedCanceller{listErrs: []error{transient, transient, transient}}

	started := time.Now()
	outcome, fault := rollbackReindexSubmit(context.Background(), canceller, rolledBackTaskID)
	elapsed := time.Since(started)

	assert.Equal(t, rollbackFailed, outcome)
	assert.ErrorIs(t, fault, transient, "the log line has to name what actually failed")
	assert.Equal(t, submitRollbackAttempts, canceller.listCalls)
	assert.GreaterOrEqual(t, elapsed, time.Duration(submitRollbackAttempts-1)*submitRollbackBackoff)
}

// A rollback outlives the request that started it: the caller may already be
// gone, and a task left running would refuse their retry.
func TestRollbackReindexSubmitSurvivesTheRequest(t *testing.T) {
	canceller := &scriptedCanceller{tasks: []*distributedtask.Task{
		rolledBackTask(t, distributedtask.TaskStatusStarted),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcome, _ := rollbackReindexSubmit(context.WithoutCancel(ctx), canceller, rolledBackTaskID)

	assert.Equal(t, rollbackCancelled, outcome)
	assert.Equal(t, 1, canceller.cancelCalls)
}

// TestRollbackSubmitResponses pins what each verdict publishes. A rollback
// that landed leaves nothing for the caller to act on, so it answers exactly
// what the pre-commit refusal does; one that did not has to name the task it
// left running.
func TestRollbackSubmitResponses(t *testing.T) {
	tests := []struct {
		name            string
		scan            backupActivityScan
		canceller       *scriptedCanceller
		taskStatus      distributedtask.TaskStatus
		wantCode        int
		wantContains    []string
		wantNotContains []string
		wantCancels     int
	}{
		{
			name:            "a peer backing up, rolled back",
			scan:            backupActivityScan{verdict: backupActivityBusy, kind: "backup", id: "backup-42", node: "node-3"},
			taskStatus:      distributedtask.TaskStatusStarted,
			wantCode:        http.StatusConflict,
			wantContains:    []string{"reindex blocked: a backup is running in the cluster; retry after it finishes"},
			wantNotContains: []string{rolledBackTaskID, "node-3", "backup-42"},
			wantCancels:     1,
		},
		{
			name:            "a peer restoring, rolled back",
			scan:            backupActivityScan{verdict: backupActivityBusy, kind: "restore", id: "restore-9"},
			taskStatus:      distributedtask.TaskStatusStarted,
			wantCode:        http.StatusConflict,
			wantContains:    []string{"reindex blocked: a restore is running in the cluster; retry after it finishes"},
			wantNotContains: []string{rolledBackTaskID, "restore-9"},
			wantCancels:     1,
		},
		{
			name:       "a peer backing up, and the task could not be stopped",
			scan:       backupActivityScan{verdict: backupActivityBusy, kind: "backup", id: "backup-42", node: "node-3"},
			taskStatus: distributedtask.TaskStatusPreparing,
			wantCode:   http.StatusConflict,
			wantContains: []string{
				"a backup is running in the cluster",
				rolledBackTaskID,
				"could not be stopped",
				"POST /v1/schema/Movies/properties/title/index/searchable/cancel",
			},
			wantNotContains: []string{"node-3", "backup-42"},
		},
		{
			name:     "a peer that did not answer",
			scan:     backupActivityScan{verdict: backupActivityUnreachable, node: "node-3", fault: errors.New("connection refused")},
			wantCode: http.StatusServiceUnavailable,
			wantContains: []string{
				"cannot confirm the cluster is free of backups",
				rolledBackTaskID,
				"was committed and is running",
			},
			wantNotContains: []string{"node-3", "connection refused"},
			// Nobody answering is what a disconnected client produces, and
			// rolling back on it destroys a migration that committed cleanly.
			wantCancels: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canceller := &scriptedCanceller{}
			if tt.taskStatus != "" {
				canceller.tasks = []*distributedtask.Task{rolledBackTask(t, tt.taskStatus)}
			}
			h := &indexesHandlers{appState: &state.State{Logger: quietLogger()}}

			released := 0
			resp := h.rollbackSubmit(context.Background(), nil, canceller, "Movies", rolledBackTaskID,
				reindexCancelRemedy("Movies", "title", db.ReindexTypeChangeTokenization),
				tt.scan, func() { released++ })

			code, body := statusOf(t, resp)
			require.Len(t, body.Error, 1)
			assert.Equal(t, tt.wantCode, code)
			for _, want := range tt.wantContains {
				assert.Contains(t, body.Error[0].Message, want)
			}
			for _, unwanted := range tt.wantNotContains {
				assert.NotContains(t, body.Error[0].Message, unwanted)
			}
			assert.Equal(t, tt.wantCancels, canceller.cancelCalls)
			assert.Equal(t, 1, released,
				"the property lock and the collection gate are released before the rollback runs")
		})
	}
}

// The locks have to be gone before the rollback starts, or an unrelated DELETE
// on this property and every backup of this collection wait out a rollback
// nobody is listening for.
func TestRollbackSubmitReleasesLocksBeforeCancelling(t *testing.T) {
	var steps []string
	recording := &recordingCanceller{
		inner: &scriptedCanceller{tasks: []*distributedtask.Task{
			rolledBackTask(t, distributedtask.TaskStatusStarted),
		}},
		steps: &steps,
	}
	h := &indexesHandlers{appState: &state.State{Logger: quietLogger()}}

	_ = h.rollbackSubmit(context.Background(), nil, recording, "Movies", rolledBackTaskID,
		reindexCancelRemedy("Movies", "title", db.ReindexTypeChangeTokenization),
		backupActivityScan{verdict: backupActivityBusy, kind: "backup"},
		func() { steps = append(steps, "release") })

	assert.Equal(t, []string{"release", "list", "cancel"}, steps)
}

type recordingCanceller struct {
	inner *scriptedCanceller
	steps *[]string
}

func (c *recordingCanceller) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	*c.steps = append(*c.steps, "list")
	return c.inner.ListDistributedTasks(ctx)
}

func (c *recordingCanceller) CancelDistributedTask(ctx context.Context, ns, id string, version uint64) error {
	*c.steps = append(*c.steps, "cancel")
	return c.inner.CancelDistributedTask(ctx, ns, id, version)
}

// TestReindexCancelRemedy pins the rendered path itself, so a mis-ordered
// argument list reds a test instead of shipping a call the API rejects.
func TestReindexCancelRemedy(t *testing.T) {
	tests := []struct {
		name          string
		migrationType db.ReindexMigrationType
		want          string
	}{
		{
			name:          "a tokenization change names the searchable index it rewrites",
			migrationType: db.ReindexTypeChangeTokenization,
			want:          "POST /v1/schema/Movies/properties/title/index/searchable/cancel",
		},
		{
			name:          "a filterable retokenize names the filterable index",
			migrationType: db.ReindexTypeChangeTokenizationFilterable,
			want:          "POST /v1/schema/Movies/properties/title/index/filterable/cancel",
		},
		{
			name:          "rangeFilters is rendered in the spelling the API accepts",
			migrationType: db.ReindexTypeEnableRangeable,
			want:          "POST /v1/schema/Movies/properties/title/index/rangeFilters/cancel",
		},
		{
			name:          "a type this build cannot map sends the operator to the read",
			migrationType: db.ReindexMigrationType("from-a-newer-node"),
			want:          "GET /v1/schema/Movies/indexes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, reindexCancelRemedy("Movies", "title", tt.migrationType))
		})
	}
}
