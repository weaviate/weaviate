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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/reindex"
)

const rolledBackTaskID = "Movies:change-tokenization:title:ab3f"

// Scripted per call, so a row can say "the second attempt works" without timing.
type scriptedCanceller struct {
	tasks      []*distributedtask.Task
	listErrs   []error
	cancelErrs []error

	listCalls   int
	cancelCalls int
}

func (c *scriptedCanceller) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	// The real store answers a dead context with its error, which is what makes
	// a rollback that inherited the request's cancellation visible.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
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

// Every way a rollback can end, and whether each one landed.
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

			started := time.Now()
			outcome, fault := rollbackReindexSubmit(context.Background(), tt.canceller, rolledBackTaskID)
			elapsed := time.Since(started)

			assert.Equal(t, tt.wantOutcome, outcome)
			// The backoff between attempts really elapsed.
			assert.GreaterOrEqual(t, elapsed, time.Duration(tt.wantLists-1)*submitRollbackBackoff)
			if outcome == rollbackFailed {
				assert.ErrorIs(t, fault, transient, "the log line has to name what failed")
			}
			assert.Equal(t, tt.wantLanded, outcome.landed())
			assert.Equal(t, tt.wantLists, tt.canceller.listCalls)
			assert.Equal(t, tt.wantCancels, tt.canceller.cancelCalls)

			event, level := outcome.auditEvent()
			assert.Equal(t, tt.wantEvent, event)
			assert.Equal(t, tt.wantLevel, level)

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h := &indexesHandlers{appState: &state.State{Logger: logger}}
			h.logRollbackOutcome(&models.Principal{Username: "alice"}, "Movies",
				rolledBackTaskID, outcome, nil)

			require.Len(t, hook.AllEntries(), 1, "exactly one operator-facing line per rollback")
			assert.Equal(t, tt.wantLevel, hook.AllEntries()[0].Level)
			assert.Equal(t, tt.wantEvent, hook.AllEntries()[0].Data["audit_event"])
			assert.Equal(t, "alice", hook.AllEntries()[0].Data["principal"],
				"a cluster-wide operation that had to be undone has to say whose it was")
		})
	}
}

// A rollback that landed leaves nothing to act on, so it answers exactly what
// the pre-commit refusal does; one that did not names the task still running.
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
		wantTaskID      string
		// A client that hung up before the refusal was written.
		requestCtxCancelled bool
		// The counter labels this exit must produce.
		wantRefusalVerdict  string
		wantRollbackOutcome string
	}{
		{
			name:            "a peer backing up, rolled back",
			scan:            backupActivityScan{verdict: backupActivityBusy, kind: "backup", id: "backup-42", node: "node-3"},
			taskStatus:      distributedtask.TaskStatusStarted,
			wantCode:        http.StatusConflict,
			wantContains:    []string{"reindex blocked: a backup is running in the cluster; retry after it finishes"},
			wantNotContains: []string{rolledBackTaskID, "node-3", "backup-42"},
			wantCancels:     1,

			wantRefusalVerdict:  reindex.VerdictBackupBusy,
			wantRollbackOutcome: "cancelled",
		},
		{
			name:            "a peer restoring, rolled back",
			scan:            backupActivityScan{verdict: backupActivityBusy, kind: "restore", id: "restore-9"},
			taskStatus:      distributedtask.TaskStatusStarted,
			wantCode:        http.StatusConflict,
			wantContains:    []string{"reindex blocked: a restore is running in the cluster; retry after it finishes"},
			wantNotContains: []string{rolledBackTaskID, "restore-9"},
			wantCancels:     1,

			wantRefusalVerdict:  reindex.VerdictRestoreBusy,
			wantRollbackOutcome: "cancelled",
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
			wantTaskID:      rolledBackTaskID,

			wantRefusalVerdict:  reindex.VerdictBackupBusy,
			wantRollbackOutcome: "refused",
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
			wantCancels:     0,
			wantTaskID:      rolledBackTaskID,

			wantRefusalVerdict: reindex.VerdictUnreachable,
		},
		{
			// The rollback has to outlive the request: a task left running
			// would 409 this caller's own retry for a task it never heard of.
			name:                "a caller that hung up before the refusal was written",
			scan:                backupActivityScan{verdict: backupActivityBusy, kind: "backup", id: "backup-42"},
			taskStatus:          distributedtask.TaskStatusStarted,
			requestCtxCancelled: true,
			wantCode:            http.StatusConflict,
			wantContains:        []string{"reindex blocked: a backup is running in the cluster"},
			wantNotContains:     []string{rolledBackTaskID},
			wantCancels:         1,

			wantRefusalVerdict:  reindex.VerdictBackupBusy,
			wantRollbackOutcome: "cancelled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.requestCtxCancelled {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			canceller := &scriptedCanceller{}
			if tt.taskStatus != "" {
				canceller.tasks = []*distributedtask.Task{rolledBackTask(t, tt.taskStatus)}
			}
			registry := prometheus.NewPedanticRegistry()
			h := &indexesHandlers{appState: &state.State{
				Logger:             quietLogger(),
				ReindexGateMetrics: reindex.NewGateMetrics(registry, nil, RollbackOutcomeLabels()),
			}}

			released := 0
			resp := h.rollbackSubmit(ctx, nil, canceller, "Movies", rolledBackTaskID,
				reindexCancelRemedy("Movies", "title", db.ReindexTypeChangeTokenization),
				tt.scan, func() { released++ })

			rec := httptest.NewRecorder()
			resp.WriteResponse(rec, runtime.JSONProducer())
			var body models.IndexRefusalResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Error, 1, "the error list every client already parses is untouched")

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantContains {
				assert.Contains(t, body.Error[0].Message, want)
			}
			for _, unwanted := range tt.wantNotContains {
				assert.NotContains(t, body.Error[0].Message, unwanted)
			}
			// A refusal that left nothing running must not name a task, or a
			// client scripting off the field cancels one that no longer exists.
			assert.Equal(t, tt.wantTaskID, body.TaskID)
			assert.Equal(t, tt.wantCancels, canceller.cancelCalls)
			assert.Equal(t, 1, released,
				"the property lock and the collection gate are released before the rollback runs")

			// The post-commit path reaches the counter through code the
			// pre-commit one never runs.
			assert.Equalf(t, 1.0, refusalCount(t, registry, tt.wantRefusalVerdict),
				"the post-commit refusal must reach the same counter the pre-commit one does")
			if tt.wantRollbackOutcome != "" {
				assert.Equal(t, 1.0, rollbackCount(t, registry, tt.wantRollbackOutcome))
			}
		})
	}
}

// The locks have to be gone before the rollback starts, not after.
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

// The rendered path itself, so a mis-ordered argument list reds a test instead
// of shipping a call the API rejects.
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

func rollbackCount(t *testing.T, registry *prometheus.Registry, outcome string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "weaviate_reindex_submit_rollbacks_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "outcome" && pair.GetValue() == outcome {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("no rollback series for outcome %q", outcome)
	return 0
}

// An unrecognized outcome must collapse onto a known label, not mint a series.
func TestRollbackOutcomeLabelsAreBounded(t *testing.T) {
	labels := RollbackOutcomeLabels()
	require.Len(t, labels, 6, "one label per outcome, and no more")

	seen := map[string]struct{}{}
	for _, label := range labels {
		_, duplicate := seen[label]
		require.Falsef(t, duplicate, "two outcomes share the label %q", label)
		seen[label] = struct{}{}
	}
	for _, outcome := range []rollbackOutcome{
		rollbackCancelled, rollbackCancelledAfterRetry, rollbackTaskGone,
		rollbackTaskTerminal, rollbackRefused, rollbackFailed, rollbackOutcome(99),
	} {
		assert.Containsf(t, seen, outcome.label(),
			"outcome %d produced the unbounded label %q", outcome, outcome.label())
	}
}
