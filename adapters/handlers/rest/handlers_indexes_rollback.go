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
	"fmt"
	"net/http"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/reindex"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const (
	submitRollbackAttempts = 3
	submitRollbackBackoff  = 250 * time.Millisecond
	// The caller is already waiting on this, and the retries below are the only
	// thing that can extend it.
	submitRollbackTimeout = 10 * time.Second
)

type rollbackOutcome int

const (
	rollbackCancelled rollbackOutcome = iota
	rollbackCancelledAfterRetry
	rollbackTaskGone
	rollbackTaskTerminal
	rollbackRefused
	rollbackFailed
)

// landed is true when no reindex will run from the rolled-back submission.
func (o rollbackOutcome) landed() bool {
	switch o {
	case rollbackCancelled, rollbackCancelledAfterRetry, rollbackTaskGone, rollbackTaskTerminal:
		return true
	case rollbackRefused, rollbackFailed:
		return false
	}
	return false
}

// One line per rollback, at a level separating what pages from what does not:
// a task still running afterwards needs an operator, one that stopped does not.
func (o rollbackOutcome) auditEvent() (event string, level logrus.Level) {
	switch o {
	case rollbackCancelled:
		return "reindex_submit_rollback_cancelled", logrus.InfoLevel
	case rollbackCancelledAfterRetry:
		return "reindex_submit_rollback_cancelled_after_retry", logrus.InfoLevel
	case rollbackTaskGone:
		return "reindex_submit_rollback_task_gone", logrus.WarnLevel
	case rollbackTaskTerminal:
		return "reindex_submit_rollback_task_terminal", logrus.InfoLevel
	case rollbackRefused:
		return "reindex_submit_rollback_refused", logrus.ErrorLevel
	case rollbackFailed:
		return "reindex_submit_rollback_failed", logrus.ErrorLevel
	}
	return "reindex_submit_rollback_failed", logrus.ErrorLevel
}

// The metric label, drawn from the same closed set the counter declares.
func (o rollbackOutcome) label() string {
	switch o {
	case rollbackCancelled:
		return "cancelled"
	case rollbackCancelledAfterRetry:
		return "cancelled_after_retry"
	case rollbackTaskGone:
		return "task_gone"
	case rollbackTaskTerminal:
		return "task_terminal"
	case rollbackRefused:
		return "refused"
	case rollbackFailed:
		return "failed"
	}
	return "failed"
}

// RollbackOutcomeLabels is every value [rollbackOutcome.label] can return, so
// each series exists at zero before the first rollback rather than appearing
// only once one has happened.
func RollbackOutcomeLabels() []string {
	return []string{
		rollbackCancelled.label(), rollbackCancelledAfterRetry.label(),
		rollbackTaskGone.label(), rollbackTaskTerminal.label(),
		rollbackRefused.label(), rollbackFailed.label(),
	}
}

func (o rollbackOutcome) summary() string {
	switch o {
	case rollbackCancelled:
		return "rollback: cancelled the reindex task this request had just committed"
	case rollbackCancelledAfterRetry:
		return "rollback: cancelled the reindex task this request had just committed, after a retry"
	case rollbackTaskGone:
		return "rollback: the reindex task this request had just committed is no longer in the task store"
	case rollbackTaskTerminal:
		return "rollback: the reindex task this request had just committed reached a terminal state on its own"
	case rollbackRefused:
		return "rollback: the task store refuses to cancel the reindex task this request had just committed; it is still running"
	case rollbackFailed:
		return "rollback: could not reach the task store to stop the reindex task this request had just committed; it is still running"
	}
	return ""
}

// releaseLocks runs before the rollback does, so an unrelated DELETE on this
// property and every backup of this collection stop waiting on a rollback
// nobody is listening for.
func (h *indexesHandlers) rollbackSubmit(ctx context.Context, principal *models.Principal,
	svc reindexTaskCanceller, collection, taskID, cancelRemedy string,
	scan backupActivityScan, releaseLocks func(),
) middleware.Responder {
	releaseLocks()

	strippedID := namespacing.StripOwnNamespace(principal, taskID)
	// Nobody answering is what a disconnected client produces on its own, and
	// rolling back on it would destroy a migration that committed cleanly.
	if scan.verdict == backupActivityUnreachable {
		h.logBackupActivityRefusal(collection, "unreachable", scan)
		h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, reindex.VerdictUnreachable)
		return jsonResponder(http.StatusServiceUnavailable, refusalNamingTask(principal, strippedID, fmt.Sprintf(
			"cannot confirm the cluster is free of backups: a node did not answer the "+
				"backup-activity probe. Reindex task %q was committed and is running; "+
				"cancel it if a backup turns out to have been in flight", strippedID)))
	}

	h.logBackupActivityRefusal(collection, "busy", scan)
	h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, submitRefusalVerdict(scan.kind))

	// Detached from the request: the answer below is a 409 whether or not the
	// caller is still there, and a task left running would 409 that caller's
	// own retry for a task it was never told about.
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), submitRollbackTimeout)
	defer cancel()

	outcome, fault := rollbackReindexSubmit(rollbackCtx, svc, taskID)
	h.logRollbackOutcome(collection, taskID, outcome, fault)
	h.appState.ReindexGateMetrics.RolledBack(outcome.label())

	if outcome.landed() {
		return jsonResponder(http.StatusConflict,
			errorResponse(principal, backupBusyRefusal(scan.kind)))
	}
	return jsonResponder(http.StatusConflict, refusalNamingTask(principal, strippedID, fmt.Sprintf(
		"reindex blocked: a %s is running in the cluster, and reindex task %q that this request "+
			"committed could not be stopped; %s stops it, then retry",
		publishableActivityKind(scan.kind), strippedID, cancelRemedy)))
}

// A field of its own as well as the prose: an id read out of a sentence breaks
// when the sentence is reworded.
func refusalNamingTask(principal *models.Principal, taskID, msg string) *models.IndexRefusalResponse {
	return &models.IndexRefusalResponse{
		ErrorResponse: *errorResponse(principal, msg),
		TaskID:        taskID,
	}
}

// The caller named the property in their own request, so a refusal that sends
// them back to it can name the exact call rather than a template.
func reindexCancelRemedy(collection, propertyName string, migrationType db.ReindexMigrationType) string {
	indexTypes, known := indexTypesFromMigrationType(migrationType)
	if !known || len(indexTypes) == 0 {
		return reindex.IndexesRoute(collection)
	}
	return reindex.CancelRoute(collection, propertyName, canonicalIndexType(indexTypes[0]))
}

// Bounded, because the caller is already waiting on the answer this decides.
func rollbackReindexSubmit(ctx context.Context, svc reindexTaskCanceller, taskID string,
) (rollbackOutcome, error) {
	outcome, fault := rollbackFailed, error(nil)
	for attempt := range submitRollbackAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return outcome, fault
			case <-time.After(submitRollbackBackoff):
			}
		}

		var settled bool
		outcome, fault, settled = rollbackReindexSubmitOnce(ctx, svc, taskID)
		if !settled {
			continue
		}
		if outcome == rollbackCancelled && attempt > 0 {
			return rollbackCancelledAfterRetry, nil
		}
		return outcome, fault
	}
	return outcome, fault
}

// rollbackReindexSubmitOnce reports settled=false for a fault worth retrying.
func rollbackReindexSubmitOnce(ctx context.Context, svc reindexTaskCanceller, taskID string,
) (outcome rollbackOutcome, fault error, settled bool) {
	tasks, err := svc.ListDistributedTasks(ctx)
	if err != nil {
		return rollbackFailed, err, false
	}

	target := findReindexTaskByID(tasks[db.ReindexNamespace], taskID)
	switch {
	case target == nil:
		return rollbackTaskGone, nil, true
	case !target.Status.IsActive():
		return rollbackTaskTerminal, nil, true
	case !target.Status.IsCancellable():
		// A cancel is accepted only while the task is STARTED, and leaving the
		// phase it is in takes a scheduler tick — far past this request.
		return rollbackRefused, nil, true
	}

	switch err := svc.CancelDistributedTask(ctx, target.Namespace, target.ID, target.Version); {
	case err == nil:
		return rollbackCancelled, nil, true
	case errors.Is(err, distributedtask.ErrTaskDoesNotExist):
		return rollbackTaskGone, nil, true
	case errors.Is(err, distributedtask.ErrTaskNotRunning):
		return rollbackTaskTerminal, nil, true
	default:
		return rollbackFailed, err, false
	}
}

func findReindexTaskByID(tasks []*distributedtask.Task, taskID string) *distributedtask.Task {
	for _, task := range tasks {
		if task.ID == taskID {
			return task
		}
	}
	return nil
}

func (h *indexesHandlers) logRollbackOutcome(collection, taskID string, outcome rollbackOutcome, fault error) {
	if h.appState.Logger == nil {
		return
	}
	event, level := outcome.auditEvent()
	entry := h.appState.Logger.WithFields(logrus.Fields{
		"audit_event": event,
		"collection":  collection,
		"taskID":      taskID,
	})
	if fault != nil {
		entry.Logf(level, "%s: %v", outcome.summary(), fault)
		return
	}
	entry.Log(level, outcome.summary())
}
