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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// flippingTaskService cancels like raceTaskService, except the cancel fails
// with a scripted error and moves the task to a scripted status first — the
// server-side view of a task that changed between the listing the handler read
// and the cancel it sent.
type flippingTaskService struct {
	*raceTaskService
	cancelErr error
	flipTo    distributedtask.TaskStatus
	// listErrAfterCancel fails every listing taken after the cancel, leaving
	// the handler without the task's new status.
	listErrAfterCancel error
	cancelled          atomic.Bool
}

func (s *flippingTaskService) CancelDistributedTask(
	_ context.Context, _, taskID string, _ uint64,
) error {
	s.mu.Lock()
	for _, t := range s.tasks {
		if t.ID == taskID && s.flipTo != "" {
			t.Status = s.flipTo
		}
	}
	s.mu.Unlock()
	s.cancelled.Store(true)
	return s.cancelErr
}

func (s *flippingTaskService) ListDistributedTasks(
	ctx context.Context,
) (map[string][]*distributedtask.Task, error) {
	if s.cancelled.Load() && s.listErrAfterCancel != nil {
		return nil, s.listErrAfterCancel
	}
	return s.raceTaskService.ListDistributedTasks(ctx)
}

// A task that is STARTED when the handler lists tasks can be PREPARING by the
// time the cancel reaches DTM, which refuses anything but STARTED. The very
// same state answers 409 when the flip happens a moment earlier, because the
// pre-cancel check catches it there. Mapping every cancel error to a 500 made
// the operator's answer depend on which side of that window the transition
// landed on.
//
// The refusal is only right for a task the backup gate still counts as live:
// only then is "poll until every index is ready" true. A cancel that failed
// for any other reason must still fail loudly — telling an operator to poll
// while RAFT is unreachable buries the real fault.
func TestCancelThatRacesTheCommitAnswersLikeTheCheckThatMissedIt(t *testing.T) {
	const (
		collection = "Movies"
		property   = "title"
		indexType  = "filterable"
		taskID     = "t1"
	)

	const refusal = "cancel refused: the migration has finished building and is committing its result; " +
		"it can no longer be cancelled. Poll GET /v1/schema/<class>/indexes until every " +
		"index reports status=\"ready\"."

	// notRunning is what the FSM returns for a cancel of a task that is not
	// STARTED: the specific sentinel and the umbrella, joined, as
	// distributedtask.wrapPermanent builds it.
	notRunning := permanentRejection(distributedtask.ErrTaskNotRunning,
		"[dtm-perm/task-not-running] task reindex/t1/1 is no longer running")

	tests := []struct {
		name string
		// cancelErr is what DTM answers the cancel with.
		cancelErr error
		// flipTo is the status the task holds from the cancel onwards.
		flipTo distributedtask.TaskStatus
		// listErrAfterCancel fails the listing the error path takes.
		listErrAfterCancel error
		// wantConflict is true when the operator must get the refusal.
		wantConflict bool
		// wantServerErrorContains is the substring the 500 must carry.
		wantServerErrorContains string
	}{
		{
			name:         "the task reaches preparing before the cancel lands",
			cancelErr:    notRunning,
			flipTo:       distributedtask.TaskStatusPreparing,
			wantConflict: true,
		},
		{
			name:         "the task reaches swapping before the cancel lands",
			cancelErr:    notRunning,
			flipTo:       distributedtask.TaskStatusSwapping,
			wantConflict: true,
		},
		{
			// A status a newer node introduced is live to the backup gate, so
			// it is live here too — the gate refuses backups of the collection
			// and names this endpoint as the way out.
			name:         "the task reaches a status this build does not recognize",
			cancelErr:    notRunning,
			flipTo:       distributedtask.TaskStatus("REBALANCING"),
			wantConflict: true,
		},
		{
			// The migration is genuinely live and uncancellable, but nothing
			// says so: the cancel failed on the transport. Answering "poll
			// until ready" would hide an unreachable leader behind a refusal
			// the operator reads as normal.
			name:                    "raft is unavailable while the task is preparing",
			cancelErr:               errors.New("raft: leader election in progress"),
			flipTo:                  distributedtask.TaskStatusPreparing,
			wantServerErrorContains: "raft: leader election in progress",
		},
		{
			name:                    "the cancel times out while the task is preparing",
			cancelErr:               errors.New("context deadline exceeded"),
			flipTo:                  distributedtask.TaskStatusPreparing,
			wantServerErrorContains: "context deadline exceeded",
		},
		{
			// A version that moved under the task is a different rejection.
			// It says the task the handler addressed is gone, not that a live
			// one is committing, so it must not borrow the refusal.
			name: "the task version moved under the cancel",
			cancelErr: permanentRejection(distributedtask.ErrTaskDoesNotExist,
				"[dtm-perm/task-not-exist] task reindex/t1/1 does not exist"),
			flipTo:                  distributedtask.TaskStatusPreparing,
			wantServerErrorContains: "does not exist",
		},
		{
			// A permanent rejection this build cannot name is not evidence of
			// a committing migration.
			name:                    "the rejection carries a marker this build does not know",
			cancelErr:               distributedtask.ErrPermanentRejection,
			flipTo:                  distributedtask.TaskStatusPreparing,
			wantServerErrorContains: "permanent FSM rejection",
		},
		{
			// The task settled instead of moving on. It is not live, so the
			// refusal's instruction does not fit, and the cancel error stands.
			name:                    "the task finishes before the cancel lands",
			cancelErr:               notRunning,
			flipTo:                  distributedtask.TaskStatusFinished,
			wantServerErrorContains: "is no longer running",
		},
		{
			// Without a listing the new status is unknown, and a refusal would
			// be a guess.
			name:                    "the listing that would settle the status fails",
			cancelErr:               notRunning,
			flipTo:                  distributedtask.TaskStatusPreparing,
			listErrAfterCancel:      errors.New("raft: not leader"),
			wantServerErrorContains: "is no longer running",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			started := buildTask(t, taskID, distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeRepairFilterable,
				Collection:    collection,
				Properties:    []string{property},
			}, nil)
			svc := &flippingTaskService{
				raceTaskService:    &raceTaskService{tasks: []*distributedtask.Task{started}},
				cancelErr:          tc.cancelErr,
				flipTo:             tc.flipTo,
				listErrAfterCancel: tc.listErrAfterCancel,
			}
			var busy atomic.Bool
			h := submissionHandlers(t, svc, togglingProber{busy: &busy})
			h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger,
				fixtureNode, func() int { return 1 }, context.Background()))

			responder := h.cancelReindexTask(context.Background(), collection, property, indexType,
				&models.Principal{Username: "u1"})

			require.True(t, svc.cancelled.Load(),
				"the task was STARTED in the listing, so the handler must have tried to cancel it")

			if tc.wantConflict {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok,
					"the task is live to the backup gate and past STARTED — the same state the "+
						"pre-cancel check answers with a 409. A 500 here makes the answer depend on "+
						"whether the flip beat the listing, got %T", responder)
				require.Equal(t, refusal, errorMessage(t, conflict.Payload),
					"both refusals must read identically or the two paths drift")
				return
			}

			serverErr, ok := responder.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
			require.Truef(t, ok,
				"this cancel failure is not a live migration committing; a 409 telling the "+
					"operator to poll would bury it, got %T", responder)
			require.Contains(t, errorMessage(t, serverErr.Payload), tc.wantServerErrorContains)
			require.NotContains(t, errorMessage(t, serverErr.Payload), "Poll GET",
				"the refusal's instruction must not reach a failure it does not describe")
		})
	}
}
