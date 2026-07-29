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

package reindex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Service owns the reindex read-side status merge, the cancel
// lifecycle, and the schema-mutation conflict pre-flight. HTTP handlers
// should authorize, parse params, call into the Service, and map
// [errors.Is]-matched sentinels in errors.go to HTTP status codes.
type Service struct {
	deps   Deps
	logger logrus.FieldLogger
}

// New constructs a Service. SubmitLocks may be supplied via deps for
// shared lock identity across multiple handler instances; if nil, a
// no-op locker is used (tests).
func New(deps Deps, logger logrus.FieldLogger) *Service {
	if deps.SubmitLocks == nil {
		deps.SubmitLocks = noopLocker{}
	}
	return &Service{deps: deps, logger: logger}
}

// SubmitResult is the service-level outcome surfaced back to the
// handler. TaskID is the DTM task identifier; Status is "CANCELLED" on
// a cancel that hit an in-flight task, [StatusNoOp] for the idempotent
// "nothing to cancel" answer (empty TaskID).
type SubmitResult struct {
	TaskID string
	Status string
}

// StatusNoOp is the cancel-when-nothing-in-flight wire status. M6
// contract: cancel is idempotent and reports NO_OP + 202 rather than
// 404; clients assert this exact string.
const StatusNoOp = "NO_OP"

// StatusCancelled is the wire status for a cancel that reached an
// in-flight task.
const StatusCancelled = "CANCELLED"

// reindexCancelDrainTimeout caps how long Cancel waits for the local
// reindex goroutine to exit before falling back to "let the next
// submit clean up". 10s matches the DTM scheduler's analogous waits.
const reindexCancelDrainTimeout = 10 * time.Second

// FindCancelTargetTask returns the in-flight (STARTED/PREPARING/
// SWAPPING) reindex task matching (collection, propertyName,
// indexType), or nil. A non-STARTED match is still a cancel target;
// the FSM turns "not running" into a NO_OP, not an error.
func FindCancelTargetTask(tasks []*distributedtask.Task, collection, propertyName, indexType string) (*distributedtask.Task, dbreindex.ReindexTaskPayload) {
	task, payload, _ := FirstActiveReindexTask(tasks, DecodeSkip, func(p dbreindex.ReindexTaskPayload) bool {
		if !strings.EqualFold(p.Collection, collection) || !slices.Contains(p.Properties, propertyName) {
			return false
		}
		matches, _ := MigrationTypeTargetsIndex(p.MigrationType, indexType)
		return matches
	})
	return task, payload
}

// Cancel finds the in-flight reindex task targeting (collection,
// propertyName, indexType) and asks DTM to cancel it.
//
// Idempotent: the caller is expected to have already verified that the
// (collection, property) tuple exists (a missing class or property is
// the handler's 404). So "no task in flight" returns
// SubmitResult{Status: StatusNoOp} with an empty TaskID, which the
// handler renders as 202 — NOT a 404. A target the FSM reports as no
// longer running ([distributedtask.ErrTaskNotRunning]) is the same
// idempotent NO_OP rather than a 500.
//
// Returns [ErrServiceUnavailable] when the cluster service is not
// wired; every other error is an unexpected failure the handler maps
// to 500.
func (s *Service) Cancel(ctx context.Context, collection, propertyName, indexType, principalUsername string) (SubmitResult, error) {
	if s.deps.Cluster == nil {
		return SubmitResult{}, fmt.Errorf("%w: cluster service unavailable; cannot cancel reindex task", ErrServiceUnavailable)
	}

	tasks, err := s.deps.Cluster.ListDistributedTasks(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("listing tasks: %w", err)
	}

	target, targetPayload := FindCancelTargetTask(tasks[dbreindex.ReindexNamespace], collection, propertyName, indexType)

	if target == nil {
		// M6 contract: cancel is idempotent. Nothing in flight → 202 with
		// Status: NO_OP and no TaskID, NOT a 404. 404 stays reserved for
		// "collection or property does not exist" (the handler's job).
		s.logger.WithFields(logrus.Fields{
			"audit_event": "reindex_task_cancel_noop",
			"collection":  collection,
			"property":    propertyName,
			"index_type":  indexType,
			"principal":   principalUsername,
		}).Info("cancel: no in-flight task to cancel; returning NO_OP")
		return SubmitResult{Status: StatusNoOp}, nil
	}

	if err := s.deps.Cluster.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		// A PREPARING/SWAPPING or already-completed target makes the FSM
		// reject with ErrTaskNotRunning: nothing to cancel, so NO_OP, not 500.
		if errors.Is(err, distributedtask.ErrTaskNotRunning) {
			s.logger.WithFields(logrus.Fields{
				"audit_event": "reindex_task_cancel_noop",
				"collection":  collection,
				"property":    propertyName,
				"index_type":  indexType,
				"taskID":      target.ID,
				"principal":   principalUsername,
			}).Info("cancel: task no longer running; returning NO_OP")
			return SubmitResult{Status: StatusNoOp}, nil
		}
		return SubmitResult{}, fmt.Errorf("cancelling task: %w", err)
	}

	// Drain the local goroutine BEFORE cleaning partial state.
	// Without this, the cleanup races against the worker which is
	// still writing to __reindex / __ingest buckets.
	if s.deps.Provider != nil {
		s.logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Info("cancel: starting drain+cleanup for cancelled reindex task")
		drainCtx, drainCancel := context.WithTimeout(ctx, reindexCancelDrainTimeout)
		drainErr := s.deps.Provider.WaitForLocalTaskDrain(drainCtx, target.TaskDescriptor)
		drainCancel()
		if drainErr != nil {
			s.logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"index_type": indexType,
			}).Errorf("cancel: timed out waiting for local reindex goroutine to drain (%v); skipping inline cleanup — next submit will retry", drainErr)
		} else {
			s.logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"index_type": indexType,
			}).Info("cancel: drain complete, running on-disk cleanup")
			// Wipe the sidecars and migration directories for every
			// indexType this migration touches — change-tokenization
			// spawns both a searchable and a filterable strategy under
			// one task, so cleaning only the URL's indexType leaves the
			// sibling orphaned.
			indexTypesToClean, known := IndexTypesFromMigrationType(targetPayload.MigrationType)
			if !known || len(indexTypesToClean) == 0 {
				indexTypesToClean = []string{indexType}
			}
			var cleanupErrs []error
			for _, it := range indexTypesToClean {
				if err := s.deps.DB.CleanStalePartialReindexState(ctx, collection, propertyName, it); err != nil {
					cleanupErrs = append(cleanupErrs, fmt.Errorf("indexType=%q: %w", it, err))
				}
			}
			if len(cleanupErrs) > 0 {
				s.logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
					"strategies": indexTypesToClean,
				}).Errorf("cancel: cleaning partial reindex state on disk for %d strategies failed: %v; next submit's defense-in-depth cleanup will retry", len(cleanupErrs), cleanupErrs)
			} else {
				s.logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
				}).Info("cancel: on-disk cleanup complete")
			}
		}
	} else {
		s.logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Warn("cancel: reindex provider not wired up; skipping drain+cleanup")
	}

	s.logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_cancelled",
		"taskID":         target.ID,
		"collection":     collection,
		"property":       propertyName,
		"index_type":     indexType,
		"migration_type": targetPayload.MigrationType,
		"principal":      principalUsername,
	}).Info("reindex provider: cancelled task")

	return SubmitResult{TaskID: target.ID, Status: StatusCancelled}, nil
}

// ShortRandomSuffix deduplicates otherwise identical task IDs submitted
// back-to-back.
func ShortRandomSuffix() string {
	b := make([]byte, 2) // 4 hex chars
	if _, err := rand.Read(b); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b)
}
