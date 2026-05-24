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
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// ParsedReindexTask pairs a distributed task with its already-unmarshalled
// reindex payload. Callers build a slice of these once per request so
// MergeReindexStatus doesn't re-unmarshal task.Payload N times where N
// is the number of properties in the collection.
type ParsedReindexTask struct {
	Task    *distributedtask.Task
	Payload reindex.ReindexTaskPayload
}

// ParseReindexTasks unmarshals every reindex task's payload once. Tasks
// with unparseable payloads are skipped — those are flagged elsewhere
// by CheckReindexConflict at submit time; for the read-side merge
// they're the same as no task.
//
// FINISHED tasks are kept in the slice; MergeReindexStatus uses them
// to surface a brief "indexing@100%" SWAPPING-window entry while
// OnGroupCompleted's swap propagates to the schema.
func ParseReindexTasks(tasks []*distributedtask.Task) []ParsedReindexTask {
	parsed := make([]ParsedReindexTask, 0, len(tasks))
	for _, task := range tasks {
		var payload reindex.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		parsed = append(parsed, ParsedReindexTask{Task: task, Payload: payload})
	}
	return parsed
}

// MergeReindexStatus checks if there's an active or recently-terminated
// reindex task that targets the given property+indexType and updates
// the IndexStatus accordingly.
//
// Status values produced (in addition to the caller-supplied default
// "ready"):
//
//   - "pending":    STARTED task, no unit progress yet.
//   - "indexing":   STARTED task with some progress, OR a FINISHED
//     task whose swap hasn't propagated to the schema flag yet (the
//     brief OnGroupCompleted finalize window). `flagOn` distinguishes
//     the two: when the schema flag is already on, a stale FINISHED
//     task is ignored — the base "ready" wins.
//   - "failed":     latest matching task ended in FAILED.
//   - "cancelled":  latest matching task ended in CANCELLED.
//
// `finalizeWindow` caps the FINISHED-but-flag-off → indexing@100%
// override. Logger may be nil; the entry is still skipped, just
// without a log line.
func MergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, flagOn bool, parsedTasks []ParsedReindexTask, finalizeWindow time.Duration, logger logrus.FieldLogger) {
	var best *distributedtask.Task
	var bestPayload reindex.ReindexTaskPayload
	for _, pt := range parsedTasks {
		task := pt.Task
		payload := pt.Payload

		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		// Require a non-empty Properties list. REST always populates it
		// with one entry; an empty list only happens via direct cluster
		// payload authoring and is treated as "match nothing" so we
		// never silently fan out a synthetic entry to every property
		// in the collection.
		if !slices.Contains(payload.Properties, propName) {
			continue
		}

		targets, known := MigrationTypeTargetsIndex(payload.MigrationType, indexType)
		if !known && logger != nil {
			logger.WithFields(logrus.Fields{
				"migration_type": payload.MigrationType,
				"task_id":        task.ID,
				"collection":     collection,
			}).Errorf("reindex status: unknown migration type %q; index status may be stale", payload.MigrationType)
		}
		if !targets {
			continue
		}

		if best == nil || taskStatusPriority(task) > taskStatusPriority(best) ||
			(taskStatusPriority(task) == taskStatusPriority(best) && task.StartedAt.After(best.StartedAt)) {
			best = task
			bestPayload = payload
		}
	}

	if best == nil {
		return
	}

	// Decide the status first; only THEN apply per-migration-type side
	// effects (Tokenization / TargetTokenization / TargetAlgorithm).
	// Setting those fields ahead of the status decision caused the
	// "post-FINISHED targetAlgorithm bleed" bug: an unconditional
	// TargetAlgorithm assignment poisoned the response with an
	// in-flight signal that no longer applied once the schema flag
	// had caught up. The rule: side-effect fields surface only when
	// the status switch below changes idx.Status away from "ready".
	surfaceSyntheticFields := false

	switch best.Status {
	case distributedtask.TaskStatusFailed:
		idx.Status = "failed"
		idx.Progress = AggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusCancelled:
		idx.Status = "cancelled"
		idx.Progress = AggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusStarted:
		progress := AggregateProgress(best)
		idx.Progress = progress
		// Any non-PENDING unit means work has started somewhere; flip
		// the pill to "indexing" without waiting for the first throttled
		// progress checkpoint.
		if progress > 0 || AnyUnitWorking(best) {
			idx.Status = "indexing"
		} else {
			idx.Status = "pending"
		}
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusPreparing, distributedtask.TaskStatusSwapping:
		// Units done; cross-replica PREP barrier or per-node swap still
		// in flight. Surface as "indexing at 100%" until FINISHED + flagOn.
		idx.Status = "indexing"
		idx.Progress = 1.0
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusFinished:
		// The DTM declares FINISHED once every unit is terminal; for
		// semantic migrations the actual schema-flag flip happens
		// later inside OnGroupCompleted's swap phase. Without a
		// synthetic entry, that window — from "task FINISHED" to
		// "schema flag flipped on this node" — leaves the GET response
		// with no synthetic entry AND no base "ready" entry (flag
		// still off), so the UI sees an empty `indexes` array and
		// renders "None".
		//
		// Bound the window by task.FinishedAt: outside it,
		// flagOn==false cannot mean "swap pending" — the realistic
		// causes are a subsequent DELETE that flipped the flag back
		// off, or a silently-failed swap (logged loudly elsewhere).
		// Neither warrants a synthetic "indexing@100%" entry.
		if !flagOn && finalizeWindow > 0 && time.Since(best.FinishedAt) < finalizeWindow {
			idx.Status = "indexing"
			idx.Progress = 1.0
			surfaceSyntheticFields = true
		}
	}

	if !surfaceSyntheticFields {
		return
	}

	switch bestPayload.MigrationType {
	case reindex.ReindexTypeEnableSearchable:
		if bestPayload.TargetTokenization != "" {
			idx.Tokenization = bestPayload.TargetTokenization
		}
	case reindex.ReindexTypeChangeTokenization,
		reindex.ReindexTypeChangeTokenizationFilterable:
		if bestPayload.TargetTokenization != "" {
			idx.TargetTokenization = bestPayload.TargetTokenization
		}
	case reindex.ReindexTypeChangeAlgorithm:
		idx.TargetAlgorithm = models.IndexStatusTargetAlgorithmBlockmax
	case reindex.ReindexTypeRebuildSearchable,
		reindex.ReindexTypeRepairFilterable,
		reindex.ReindexTypeEnableFilterable, reindex.ReindexTypeEnableRangeable,
		reindex.ReindexTypeRepairRangeable:
		// No tokenization or algorithm side effects.
	}
}

// taskStatusPriority returns a priority for picking the most
// user-relevant task when more than one task matches a (collection,
// prop, indexType). In-flight beats terminal.
func taskStatusPriority(task *distributedtask.Task) int {
	switch task.Status {
	case distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping:
		return 2
	case distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
		distributedtask.TaskStatusFinished:
		return 1
	default:
		return 0
	}
}

// AggregateProgress averages Unit.Progress across all units in the
// task. Returns 0 when there are no units.
func AggregateProgress(task *distributedtask.Task) float32 {
	if len(task.Units) == 0 {
		return 0
	}
	var total float32
	for _, u := range task.Units {
		total += u.Progress
	}
	return total / float32(len(task.Units))
}

// AnyUnitWorking returns true if at least one unit has transitioned
// out of PENDING — i.e. some shard is actively iterating, has
// finished, or failed.
func AnyUnitWorking(task *distributedtask.Task) bool {
	for _, u := range task.Units {
		if u.Status != distributedtask.UnitStatusPending {
			return true
		}
	}
	return false
}

// IsSyntheticStatus reports whether the IndexStatus.Status value was
// emitted by MergeReindexStatus (i.e. driven by a reindex task) and so
// should be surfaced even when the property's schema flag for that
// index type is off. The default "ready" remains invisible when the
// flag is off, since it carries no actionable signal.
func IsSyntheticStatus(s string) bool {
	switch s {
	case models.IndexStatusStatusIndexing,
		models.IndexStatusStatusPending,
		models.IndexStatusStatusFailed,
		models.IndexStatusStatusCancelled:
		return true
	}
	return false
}
