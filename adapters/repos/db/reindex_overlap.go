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

package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

type ReindexOverlapVerdict struct {
	Spanned      bool
	Undetermined bool

	Collection string
	TaskID     string

	Detail string
}

func (v ReindexOverlapVerdict) clean() bool { return !v.Spanned && !v.Undetermined }

// A backup naming no class captured nothing and cannot have been spanned,
// the opposite of how the restore gate reads an empty list.
type ReindexOverlapLookup func(classes []string, since time.Time) ReindexOverlapVerdict

type ReindexOverlapLookupBuilder func(ctx context.Context) ReindexOverlapLookup

type ReindexWorkerLookup func(task distributedtask.TaskDescriptor) bool

// Overlap, not liveness: one that ran inside the window is already invisible.
func NewReindexOverlapLookup(
	tasks []*distributedtask.Task,
	completedTaskTTL time.Duration,
	hasLocalWorker ReindexWorkerLookup,
	now func() time.Time,
) ReindexOverlapLookup {
	decoded := decodeReindexTasksByID(tasks)

	return func(classes []string, since time.Time) ReindexOverlapVerdict {
		if len(classes) == 0 {
			return ReindexOverlapVerdict{}
		}
		if age := now().Sub(since); completedTaskTTL > 0 && age >= completedTaskTTL {
			return ReindexOverlapVerdict{
				Undetermined: true,
				Detail: fmt.Sprintf(
					"this backup ran for %s, longer than the %s a finished migration stays listed, "+
						"so a migration that spanned it may already have been dropped",
					age.Round(time.Second), completedTaskTTL),
			}
		}

		captured := make(map[string]struct{}, len(classes))
		for _, class := range classes {
			captured[strings.ToLower(class)] = struct{}{}
		}

		for _, task := range decoded {
			if task.Scope != ReindexPayloadScopeCluster {
				if _, ok := captured[strings.ToLower(task.Collection)]; !ok {
					continue
				}
			}
			if verdict := judgeReindexOverlap(task.task, since, hasLocalWorker); !verdict.clean() {
				verdict.Collection = task.Collection
				verdict.TaskID = task.task.ID
				return verdict
			}
		}
		return ReindexOverlapVerdict{}
	}
}

func judgeReindexOverlap(
	task *distributedtask.Task,
	since time.Time,
	hasLocalWorker ReindexWorkerLookup,
) ReindexOverlapVerdict {
	if IsLiveReindexTaskStatus(task.Status) {
		return ReindexOverlapVerdict{Spanned: true}
	}
	if task.FinishedAt.IsZero() {
		return ReindexOverlapVerdict{
			Undetermined: true,
			Detail:       "a migration reached a terminal status without recording when it finished",
		}
	}
	if task.FinishedAt.Before(since) {
		return ReindexOverlapVerdict{}
	}
	if task.Status != distributedtask.TaskStatusCancelled {
		return ReindexOverlapVerdict{Spanned: true}
	}
	return judgeCancelledReindexOverlap(task, hasLocalWorker)
}

// A cancel before any unit left PENDING wrote nothing, which is what a
// submission losing a race to a backup produces. Everything else refuses: an
// empty unit list is unknown, and a worker registers before a unit moves.
func judgeCancelledReindexOverlap(
	task *distributedtask.Task,
	hasLocalWorker ReindexWorkerLookup,
) ReindexOverlapVerdict {
	if len(task.Units) == 0 {
		return ReindexOverlapVerdict{
			Undetermined: true,
			Detail:       "a cancelled migration recorded no units, so nothing says whether it wrote",
		}
	}
	for _, unit := range task.Units {
		if unit == nil {
			return ReindexOverlapVerdict{
				Undetermined: true,
				Detail:       "a cancelled migration recorded a unit with no state",
			}
		}
		if unit.Status != distributedtask.UnitStatusPending {
			return ReindexOverlapVerdict{Spanned: true}
		}
	}
	if hasLocalWorker(task.TaskDescriptor) {
		return ReindexOverlapVerdict{Spanned: true}
	}
	return ReindexOverlapVerdict{}
}

func (db *DB) SetReindexOverlapLookup(builder ReindexOverlapLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookupBuilder = builder
}

// Known limitation: the capture start and FinishedAt are stamped by
// different nodes, so a proposer clock far enough behind reads a migration
// that finished inside the window as finishing before it. Closing this means
// putting backup state in RAFT.
func (db *DB) RefuseIfReindexOverlapped(ctx context.Context, classes []string, since time.Time) error {
	if db.config.RuntimeReindexDisabled {
		return nil
	}

	db.reindexAuditMu.RLock()
	builder := db.reindexOverlapLookupBuilder
	db.reindexAuditMu.RUnlock()
	if builder == nil {
		db.warnUnwiredGate(&overlapCheckWarnBudget, "backup_reindex_overlap", "commit-time overlap",
			"Check the SetReindexOverlapLookup wiring in configure_api.go.")
		return nil
	}
	verdict := builder(ctx)(classes, since)
	if verdict.clean() {
		return nil
	}
	db.warnOverlapRefusal(classes, verdict)
	return reindexOverlapRefusal(verdict)
}

func (db *DB) warnOverlapRefusal(classes []string, verdict ReindexOverlapVerdict) {
	reason := "overlap_observed"
	if verdict.Undetermined {
		reason = "overlap_undetermined"
	}
	db.warnRefusal("backup_reindex_overlap", reason,
		"commit-time overlap check: failing this backup; the published failure names a collection only",
		logrus.Fields{
			"task_id":              verdict.TaskID,
			"captured_class_count": len(classes),
			"captured_classes":     cappedSample(classes),
			"blocking_collection":  verdict.Collection,
		})
}

// One sentinel per arm, never both: matching the observed one on an
// undetermined answer reports a torn backup as a known one.
func reindexOverlapRefusal(verdict ReindexOverlapVerdict) error {
	if verdict.Undetermined {
		return fmt.Errorf("%w: %s",
			entitiesbackup.ErrReindexOverlapUndetermined, verdict.Detail)
	}
	named := "a collection this backup captured"
	if verdict.Collection != "" {
		named = fmt.Sprintf("collection %q", verdict.Collection)
	}
	return fmt.Errorf("%w: %s was migrated while this backup was being captured; "+
		"take a new backup once the migration has finished",
		entitiesbackup.ErrBackupSpannedReindex, named)
}
