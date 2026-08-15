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
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

type ReindexOverlapVerdict struct {
	Overlapped   bool
	Undetermined bool

	Collection string
	TaskID     string

	Detail string
}

func (v ReindexOverlapVerdict) allowsBackup() bool { return !v.Overlapped && !v.Undetermined }

// ReindexOverlapLookup reports whether a migration rewrote any of classes at
// or after since. An empty classes list captured nothing, so nothing overlaps it.
type ReindexOverlapLookup func(classes []string, since time.Time) ReindexOverlapVerdict

type ReindexOverlapLookupBuilder func(ctx context.Context) ReindexOverlapLookup

type ReindexWorkerLookup func(task distributedtask.TaskDescriptor) bool

// OverlapListRetryDelays: a failed check discards a whole finished upload.
var OverlapListRetryDelays = []time.Duration{
	time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second,
}

// ListReindexTasksForOverlap retries list on the delays schedule.
func ListReindexTasksForOverlap(
	ctx context.Context,
	list func(context.Context) (map[string][]*distributedtask.Task, error),
	delays []time.Duration,
) ([]*distributedtask.Task, error) {
	var err error
	for attempt := 0; ; attempt++ {
		var byNamespace map[string][]*distributedtask.Task
		if byNamespace, err = list(ctx); err == nil {
			return byNamespace[ReindexNamespace], nil
		}
		if attempt >= len(delays) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(delays[attempt]):
		}
	}
}

// overlapCandidate is a task the check has to judge. An empty collection is
// a payload that named none, which scopes the task to the whole cluster.
type overlapCandidate struct {
	task       *distributedtask.Task
	collection string
}

// The check reads the collection name and nothing else, so it decodes that
// field alone. Decoding the whole payload would materialize the unit and
// tenant lists — on a 100k-tenant collection, hundreds of thousands of
// strings per retained task, at the end of every backup on every node.
func reindexOverlapCandidates(tasks []*distributedtask.Task) []overlapCandidate {
	out := make([]overlapCandidate, 0, len(tasks))
	for _, task := range tasks {
		collection, _ := ExtractReindexTaskCollection(task.Payload)
		out = append(out, overlapCandidate{task: task, collection: collection})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].task.ID < out[j].task.ID })
	return out
}

// A migration can start and finish inside the capture window, so liveness
// answers nothing here; this asks whether one overlapped the capture.
func NewReindexOverlapLookup(
	tasks []*distributedtask.Task,
	completedTaskTTL time.Duration,
	hasLocalWorker ReindexWorkerLookup,
	now func() time.Time,
) ReindexOverlapLookup {
	candidates := reindexOverlapCandidates(tasks)

	return func(classes []string, since time.Time) ReindexOverlapVerdict {
		if len(classes) == 0 {
			return ReindexOverlapVerdict{}
		}
		captured := make(map[string]struct{}, len(classes))
		for _, class := range classes {
			captured[strings.ToLower(class)] = struct{}{}
		}

		var strongest ReindexOverlapVerdict
		for _, candidate := range candidates {
			if candidate.collection != "" {
				if _, ok := captured[strings.ToLower(candidate.collection)]; !ok {
					continue
				}
			}
			verdict := decideReindexOverlap(candidate.task, since, hasLocalWorker)
			if verdict.allowsBackup() {
				continue
			}
			verdict.Collection = candidate.collection
			verdict.TaskID = candidate.task.ID
			// Ties keep the earlier task, and the candidates are ordered by
			// task id, so every node names the same one.
			if overlapVerdictRank(verdict) > overlapVerdictRank(strongest) {
				strongest = verdict
			}
		}
		if !strongest.allowsBackup() {
			return strongest
		}

		// Last, so a task that answered outranks the guess this makes when
		// none did.
		if age := now().Sub(since); age >= completedTaskTTL {
			return ReindexOverlapVerdict{
				Undetermined: true,
				Detail: fmt.Sprintf(
					"this backup ran for %s, longer than the %s a finished migration stays listed, "+
						"so a migration that overlapped it may already have been dropped",
					age.Round(time.Second), completedTaskTTL),
			}
		}
		return ReindexOverlapVerdict{}
	}
}

// A weaker answer must not hide a stronger one: a scan that stopped at the
// first refusal could report "unknown" while a later task proved an overlap.
func overlapVerdictRank(v ReindexOverlapVerdict) int {
	switch {
	case v.Overlapped:
		return 2
	case v.Undetermined:
		return 1
	default:
		return 0
	}
}

func decideReindexOverlap(
	task *distributedtask.Task,
	since time.Time,
	hasLocalWorker ReindexWorkerLookup,
) ReindexOverlapVerdict {
	if IsLiveReindexTaskStatus(task.Status) {
		return ReindexOverlapVerdict{Overlapped: true}
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
		return ReindexOverlapVerdict{Overlapped: true}
	}
	return decideCancelledReindexOverlap(task, hasLocalWorker)
}

// Only a cancel that landed before any unit left PENDING wrote nothing, the
// state a submission that lost a race to a backup ends in. Anything else fails.
func decideCancelledReindexOverlap(
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
			return ReindexOverlapVerdict{Overlapped: true}
		}
	}
	if hasLocalWorker(task.TaskDescriptor) {
		return ReindexOverlapVerdict{Overlapped: true}
	}
	return ReindexOverlapVerdict{}
}

// refuseIfOverlapCheckCannotAnswer refuses at admission what the commit-time
// check would refuse after the whole upload. A zero TTL collects a finished
// migration on the next scheduler tick, so no capture can ever be cleared
// against one, and the backup id and the uploaded bytes are already spent by
// the time the check says so.
func (db *DB) refuseIfOverlapCheckCannotAnswer() error {
	if db.config.RuntimeReindexDisabled || db.config.CompletedTaskTTL > 0 {
		return nil
	}
	return blockedRefusal(
		"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0, so a finished runtime-reindex is dropped " +
			"from the cluster task list before a backup can be judged against it and every backup " +
			"would fail at the end of its upload; raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS " +
			"above the time a backup takes, or set RUNTIME_REINDEX_ENABLED=false to turn the " +
			"migration feature off")
}

func (db *DB) SetReindexOverlapLookup(builder ReindexOverlapLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookupBuilder = builder
}

// Known limitation: the capture start and FinishedAt come from different
// nodes' clocks, so enough skew hides a migration that finished inside the
// window. Closing this means putting backup state in RAFT.
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
	lookup := builder(ctx)
	if lookup == nil {
		db.warnUnwiredGate(&overlapCheckWarnBudget, "backup_reindex_overlap", "commit-time overlap",
			"Check the SetReindexOverlapLookup wiring in configure_api.go.")
		return nil
	}
	verdict := lookup(classes, since)
	if verdict.allowsBackup() {
		return nil
	}
	// An operator's cancel arrives on this ctx; that stays a cancel, not a refusal.
	if verdict.Undetermined && ctx.Err() != nil {
		return ctx.Err()
	}
	db.warnOverlapRefusal(classes, verdict)
	return reindexOverlapRefusal(verdict, classes)
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

// Two errors, never both: an undetermined answer must not match the observed one.
func reindexOverlapRefusal(verdict ReindexOverlapVerdict, classes []string) error {
	if verdict.Undetermined {
		return fmt.Errorf("%w: %s",
			entitiesbackup.ErrReindexOverlapUndetermined, verdict.Detail)
	}
	named, _ := overlapSubject(classes, verdict.Collection)
	return fmt.Errorf("%w: %s was migrated while this backup was being captured; "+
		"take a new backup once the migration has finished",
		entitiesbackup.ErrReindexOverlappedBackup, named)
}

// overlapSubject echoes the caller's own spelling of the captured class the
// verdict points at. Printing the task's spelling instead would let a name
// this backup never captured reach the API whenever the decoder attributes a
// task to something the caller did not ask about.
func overlapSubject(classes []string, blocking string) (string, bool) {
	for _, class := range classes {
		if strings.EqualFold(class, blocking) {
			return fmt.Sprintf("collection %q", class), true
		}
	}
	return "a collection this backup captured", false
}
