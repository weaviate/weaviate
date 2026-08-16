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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/reindex"
)

// ReindexOverlapOutcome is what the check concluded, weakest first: a scan keeps the
// strongest, and Undetermined outranks Ended because it may still be running.
type ReindexOverlapOutcome int

const (
	ReindexOverlapNone ReindexOverlapOutcome = iota
	ReindexOverlapEnded
	ReindexOverlapUndetermined
	ReindexOverlapLive
)

// ReindexOverlapVerdict carries an Outcome; TaskID is for the node log only.
type ReindexOverlapVerdict struct {
	Outcome ReindexOverlapOutcome

	Collection string
	TaskID     string

	Detail string
	Remedy string
}

func (v ReindexOverlapVerdict) allowsBackup() bool { return v.Outcome == ReindexOverlapNone }

// ReindexOverlapLookup judges one capture; an empty classes list overlaps nothing.
type ReindexOverlapLookup func(classes []string, since time.Time) ReindexOverlapVerdict

// ReindexOverlapLookupBuilder snapshots the task list; nil means allow.
type ReindexOverlapLookupBuilder func(ctx context.Context) ReindexOverlapLookup

// ReindexWorkerLookup answers for the local node only; lastExit is when its last worker stopped.
type ReindexWorkerLookup func(task distributedtask.TaskDescriptor) (running bool, lastExit time.Time)

// OverlapListRetryDelays is the retry schedule for the commit-time task list call.
var OverlapListRetryDelays = []time.Duration{
	time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second,
}

// OverlapListAttemptTimeout bounds one attempt, and must: the RPC waits for ready and
// the caller has no deadline. 10s matches RAFT_CONSISTENCY_WAIT_TIMEOUT; tighter is unsafe.
const OverlapListAttemptTimeout = 10 * time.Second

// ListReindexTasksForOverlap retries list on the delays schedule, each attempt
// bounded by attemptTimeout; a cancelled context still returns the list error.
func ListReindexTasksForOverlap(
	ctx context.Context,
	list func(context.Context) (map[string][]*distributedtask.Task, error),
	delays []time.Duration,
	attemptTimeout time.Duration,
) ([]*distributedtask.Task, error) {
	var err error
	for attempt := 0; ; attempt++ {
		var byNamespace map[string][]*distributedtask.Task
		// Bounded per attempt, never on ctx: that would cut off the retry schedule.
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		byNamespace, err = list(attemptCtx)
		cancel()
		if err == nil {
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

// An empty collection means the payload named none: the task is cluster-wide.
type overlapCandidate struct {
	task       *distributedtask.Task
	collection string
}

// Only the collection field: a full decode materializes every task's unit lists.
func reindexOverlapCandidates(tasks []*distributedtask.Task) []overlapCandidate {
	out := make([]overlapCandidate, 0, len(tasks))
	for _, task := range tasks {
		collection, _ := ExtractReindexTaskCollection(task.Payload)
		out = append(out, overlapCandidate{task: task, collection: collection})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].task.ID < out[j].task.ID })
	return out
}

// NewReindexOverlapLookup asks whether a migration overlapped a capture, not whether one runs.
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
			if verdict.Outcome > strongest.Outcome {
				strongest = verdict
			}
		}
		if !strongest.allowsBackup() {
			return strongest
		}

		// Last, so a task that answered outranks it; see docs/runtime-reindex.md 13.
		if age := now().Sub(since); age >= completedTaskTTL {
			return ReindexOverlapVerdict{
				Outcome: ReindexOverlapUndetermined,
				Detail: fmt.Sprintf(
					"this backup ran for %s and reached the %s window in which a finished migration "+
						"stays listed, so a migration that overlapped it may already have been dropped",
					age.Round(time.Second), completedTaskTTL),
				Remedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS above the time a backup takes",
			}
		}
		return ReindexOverlapVerdict{}
	}
}

func decideReindexOverlap(
	task *distributedtask.Task,
	since time.Time,
	hasLocalWorker ReindexWorkerLookup,
) ReindexOverlapVerdict {
	// Before the liveness question: a status this node cannot name would read as live.
	if !task.Status.IsRecognized() {
		return ReindexOverlapVerdict{
			Outcome: ReindexOverlapUndetermined,
			Detail: "a migration is in a status this node cannot name, so nothing here says " +
				"whether it overlapped this backup",
			Remedy: "finish the rolling upgrade so every node knows that status, or wait for " +
				"the cluster task list to drop that record",
		}
	}
	if IsLiveReindexTaskStatus(task.Status) {
		return ReindexOverlapVerdict{Outcome: ReindexOverlapLive}
	}
	// A terminal status is not a stopped worker: sibling units and cleanup run on.
	running, lastExit := hasLocalWorker(task.TaskDescriptor)
	if running {
		return ReindexOverlapVerdict{
			Outcome: ReindexOverlapLive,
			Remedy: "a node has not finished this migration's work yet, so wait for that " +
				"before retrying under a new backup id",
		}
	}
	// A worker that stopped inside the window wrote inside it, whatever FinishedAt says.
	if !lastExit.Before(since) {
		return ReindexOverlapVerdict{Outcome: ReindexOverlapEnded}
	}
	if !task.FinishedAt.After(time.UnixMilli(0)) {
		return ReindexOverlapVerdict{
			Outcome: ReindexOverlapUndetermined,
			Detail:  "a migration reached a terminal status without recording when it finished",
			Remedy:  ReindexOverlapIncompleteRecordRemedy,
		}
	}
	if task.FinishedAt.Before(since) {
		return ReindexOverlapVerdict{}
	}
	if task.Status != distributedtask.TaskStatusCancelled {
		return ReindexOverlapVerdict{Outcome: ReindexOverlapEnded}
	}
	return decideCancelledReindexOverlap(task)
}

// Clears a capture only when every unit is still PENDING: that status is one-way
// and the claim leaving it precedes any shard write, per [ReindexProvider.processOneUnit].
func decideCancelledReindexOverlap(task *distributedtask.Task) ReindexOverlapVerdict {
	if len(task.Units) == 0 {
		return ReindexOverlapVerdict{
			Outcome: ReindexOverlapUndetermined,
			Detail:  "a cancelled migration recorded no units, so nothing says whether it wrote",
			Remedy:  ReindexOverlapIncompleteRecordRemedy,
		}
	}
	for _, unit := range task.Units {
		// An unnameable unit status reads as a write, unlike the task-status rule.
		if unit.Status != distributedtask.UnitStatusPending {
			return ReindexOverlapVerdict{Outcome: ReindexOverlapEnded}
		}
	}
	return ReindexOverlapVerdict{}
}

func (db *DB) refuseIfOverlapCheckCannotAnswer() error {
	if db.config.RuntimeReindexDisabled || db.config.CompletedTaskTTL > 0 {
		return nil
	}
	// A builder can genuinely be unwired: it is installed after canCommit starts serving.
	db.reindexAuditMu.RLock()
	wired := db.reindexOverlapLookupBuilder != nil
	db.reindexAuditMu.RUnlock()
	if !wired {
		return nil
	}
	// Its own sentinel: the in-flight one promises a migration that does not exist.
	// RUNTIME_REINDEX_ENABLED=false lifts this; that flag is preview-only, gone at GA.
	return entitiesbackup.ReindexOverlapCheckError{Msg: fmt.Sprintf("%s: %s",
		entitiesbackup.ErrReindexOverlapCheckUnanswerable,
		"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0, so a finished runtime-reindex is dropped "+
			"from the cluster task list before a backup can be judged against it and every backup "+
			"would fail at the end of its upload; raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS "+
			"above the time a backup takes")}
}

// SetReindexOverlapLookup installs the builder the check reads; uninstalled admits.
func (db *DB) SetReindexOverlapLookup(builder ReindexOverlapLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookupBuilder = builder
}

// RefuseIfReindexOverlapped fails a capture a migration overlapped; asked once per commit.
func (db *DB) RefuseIfReindexOverlapped(ctx context.Context, classes []string, since time.Time) error {
	if db.config.RuntimeReindexDisabled {
		return nil
	}

	db.reindexAuditMu.RLock()
	builder := db.reindexOverlapLookupBuilder
	db.reindexAuditMu.RUnlock()
	var lookup ReindexOverlapLookup
	if builder != nil {
		lookup = builder(ctx)
	}
	if lookup == nil {
		db.warnUnwiredGate(&overlapCheckWarnBudget, "backup_reindex_overlap", "commit-time overlap",
			"Check the SetReindexOverlapLookup wiring in configure_api.go.")
		return nil
	}
	verdict := lookup(classes, since)
	if verdict.allowsBackup() {
		return nil
	}
	// A cancel stays a cancel unless an overlap was observed: a cancelled id can be re-posted.
	if verdict.Outcome == ReindexOverlapUndetermined && errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	db.warnOverlapRefusal(classes, verdict)
	return reindexOverlapRefusal(verdict, classes)
}

func (db *DB) warnOverlapRefusal(classes []string, verdict ReindexOverlapVerdict) {
	reason := reindexReasonOverlapObserved
	if verdict.Outcome == ReindexOverlapUndetermined {
		reason = reindexReasonOverlapUndetermined
	}
	db.warnRefusal("backup_reindex_overlap", reason,
		"commit-time overlap check: failing this backup",
		logrus.Fields{
			"task_id":              verdict.TaskID,
			"captured_class_count": len(classes),
			"captured_classes":     cappedSample(classes),
			"blocking_collection":  verdict.Collection,
		})
}

const ReindexOverlapIncompleteRecordRemedy = "no capture can be judged against that record, so backups " +
	"stay refused until the cluster task list drops it on a garbage-collection pass"

func reindexOverlapRefusal(verdict ReindexOverlapVerdict, classes []string) error {
	sentinel, finding, remedy := entitiesbackup.ErrReindexOverlapUndetermined, verdict.Detail, verdict.Remedy
	if verdict.Outcome != ReindexOverlapUndetermined {
		matched := matchCapturedClass(classes, verdict.Collection)
		sentinel = entitiesbackup.ErrReindexOverlappedBackup
		if matched == "" {
			finding = "a runtime-reindex that cannot be attributed to a collection ran while this " +
				"backup was being captured, so this capture cannot be cleared"
		} else {
			finding = fmt.Sprintf("collection %q was migrated while this backup was being captured", matched)
		}
		if remedy == "" {
			remedy = "that migration is already over, so a retry under a new backup id is not blocked by it"
			if verdict.Outcome == ReindexOverlapLive {
				remedy = "wait for that migration to finish"
				if matched != "" {
					remedy += ". " + reindex.MigrationRemedy(matched)
				}
			}
		}
	}
	// Spliced into text the coordinator classifies by substring; a cancel would lie.
	return fmt.Errorf("%w: %s. This backup id is spent, so a retry needs a new one: %s. "+
		"The partial upload under this id is not removed automatically and has to be deleted out of band",
		sentinel, entitiesbackup.CancelSafeText(finding), entitiesbackup.CancelSafeText(remedy))
}

// matchCapturedClass echoes the caller's spelling, "" for none; the task's could leak.
func matchCapturedClass(classes []string, blocking string) string {
	for _, class := range classes {
		if strings.EqualFold(class, blocking) {
			return class
		}
	}
	return ""
}
