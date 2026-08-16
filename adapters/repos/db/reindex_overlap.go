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

// ReindexOverlapOutcome is what the check concluded about one capture, weakest
// first: a scan keeps the strongest, so a weaker answer never hides an overlap.
// An answer nobody could give outranks a finished one: the migration it could
// not judge may still be running. Only the zero value publishes; a live overlap
// earns a wait, an ended one an id.
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
	// A verdict that sets Remedy keeps it; the refusal composes one otherwise.
	Remedy string
}

func (v ReindexOverlapVerdict) allowsBackup() bool { return v.Outcome == ReindexOverlapNone }

// ReindexOverlapLookup reports whether a migration rewrote any of classes at
// or after since. An empty classes list captured nothing, so nothing overlaps it.
type ReindexOverlapLookup func(classes []string, since time.Time) ReindexOverlapVerdict

// ReindexOverlapLookupBuilder snapshots the task list; nil means allow.
type ReindexOverlapLookupBuilder func(ctx context.Context) ReindexOverlapLookup

// ReindexWorkerLookup answers for the local node only; a peer's task is false
// and a zero time. lastExit is when this node's last worker for the task
// stopped - what the task's own record cannot say.
type ReindexWorkerLookup func(task distributedtask.TaskDescriptor) (running bool, lastExit time.Time)

// OverlapListRetryDelays beats discarding an upload that already finished.
var OverlapListRetryDelays = []time.Duration{
	time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second,
}

// ListReindexTasksForOverlap retries list on the delays schedule; an absent
// namespace yields no tasks, which the caller reads as allow. Even a cancelled
// context returns the list error, so its cause stays visible.
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

// An empty collection is a payload that named none: cluster-wide.
type overlapCandidate struct {
	task       *distributedtask.Task
	collection string
}

// Decodes the collection field alone: the whole payload would materialize every
// retained task's unit and tenant lists, at the end of every backup on every node.
func reindexOverlapCandidates(tasks []*distributedtask.Task) []overlapCandidate {
	out := make([]overlapCandidate, 0, len(tasks))
	for _, task := range tasks {
		collection, _ := ExtractReindexTaskCollection(task.Payload)
		out = append(out, overlapCandidate{task: task, collection: collection})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].task.ID < out[j].task.ID })
	return out
}

// NewReindexOverlapLookup asks whether a migration overlapped a capture, not
// whether one runs: one contained in the window answers no liveness question.
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
			// Ties keep the lowest task id, so every node names the same one.
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
	// Before the liveness question, which reads a status it cannot name as live
	// and would publish a migration that ended long ago as an observed overlap.
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
	// A terminal status is not a stopped worker: the unit failure that fails the
	// whole task stamps FinishedAt, and the units still running are never waited
	// for. A cancel leaves its file cleanup running the same way.
	running, lastExit := hasLocalWorker(task.TaskDescriptor)
	if running {
		return ReindexOverlapVerdict{
			Outcome: ReindexOverlapLive,
			Remedy: "a node has not finished this migration's work yet, so wait for that " +
				"before retrying under a new backup id",
		}
	}
	// The window this check owns ends at commit, not where it runs: a worker
	// that stopped inside it wrote inside it, whatever FinishedAt below says.
	if !lastExit.Before(since) {
		return ReindexOverlapVerdict{Outcome: ReindexOverlapEnded}
	}
	if task.FinishedAt.IsZero() {
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

// The only route here that clears a capture: every unit still PENDING, which is
// one-way, and the claim that leaves it precedes the shard lookup — see
// [ReindexProvider.processOneUnit]. The caller ruled out a still-running worker.
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

// refuseIfOverlapCheckCannotAnswer refuses at admission what the commit-time
// check would refuse after the upload: a zero TTL clears nothing, ever.
func (db *DB) refuseIfOverlapCheckCannotAnswer() error {
	if db.config.RuntimeReindexDisabled || db.config.CompletedTaskTTL > 0 {
		return nil
	}
	// An unwired builder is a real state, not just a fixture one:
	// installReindexGateLookups runs well after the cluster listener starts
	// serving canCommit (see its godoc). Admitting there is what the siblings do.
	db.reindexAuditMu.RLock()
	wired := db.reindexOverlapLookupBuilder != nil
	db.reindexAuditMu.RUnlock()
	if !wired {
		return nil
	}
	// Its own sentinel: nothing is in flight, and the in-flight one would be rebuilt
	// into a wait for a migration that does not exist. RUNTIME_REINDEX_ENABLED=false
	// is no way out either: it gates submits, not migrations already running.
	return entitiesbackup.ReindexOverlapCheckError{Msg: fmt.Sprintf("%s: %s",
		entitiesbackup.ErrReindexOverlapCheckUnanswerable,
		"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0, so a finished runtime-reindex is dropped "+
			"from the cluster task list before a backup can be judged against it and every backup "+
			"would fail at the end of its upload; raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS "+
			"above the time a backup takes")}
}

// SetReindexOverlapLookup installs the builder the commit-time check reads from.
// An uninstalled lookup admits, per [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexOverlapLookup(builder ReindexOverlapLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookupBuilder = builder
}

// RefuseIfReindexOverlapped fails a finished capture whose window a migration
// overlapped; the uploader asks once per node at commit.
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
	// An operator's cancel arrives on this ctx and stays a cancel, unless an overlap
	// was observed: that capture may be torn, and a cancelled id can be re-posted.
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

// ReindexOverlapIncompleteRecordRemedy: wait, the list will drop the record.
const ReindexOverlapIncompleteRecordRemedy = "no capture can be judged against that record, so backups " +
	"stay refused until the cluster task list drops it on a garbage-collection pass"

// Two errors, never both, worded alike: either reader needs the same things.
func reindexOverlapRefusal(verdict ReindexOverlapVerdict, classes []string) error {
	sentinel, finding, remedy := entitiesbackup.ErrReindexOverlapUndetermined, verdict.Detail, verdict.Remedy
	if verdict.Outcome != ReindexOverlapUndetermined {
		matched := matchCapturedClass(classes, verdict.Collection)
		sentinel = entitiesbackup.ErrReindexOverlappedBackup
		if matched == "" {
			// Not attributed to a captured collection, only inseparable from it.
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
					// Every step it names acts on a task that has not ended.
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

// matchCapturedClass echoes the caller's spelling of the class the verdict points
// at, "" for none. The task's would let an uncaptured name reach the API.
func matchCapturedClass(classes []string, blocking string) string {
	for _, class := range classes {
		if strings.EqualFold(class, blocking) {
			return class
		}
	}
	return ""
}
