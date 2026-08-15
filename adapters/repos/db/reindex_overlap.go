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

// ReindexOverlapVerdict is what the commit-time check concluded about one
// capture. Overlapped and Undetermined are mutually exclusive: the zero value
// allows the capture to be published, and either flag refuses it. TaskID is
// for the node log, never for the published message.
type ReindexOverlapVerdict struct {
	Overlapped   bool
	Undetermined bool

	Collection string
	TaskID     string

	Detail string
	Remedy string
}

func (v ReindexOverlapVerdict) allowsBackup() bool { return !v.Overlapped && !v.Undetermined }

// ReindexOverlapLookup reports whether a migration rewrote any of classes at
// or after since. An empty classes list captured nothing, so nothing overlaps it.
type ReindexOverlapLookup func(classes []string, since time.Time) ReindexOverlapVerdict

// ReindexOverlapLookupBuilder snapshots the cluster task list, which can
// block for the whole retry schedule below. Returning nil means allow.
type ReindexOverlapLookupBuilder func(ctx context.Context) ReindexOverlapLookup

// ReindexWorkerLookup answers for the local node only: a task running on a
// peer answers false here.
type ReindexWorkerLookup func(task distributedtask.TaskDescriptor) bool

// OverlapListRetryDelays is worth the wait: the alternative is discarding an
// upload that already finished.
var OverlapListRetryDelays = []time.Duration{
	time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second,
}

// ListReindexTasksForOverlap retries list on the delays schedule. A namespace
// absent from the answer yields no tasks, which the caller reads as allow. A
// cancelled context returns the last list error rather than the context error,
// so the caller still sees why the list failed.
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
// field alone. Decoding the whole payload would materialize every retained
// task's unit and tenant lists, at the end of every backup on every node.
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
// whether one is running: a migration that starts and finishes inside the
// capture window is absent from every liveness answer and still rewrote the
// captured files.
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
		// none did. A third clock: age is this node's, while the evidence is
		// dropped by whichever node's tick finds it expired, re-checked by the
		// applying node against a FinishedAt the recording node stamped.
		// Behind, this clears a capture whose evidence was already collected;
		// ahead, it burns a clean capture's id at 100%. Closing it needs a
		// reference time on the task list response.
		if age := now().Sub(since); age >= completedTaskTTL {
			return ReindexOverlapVerdict{
				Undetermined: true,
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
	// Before the liveness question, which answers "live" for a status it
	// cannot name and so would publish a migration that ended before this
	// capture began as an observed overlap, under a remedy that never comes.
	if !task.Status.IsRecognized() {
		return ReindexOverlapVerdict{
			Undetermined: true,
			Detail: "a migration is in a status this node cannot name, so nothing here says " +
				"whether it overlapped this backup",
			Remedy: "finish the rolling upgrade so every node knows that status, or wait for " +
				"the cluster task list to drop that record",
		}
	}
	if IsLiveReindexTaskStatus(task.Status) {
		return ReindexOverlapVerdict{Overlapped: true}
	}
	if task.FinishedAt.IsZero() {
		return ReindexOverlapVerdict{
			Undetermined: true,
			Detail:       "a migration reached a terminal status without recording when it finished",
			Remedy:       ReindexOverlapIncompleteRecordRemedy,
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

// The only route here that clears a capture, and it stands on two legs.
// No task worker wrote: PENDING is one-way, and the claim that leaves it
// precedes the shard lookup — see [ReindexProvider.processOneUnit] and
// [distributedtask.ThrottledRecorder] (a 0.0 claim is never throttled).
// hasLocalWorker covers only the window before that claim lands.
// The cleanup the cancellation itself triggers does write inside the capture
// window, unseen from here, and still cannot tear a staged capture:
// [Index.backupShardWithHardlinks] holds the shard's load mutex across
// list-then-link, staged files keep inodes of their own, and a capture of
// either side of that cleanup restores to the same data.
func decideCancelledReindexOverlap(
	task *distributedtask.Task,
	hasLocalWorker ReindexWorkerLookup,
) ReindexOverlapVerdict {
	if len(task.Units) == 0 {
		return ReindexOverlapVerdict{
			Undetermined: true,
			Detail:       "a cancelled migration recorded no units, so nothing says whether it wrote",
			Remedy:       ReindexOverlapIncompleteRecordRemedy,
		}
	}
	for _, unit := range task.Units {
		if unit == nil {
			return ReindexOverlapVerdict{
				Undetermined: true,
				Detail:       "a cancelled migration recorded a unit with no state",
				Remedy:       ReindexOverlapIncompleteRecordRemedy,
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
// against one, and by then the backup id and the uploaded bytes are spent.
func (db *DB) refuseIfOverlapCheckCannotAnswer() error {
	if db.config.RuntimeReindexDisabled || db.config.CompletedTaskTTL > 0 {
		return nil
	}
	// Exactly the condition under which the commit-time check runs at all: an
	// uninstalled one admits, so admitting here keeps the two the same answer.
	db.reindexAuditMu.RLock()
	wired := db.reindexOverlapLookupBuilder != nil
	db.reindexAuditMu.RUnlock()
	if !wired {
		return nil
	}
	// Its own sentinel and its own text: no migration is in flight here, and
	// the in-flight refusal would be rebuilt by the coordinator into a wait
	// for a migration that does not exist, without either variable name.
	return entitiesbackup.ReindexOverlapCheckError{Msg: fmt.Sprintf("%s: %s",
		entitiesbackup.ErrReindexOverlapCheckUnanswerable,
		"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0, so a finished runtime-reindex is dropped "+
			"from the cluster task list before a backup can be judged against it and every backup "+
			"would fail at the end of its upload; raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS "+
			"above the time a backup takes, or set RUNTIME_REINDEX_ENABLED=false to turn the "+
			"migration feature off")}
}

// SetReindexOverlapLookup installs the builder the commit-time check reads
// from. An uninstalled lookup admits, for the reason given on
// [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexOverlapLookup(builder ReindexOverlapLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookupBuilder = builder
}

// RefuseIfReindexOverlapped fails a finished capture whose window a migration
// overlapped; the uploader asks once per node at commit. Known limitation: the
// capture start and a task's FinishedAt come from different nodes' clocks, and
// enough skew either way hides an overlap or fails a capture nothing touched.
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
	// An operator's cancel arrives on this ctx; that stays a cancel, not a
	// refusal. A deadline is not a cancel, and publishing it would name
	// neither sentinel and give no remedy.
	if verdict.Undetermined && errors.Is(ctx.Err(), context.Canceled) {
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
		"commit-time overlap check: failing this backup",
		logrus.Fields{
			"task_id":              verdict.TaskID,
			"captured_class_count": len(classes),
			"captured_classes":     cappedSample(classes),
			"blocking_collection":  verdict.Collection,
		})
}

// ReindexOverlapIncompleteRecordRemedy is what an operator can do about a
// migration record too incomplete to judge a capture against: nothing, until
// the cluster task list drops it. Naming the wait beats naming no step.
const ReindexOverlapIncompleteRecordRemedy = "no capture can be judged against that record, so backups " +
	"stay refused until the cluster task list drops it, DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS " +
	"after the migration finished"

// Two errors, never both: an undetermined answer must not match the observed
// one. Both are worded alike, because an operator reading either needs the
// same things from it.
func reindexOverlapRefusal(verdict ReindexOverlapVerdict, classes []string) error {
	sentinel, finding, remedy := entitiesbackup.ErrReindexOverlapUndetermined, verdict.Detail, verdict.Remedy
	if !verdict.Undetermined {
		matched := matchCapturedClass(classes, verdict.Collection)
		sentinel = entitiesbackup.ErrReindexOverlappedBackup
		remedy = "wait for that migration to finish"
		if matched == "" {
			// Nothing observed puts the migration on a collection this backup
			// captured, only that the two cannot be separated. Same hedge the
			// restore gate makes on the same evidence.
			finding = "a runtime-reindex that cannot be attributed to a collection ran while this " +
				"backup was being captured, so this capture cannot be cleared"
		} else {
			finding = fmt.Sprintf("collection %q was migrated while this backup was being captured", matched)
			// The remedy renders the collection into URL paths.
			remedy += ". " + reindex.MigrationRemedy(matched)
		}
	}
	// The detail and remedy are spliced into the text the coordinator classifies
	// by substring, and a future one could quote a cancelled context.
	return fmt.Errorf("%w: %s. This backup id is spent, so a retry needs a new one: %s. "+
		"The partial upload under this id is not removed automatically and has to be deleted out of band",
		sentinel, entitiesbackup.CancelSafeText(finding), entitiesbackup.CancelSafeText(remedy))
}

// matchCapturedClass echoes the caller's own spelling of the captured class
// the verdict points at, and "" when it points at none. Printing the task's
// spelling instead would let a name this backup never captured reach the API.
func matchCapturedClass(classes []string, blocking string) string {
	for _, class := range classes {
		if strings.EqualFold(class, blocking) {
			return class
		}
	}
	return ""
}
