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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

// ShardReindexActivityLookup reports whether any LIVE reindex task in
// the DTM snapshot targets (collection, shardName). Used by the backup
// gate; consults RAFT-replicated DTM rather than local filesystem
// markers, so the answer is cluster-wide-consistent.
type ShardReindexActivityLookup func(collection, shardName string) bool

// ShardReindexActivityLookupBuilder returns a fresh snapshot.
type ShardReindexActivityLookupBuilder func() ShardReindexActivityLookup

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([DB.AnyLiveReindexForShard]). The builder is invoked per backup
// precheck to obtain a fresh DTM snapshot.
//
// Calls before installation default to "no live reindex" with a one-time
// WARN: production HTTP gates on bootstrap completion (the lookup is
// wired by configure_api.go's post-bootstrap goroutine), so an external
// backup request cannot land before this builder is installed. The WARN
// is the operator-facing signal if startup ordering ever breaks the
// wiring. Refusing instead would block every module-test fixture that
// bypasses the bootstrap path. See [DB.AnyLiveReindexForShard].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}

// AnyReindexActivityLookup reports whether any runtime-reindex task is live in
// the cluster. A non-nil error means the answer could not be determined.
type AnyReindexActivityLookup func(ctx context.Context) (bool, error)

// AnyCleanupInProgressLookup reports whether this node is still tearing reindex
// sidecar dirs for a task that has already reached a terminal status on any of
// the given collections. An empty list asks about every collection, which is
// what the restore path has to do when it does not yet know its class list.
//
// It is scoped by collection because the answer can be stuck: a worker that
// never exits holds its collection's gate until the cap, and a blind answer
// would refuse restores of every OTHER collection for that whole time.
type AnyCleanupInProgressLookup func(collections []string) bool

// SetAnyCleanupInProgressLookup installs the node-local cleanup probe OR-ed
// into [DB.RefuseIfAnyReindexInFlight]. Sibling of
// [DB.SetReindexCleanupInProgressLookup], which serves the per-shard gate.
func (db *DB) SetAnyCleanupInProgressLookup(lookup AnyCleanupInProgressLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.anyCleanupInProgressLookup = lookup
}

// SetAnyReindexActivityLookup installs the cluster-wide predicate consulted by
// [DB.RefuseIfAnyReindexInFlight]. Wired post-bootstrap in configure_api.go
// alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetAnyReindexActivityLookup(lookup AnyReindexActivityLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.anyReindexActivityLookup = lookup
}

// redactedCauseErr keeps a cause reachable via errors.Is without printing it;
// the cause may name nodes that must not reach an API response body.
type redactedCauseErr struct {
	msg   string
	cause error
}

func (e redactedCauseErr) Error() string { return e.msg }

func (e redactedCauseErr) Unwrap() []error {
	return []error{entitiesbackup.ErrReindexInFlight, e.cause}
}

// unwiredRestoreGateWarnSampler rate-limits the "lookup not installed" WARN,
// matching [unwiredGateWarnSampler] on the backup side; see that variable for
// why the line has to keep reappearing rather than fire once per process.
var unwiredRestoreGateWarnSampler = logrusext.NewSampler(logrus.StandardLogger(), 1, time.Hour)

// RefuseIfAnyReindexInFlight is the restore-side, cluster-wide counterpart of
// the per-shard backup gate: a restoring class has no local index yet, so a
// per-class lookup could never see a live task. Fails closed on a live task
// or a lookup error; an unwired lookup allows the restore with a rate-limited
// WARN, matching [DB.AnyLiveReindexForShard].
func (db *DB) RefuseIfAnyReindexInFlight(ctx context.Context, collections []string) error {
	db.reindexAuditMu.RLock()
	lookup := db.anyReindexActivityLookup
	cleanupLookup := db.anyCleanupInProgressLookup
	db.reindexAuditMu.RUnlock()

	// A cancelled task leaves DTM immediately but keeps deleting sidecar dirs
	// for tens of seconds after — exactly the window the error text's retry advice lands in.
	if cleanupLookup != nil && cleanupLookup(collections) {
		if db.logger != nil {
			db.logger.WithField("action", "restore_reindex_gate").
				Debug("restore-reindex gate: refusing — a cancelled task is still removing reindex sidecars on this node")
		}
		return fmt.Errorf(
			"%w: a cancelled migration is still removing its temporary index files; retry in a few seconds",
			entitiesbackup.ErrReindexInFlight,
		)
	}

	if lookup == nil {
		unwiredRestoreGateWarnSampler.WithSampling(func(logrus.FieldLogger) {
			logger := db.logger
			if logger == nil {
				logger = logrus.New()
			}
			logger.WithField("action", "restore_reindex_gate").
				Warn("restore-reindex gate: AnyReindexActivityLookup not yet installed; allowing restore. " +
					"Expected briefly during startup; if this persists past bootstrap, check the SetAnyReindexActivityLookup wiring in configure_api.go.")
		})
		return nil
	}

	live, err := lookup(ctx)
	if err != nil {
		// The RAFT error may name nodes; restoring grants no access to node names,
		// so the detail stays in the log only.
		if db.logger != nil {
			db.logger.WithField("action", "restore_reindex_gate").
				Errorf("restore-reindex gate: cannot query the cluster task manager, assuming a migration is live: %v", err)
		}
		// "(assumed)" marks this as a fallback, not an observed live task.
		return redactedCauseErr{
			msg: entitiesbackup.ErrReindexInFlight.Error() +
				" (assumed): the cluster task manager could not be queried; retry once it is reachable",
			cause: err,
		}
	}
	if !live {
		return nil
	}
	if db.logger != nil {
		db.logger.WithField("action", "restore_reindex_gate").
			Debug("restore-reindex gate: refusing — DTM lists a live runtime-reindex task in the cluster")
	}
	return fmt.Errorf(
		"%w: retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
		entitiesbackup.ErrReindexInFlight,
	)
}

// ReindexOverlapLookup answers the backup's commit-time question: did any
// reindex task on these collections have a lifetime overlapping [since, now]?
//
// Overlap, not liveness. A migration that both starts and finishes while the
// files are being copied leaves the capture just as inconsistent, and asking
// whether one is running at commit time answers no — there is nothing left to
// see. Every caller of this lookup depends on that distinction.
//
// A non-nil error means either that one overlapped or that the question can no
// longer be answered; callers fail closed on both.
//
// A task cancelled before completing is not counted, whether it was withdrawn
// because a backup claimed first or cancelled by an operator part-way through:
// no cluster state tells the two apart, and neither is provably write-free
// (0-wi#473).
//
// The gap that leaves is the backup's whole duration, not a window at the end
// of it: a migration admitted through a fail-open route and cancelled before
// commit is invisible to every layer from shard-halt onwards, because the
// per-shard check runs once at halt and cannot see one that starts after it.
// Pinned by TestReindexOverlapLookupResidual.
type ReindexOverlapLookup func(ctx context.Context, collections []string, since time.Time) error

// SetReindexOverlapLookup installs the lookup consulted by
// [DB.RefuseIfReindexOverlapped]. Wired from configure_api.go, which is where
// both the task list and the retention window are reachable.
func (db *DB) SetReindexOverlapLookup(lookup ReindexOverlapLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookup = lookup
}

// redactedOverlapErr keeps a cause reachable for errors.Is without printing
// it. The causes here are RAFT and decoding errors that name nodes and task
// internals, and this text reaches an API response body.
type redactedOverlapErr struct {
	msg   string
	cause error
}

func (e redactedOverlapErr) Error() string { return e.msg }

// Unwrap yields the sentinel as well as the cause. Without it every caller
// classifying this refusal with errors.Is sees a plain error and treats a
// fail-closed overlap refusal as an unrelated failure. Matches redactedCauseErr.
func (e redactedOverlapErr) Unwrap() []error {
	return []error{entitiesbackup.ErrBackupSpannedReindex, e.cause}
}

// ReindexTaskLister lists DTM tasks by namespace, narrowed from the cluster
// service so the overlap rules can be exercised without a RAFT node.
type ReindexTaskLister func(ctx context.Context) (map[string][]*distributedtask.Task, error)

// NewReindexOverlapLookup builds the [ReindexOverlapLookup] rules: a task
// overlaps when it is still running or reached a terminal status at or after
// the backup started, equal timestamps counting as overlap.
//
// Past completedTaskTTL a finished task is dropped from the list, so its
// absence stops being evidence; the lookup refuses outright rather than read an
// empty list as all-clear.
func NewReindexOverlapLookup(list ReindexTaskLister, completedTaskTTL time.Duration) ReindexOverlapLookup {
	return func(ctx context.Context, collections []string, since time.Time) error {
		if completedTaskTTL > 0 && time.Since(since) >= completedTaskTTL {
			return fmt.Errorf(
				"cannot rule out a runtime-reindex during this backup: it ran longer than the %s the cluster keeps finished tasks for",
				completedTaskTTL)
		}
		tasksByNamespace, err := list(ctx)
		if err != nil {
			// The RAFT error names the nodes it could not reach, and this text
			// is stored in the failure meta and served from the status API.
			return redactedOverlapErr{
				msg:   "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried",
				cause: err,
			}
		}
		wanted := make(map[string]struct{}, len(collections))
		for _, c := range collections {
			wanted[strings.ToLower(c)] = struct{}{}
		}
		for _, task := range tasksByNamespace[ReindexNamespace] {
			var payload ReindexTaskPayload
			if err := json.Unmarshal(task.Payload, &payload); err != nil {
				return redactedOverlapErr{
					msg:   "cannot rule out a runtime-reindex during this backup: a task payload is unreadable",
					cause: err,
				}
			}
			if _, ok := wanted[strings.ToLower(payload.Collection)]; !ok {
				continue
			}
			if !IsLiveReindexTaskStatus(task.Status) {
				if task.Status == distributedtask.TaskStatusCancelled && !reindexTaskTouchedShards(task) {
					// A cancelled task that never claimed a unit wrote nothing,
					// so it cannot have spanned this backup. One is produced on
					// purpose by the submit path's post-commit rollback.
					continue
				}
				if !task.FinishedAt.IsZero() && task.FinishedAt.Before(since) {
					continue
				}
			}
			return fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
				entitiesbackup.ErrBackupSpannedReindex, payload.Collection)
		}
		return nil
	}
}

// reindexTaskTouchedShards reports whether any unit of the task ever left
// PENDING, i.e. whether a worker could have written to a shard.
//
// A CANCELLED task is not automatically harmless to a backup: cancel only
// applies to a STARTED task, and the workers may already have been rebuilding
// buckets when it landed. Skipping every cancelled task let the submit path's
// own rollback manufacture the one state this backstop ignores. Unit state is
// what separates the two: no unit out of PENDING means no worker claimed one.
func reindexTaskTouchedShards(task *distributedtask.Task) bool {
	for _, unit := range task.Units {
		if unit != nil && unit.Status != distributedtask.UnitStatusPending {
			return true
		}
	}
	return false
}

// See [unwiredGateWarnSampler] for the rate-limiting rationale.
var unwiredOverlapWarnSampler = logrusext.NewSampler(logrus.StandardLogger(), 1, time.Hour)

// RefuseIfReindexOverlapped is the backup's commit-time backstop; see
// [ReindexOverlapLookup] for why the question is overlap and not liveness.
//
// Unwired means fail-open with a rate-limited WARN, matching the other gates:
// module tests construct a DB without the post-bootstrap install path, and
// production gates external traffic on bootstrap completion.
func (db *DB) RefuseIfReindexOverlapped(ctx context.Context, collections []string, since time.Time) error {
	db.reindexAuditMu.RLock()
	lookup := db.reindexOverlapLookup
	db.reindexAuditMu.RUnlock()

	if lookup == nil {
		unwiredOverlapWarnSampler.WithSampling(func(logrus.FieldLogger) {
			logger := db.logger
			if logger == nil {
				logger = logrus.New()
			}
			logger.WithField("action", "backup_reindex_overlap").
				Warn("backup-reindex overlap check: lookup not yet installed; allowing the backup. " +
					"Expected briefly during startup; if this persists past bootstrap, check the " +
					"SetReindexOverlapLookup wiring in configure_api.go.")
		})
		return nil
	}
	return lookup(ctx, collections, since)
}
