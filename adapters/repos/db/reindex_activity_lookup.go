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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
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
// WARN. The builder is wired by configure_api.go's post-bootstrap
// goroutine, and an external backup request can arrive before that runs,
// so the WARN names a backup that really was admitted without a check.
// Refusing instead would block every module-test fixture that bypasses
// the bootstrap path. See [DB.AnyLiveReindexForShard].
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

// redactedErr keeps a sentinel and a cause reachable via errors.Is without
// printing either. The cause names nodes and task internals that must not reach
// an API response body, and the sentinel's own text would otherwise be printed
// twice by %w wrapping. Without the sentinel in Unwrap, every caller
// classifying the refusal sees a plain error and treats a fail-closed gate
// refusal as an unrelated failure.
//
// A nil cause is allowed: the retention-window refusal has nothing to hide,
// only a message that must not be prefixed with the sentinel's text.
type redactedErr struct {
	msg      string
	sentinel error
	cause    error
}

func (e redactedErr) Error() string { return e.msg }

func (e redactedErr) Unwrap() []error {
	if e.cause == nil {
		return []error{e.sentinel}
	}
	return []error{e.sentinel, e.cause}
}

// RefuseIfAnyReindexInFlight is the restore-side, cluster-wide counterpart of
// the per-shard backup gate: a restoring class has no local index yet, so a
// per-class lookup could never see a live task. Fails closed on a live task
// or a lookup error; an unwired lookup allows the restore with a rate-limited
// WARN, matching [DB.AnyLiveReindexForShard].
func (db *DB) RefuseIfAnyReindexInFlight(ctx context.Context, collections []string) error {
	if db.config.RuntimeReindexDisabled {
		// Same contract the backup half honors: with RUNTIME_REINDEX_ENABLED
		// off there is no reindex check at all, so off means the behavior
		// operators had before the restore gate existed. Returns before the
		// lookup, which is a leader-forwarded RAFT query.
		return nil
	}

	db.reindexAuditMu.RLock()
	lookup := db.anyReindexActivityLookup
	cleanupLookup := db.anyCleanupInProgressLookup
	db.reindexAuditMu.RUnlock()

	// The lookup answers yes for two different node-local holds, and cannot say
	// which: a cancelled task still deleting sidecar dirs (tens of seconds), or
	// a submission sweep clearing the way for a migration that is about to
	// start. The text has to fit both, so it promises neither a short wait nor
	// a finished migration. The per-shard backup gate can name the case
	// (reindexBlockedBySubmit vs reindexBlockedByCleanup) because its lookup
	// returns a [ReindexHold]; this one returns a bool.
	if cleanupLookup != nil && cleanupLookup(collections) {
		if db.logger != nil {
			db.logger.WithField("action", "restore_reindex_gate").
				Debug("restore-reindex gate: refusing — a teardown or a submission sweep is holding reindex sidecars on this node")
		}
		return fmt.Errorf(
			"%w: a migration is holding temporary index files on this node: either a cancelled one still removing them, "+
				"or a newly submitted one preparing to run. Retry in a few seconds; if a new migration has started, "+
				"wait for it to finish (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") "+
				"or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
			entitiesbackup.ErrReindexInFlight,
		)
	}

	if lookup == nil {
		warnUnwiredGate(db.gateSamplers().unwiredRestoreGate, "restore_reindex_gate",
			"restore-reindex gate: AnyReindexActivityLookup not yet installed; allowing restore. "+
				"Expected briefly during startup; if this persists past bootstrap, check the SetAnyReindexActivityLookup wiring in configure_api.go.")
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
		return redactedErr{
			msg: entitiesbackup.ErrReindexInFlight.Error() +
				" (assumed): the cluster task manager could not be queried; retry once it is reachable",
			sentinel: entitiesbackup.ErrReindexInFlight,
			cause:    err,
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
// A task cancelled before completing is not counted: no cluster state separates
// one withdrawn because a backup claimed first from one an operator cancelled
// part-way through, and neither is provably write-free (0-wi#473). The gap that
// leaves is the backup's whole duration, since the per-shard check runs once at
// halt and cannot see a migration that starts after it. Pinned by
// TestReindexOverlapLookupResidual.
type ReindexOverlapLookup func(ctx context.Context, collections []string, since time.Time) error

// SetReindexOverlapLookup installs the lookup consulted by
// [DB.RefuseIfReindexOverlapped]. Wired from configure_api.go, which is where
// both the task list and the retention window are reachable.
func (db *DB) SetReindexOverlapLookup(lookup ReindexOverlapLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookup = lookup
}

// redactedOverlapErr is [redactedErr] fixed to the overlap sentinel; every
// refusal from the commit-time check carries it.
func redactedOverlapErr(msg string, cause error) error {
	return redactedErr{msg: msg, sentinel: entitiesbackup.ErrBackupSpannedReindex, cause: cause}
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
			return redactedOverlapErr(fmt.Sprintf(
				"cannot rule out a runtime-reindex during this backup: it ran longer than the %s the cluster keeps finished tasks for",
				completedTaskTTL), nil)
		}
		tasksByNamespace, err := list(ctx)
		if err != nil {
			// The RAFT error names the nodes it could not reach, and this text
			// is stored in the failure meta and served from the status API.
			return redactedOverlapErr(
				"cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried", err)
		}
		wanted := lowercasedSet(collections)
		for _, task := range tasksByNamespace[ReindexNamespace] {
			collection, overlaps, err := reindexTaskOverlaps(task, wanted, since)
			if err != nil {
				return err
			}
			if overlaps {
				return fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
					entitiesbackup.ErrBackupSpannedReindex, collection)
			}
		}
		return nil
	}
}

// lowercasedSet indexes the collection names for the case-insensitive match
// against task payloads.
func lowercasedSet(collections []string) map[string]struct{} {
	set := make(map[string]struct{}, len(collections))
	for _, c := range collections {
		set[strings.ToLower(c)] = struct{}{}
	}
	return set
}

// reindexOverlapClockSkewAllowance widens the "finished before the backup
// started" window because the two timestamps compared here are stamped on
// different machines: since is the backup participant's wall clock, while
// FinishedAt is the RAFT proposer's. Without an allowance, a proposer running
// behind the participant makes a migration that really did finish inside the
// backup window look like it finished before it, and the backup is published as
// clean while it is in fact torn.
//
// The two errors are not symmetric. Too small an allowance publishes a torn
// backup, which is silent and unrecoverable. Too large an allowance refuses a
// backup that was in fact clean, which the operator sees and can retry. We
// would rather refuse a clean backup than publish a torn one, so the allowance
// is generous relative to real drift: 30s is two orders of magnitude above the
// sub-second offset an NTP-synced cluster holds, while only extending the
// refusal window by 30s for migrations that finished just before the backup.
const reindexOverlapClockSkewAllowance = 30 * time.Second

// reindexTaskOverlaps applies the overlap rules to a single task: it overlaps
// when it targets one of the wanted collections and is either still running or
// reached a terminal status at or after since, equal timestamps counting as
// overlap. Only a finish more than [reindexOverlapClockSkewAllowance] before
// since clears the task, because the two timestamps come from different clocks.
// The returned collection name is the payload's, for the refusal message; it is
// only meaningful when overlaps is true.
//
// A non-nil error means the task payload could not be read, so overlap can no
// longer be ruled out; the caller fails closed on it.
func reindexTaskOverlaps(task *distributedtask.Task, wanted map[string]struct{}, since time.Time) (string, bool, error) {
	var payload ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return "", false, redactedOverlapErr(
			"cannot rule out a runtime-reindex during this backup: a task payload is unreadable", err)
	}
	if _, ok := wanted[strings.ToLower(payload.Collection)]; !ok {
		return "", false, nil
	}
	if IsLiveReindexTaskStatus(task.Status) {
		return payload.Collection, true, nil
	}
	if task.Status == distributedtask.TaskStatusCancelled && !reindexTaskTouchedShards(task) {
		// A cancelled task that never claimed a unit wrote nothing, so it
		// cannot have spanned this backup. One is produced on purpose by the
		// submit path's post-commit rollback.
		return "", false, nil
	}
	if !task.FinishedAt.IsZero() && task.FinishedAt.Before(since.Add(-reindexOverlapClockSkewAllowance)) {
		return "", false, nil
	}
	return payload.Collection, true, nil
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

// RefuseIfReindexOverlapped is the backup's commit-time backstop; see
// [ReindexOverlapLookup] for why the question is overlap and not liveness.
//
// Unwired means fail-open with a rate-limited WARN, matching the other gates:
// module tests construct a DB without the post-bootstrap install path, and a
// production request that arrives before that path runs is better admitted
// than refused. The WARN is what tells the operator it happened.
func (db *DB) RefuseIfReindexOverlapped(ctx context.Context, collections []string, since time.Time) error {
	if db.config.RuntimeReindexDisabled {
		// Same contract as the other two gates: RUNTIME_REINDEX_ENABLED=false
		// means no reindex check anywhere. Returning here also skips the lookup
		// itself, a leader-forwarded RAFT query that could fail a backup for
		// reasons needing no reindex to exist (an unreachable leader, or a
		// backup outliving the completed-task retention window).
		//
		// Residual, at its true width: "no new task can start" is not "no task
		// is running". A reindex is still live with the flag off if
		//
		//  1. it was already running when the flag was turned off;
		//  2. the node BOOTED with the flag off and resumed a STARTED task from
		//     DTM — the flag gates submission, not recovery;
		//  3. a cancel is tearing down, which stays allowed with the flag off.
		//
		// Accepted: the flag is the escape hatch for the whole feature, so it
		// cannot itself fail backups.
		return nil
	}

	db.reindexAuditMu.RLock()
	lookup := db.reindexOverlapLookup
	db.reindexAuditMu.RUnlock()

	if lookup == nil {
		warnUnwiredGate(db.gateSamplers().unwiredOverlap, "backup_reindex_overlap",
			"backup-reindex overlap check: lookup not yet installed; allowing the backup. "+
				"Expected briefly during startup; if this persists past bootstrap, check the "+
				"SetReindexOverlapLookup wiring in configure_api.go.")
		return nil
	}
	return lookup(ctx, collections, since)
}
