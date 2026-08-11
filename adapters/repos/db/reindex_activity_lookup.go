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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// ShardReindexActivityLookup reports whether any LIVE reindex task in
// the DTM snapshot targets (collection, shardName). Used by the backup
// gate; consults RAFT-replicated DTM rather than local filesystem
// markers, so the answer is cluster-wide-consistent.
type ShardReindexActivityLookup func(collection, shardName string) bool

// ShardReindexActivityLookupBuilder returns a fresh snapshot. It takes the
// caller's context since building one queries the RAFT leader; a fixed
// wiring context would let an unanswering leader park the caller forever.
type ShardReindexActivityLookupBuilder func(ctx context.Context) ShardReindexActivityLookup

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([DB.AnyLiveReindexForShard]). The builder is invoked per backup
// precheck to obtain a fresh DTM snapshot.
//
// Calls before installation default to "no live reindex" and WARN,
// rate-limited to one line per hour so a shard-by-shard pass cannot flood
// the log. Refusing instead would block every module-test fixture that
// bypasses the bootstrap path. See [DB.AnyLiveReindexForShard].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}

// AnyReindexActivityLookup reports whether a runtime-reindex task on any of the
// given collections is live in the cluster. An empty list asks about every
// collection (the restore path's case when it doesn't yet know its class
// list). Scoped by collection: a migration can run for days, and a blind
// answer would refuse restores of every OTHER collection for that whole
// time. A live task whose payload names no collection still answers yes for
// every collection, since nothing says what it holds.
type AnyReindexActivityLookup func(ctx context.Context, collections []string) (bool, error)

// AnyCleanupInProgressLookup reports whether this node is still tearing reindex
// sidecar dirs for a task that has already reached a terminal status on any of
// the given collections. An empty list asks about every collection. Scoped by
// collection because the answer can be stuck (a worker that never exits holds
// its gate until the cap), so a blind answer would refuse every OTHER
// collection's restore for that whole time.
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

// redactedErr keeps its sentinels and cause reachable via errors.Is without
// printing them: the cause can name nodes and task internals that must not
// reach an API response body. More than one sentinel because a refusal can
// need classifying on two axes at once (what it refuses, and whether it
// observed it). A nil cause is allowed for a refusal with nothing to hide.
type redactedErr struct {
	msg       string
	sentinels []error
	cause     error
}

func (e redactedErr) Error() string { return e.msg }

func (e redactedErr) Unwrap() []error {
	if e.cause == nil {
		return e.sentinels
	}
	return append(append([]error{}, e.sentinels...), e.cause)
}

// RefuseIfAnyReindexInFlight is the restore-side counterpart of the per-shard
// backup gate. It asks the cluster rather than this node because a restoring
// class has no local index yet, so a per-SHARD lookup could never see a live
// task — but the question is still scoped to the collections being restored,
// which the task list can answer without any index (see
// [AnyReindexActivityLookup]). Fails closed on a live task or a lookup error;
// an unwired lookup allows the restore with a rate-limited WARN, matching
// [DB.AnyLiveReindexForShard].
func (db *DB) RefuseIfAnyReindexInFlight(ctx context.Context, collections []string) error {
	if db.config.RuntimeReindexDisabled {
		// Same contract as the backup half: off means no reindex check at all.
		return nil
	}

	db.reindexAuditMu.RLock()
	lookup := db.anyReindexActivityLookup
	cleanupLookup := db.anyCleanupInProgressLookup
	db.reindexAuditMu.RUnlock()

	// cleanupLookup returns bool, so it can't distinguish a cancelled task
	// still deleting sidecars from a submission sweep about to start; the
	// text below has to fit both.
	if cleanupLookup != nil && cleanupLookup(collections) {
		if db.logger != nil {
			db.logger.WithField("action", "restore_reindex_gate").
				Debug("restore-reindex gate: refusing — a teardown or a submission sweep is holding reindex sidecars on this node")
		}
		poll, cancel := reindexRemediationURLs(collections)
		return fmt.Errorf(
			"%w: a migration is holding temporary index files on this node: either a cancelled one still removing them, "+
				"or a newly submitted one preparing to run. Retry in a few seconds; if a new migration has started, "+
				"wait for it to finish (poll %s until all indexes report status=\"ready\") "+
				"or cancel it via %s {\"<indexType>\":{\"cancel\":true}}",
			entitiesbackup.ErrReindexInFlight, poll, cancel,
		)
	}

	if lookup == nil {
		warnUnwiredGate(db.gateSamplers().unwiredRestoreGate, "restore_reindex_gate",
			"restore-reindex gate: AnyReindexActivityLookup not yet installed; allowing restore. "+
				"Expected briefly during startup; if this persists past bootstrap, check the SetAnyReindexActivityLookup wiring in configure_api.go.")
		return nil
	}

	live, err := lookup(ctx, collections)
	if err != nil {
		// The RAFT error may name nodes; keep detail in the log, not the response.
		if db.logger != nil {
			db.logger.WithField("action", "restore_reindex_gate").
				Errorf("restore-reindex gate: cannot query the cluster task manager, assuming a migration is live: %v", err)
		}
		// "(assumed)" marks this as a fallback, not an observed live task.
		return redactedErr{
			msg: entitiesbackup.ErrReindexInFlight.Error() +
				" (assumed): the cluster task manager could not be queried; retry once it is reachable",
			sentinels: []error{entitiesbackup.ErrReindexInFlight},
			cause:     err,
		}
	}
	if !live {
		return nil
	}
	if db.logger != nil {
		db.logger.WithField("action", "restore_reindex_gate").
			Debug("restore-reindex gate: refusing — DTM lists a live runtime-reindex task in the cluster")
	}
	// Unconditional cancel advice, matching the backup gate's text in
	// [reindexInFlightError]: DTM refuses a cancel only for a task that already
	// reached a terminal status, and such a task is not live here.
	poll, cancel := reindexRemediationURLs(collections)
	return fmt.Errorf(
		"%w: retry after the migration finishes (poll %s until all indexes report status=\"ready\"), or lift this refusal now by cancelling it via %s {\"<indexType>\":{\"cancel\":true}}. Cancel is accepted at every stage of a migration, including while it is committing its result",
		entitiesbackup.ErrReindexInFlight, poll, cancel,
	)
}

// reindexRemediationURLs renders the poll and cancel routes the restore
// refusals point at. Only a single-collection restore can name the class: with
// none, or with several, the refusal cannot say which one holds the gate, so
// the class stays a placeholder the operator fills in.
func reindexRemediationURLs(collections []string) (poll, cancel string) {
	class := "<class>"
	if len(collections) == 1 {
		class = collections[0]
	}
	return fmt.Sprintf("GET /v1/schema/%s/indexes", class),
		fmt.Sprintf("PUT /v1/schema/%s/indexes/<prop>", class)
}

// ReindexOverlapLookup answers the backup's commit-time question: did any
// reindex task on these collections have a lifetime overlapping [since, now]?
//
// Overlap, not liveness: a migration that both starts and finishes while
// files are being copied leaves the capture just as inconsistent, but "is one
// running now" would answer no.
//
// A non-nil error means either that one overlapped or that the question can
// no longer be answered; callers fail closed on both, told apart by
// [entitiesbackup.ErrReindexOverlapUndetermined] so the refusal never claims
// a migration the check never saw.
//
// Cancelled tasks are decided by unit state, not timestamps: cancel only
// applies to an already-STARTED task, so a cancelled one may have already
// rebuilt buckets, and counts as an overlap whenever any unit left PENDING.
// Exempted when nothing was claimed, since the submit path's post-commit
// rollback manufactures exactly that state and counting it would fail the
// backup that won the race.
//
// The exemption leaves no residual. A worker's first action is a progress
// report of 0.0, and 0.0 still flips the unit out of PENDING even though it is
// not greater than the stored 0.0 — the status flip is unconditional, only the
// stored value is monotonic. That report is a synchronous leader apply, so
// every write a worker does is strictly downstream of a unit already reading
// IN_PROGRESS on the leader this lookup queries. Pinned by
// TestReindexOverlapLookupCountsCancelledTasksThatRan.
//
// since is compared across two machines' clocks: the backup stamps its start
// time on the capturing node, a task's FinishedAt on whichever node proposed
// it. Skewed far enough apart, a migration that did finish inside the window
// reads as having finished before it. Accepted, not worked around: backup state
// is not in RAFT, so there is no cluster-consistent ordering to compare
// against. See docs/runtime-reindex.md.
type ReindexOverlapLookup func(ctx context.Context, collections []string, since time.Time) error

// SetReindexOverlapLookup installs the lookup consulted by
// [DB.RefuseIfReindexOverlapped]. Wired from configure_api.go, which is where
// both the task list and the retention window are reachable.
func (db *DB) SetReindexOverlapLookup(lookup ReindexOverlapLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookup = lookup
}

// redactedOverlapErr builds the commit-time refusals that never saw an overlap
// and only failed to rule one out. It carries [ErrBackupSpannedReindex] so the
// refusal classifies like any other from this check, and
// [ErrReindexOverlapUndetermined] so the caller can tell the operator which of
// the two happened. The one refusal that did observe an overlap is built inline
// with [ErrBackupSpannedReindex] alone.
func redactedOverlapErr(msg string, cause error) error {
	return redactedErr{
		msg: msg,
		sentinels: []error{
			entitiesbackup.ErrBackupSpannedReindex,
			entitiesbackup.ErrReindexOverlapUndetermined,
		},
		cause: cause,
	}
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
				"cannot rule out a runtime-reindex during this backup: it ran longer than the %s the cluster keeps finished tasks for; "+
					"retry the backup, and raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS if backups here routinely take that long",
				completedTaskTTL), nil)
		}
		tasksByNamespace, err := list(ctx)
		if err != nil {
			// The RAFT error names the nodes it could not reach, and this text
			// is stored in the failure meta and served from the status API.
			return redactedOverlapErr(
				"cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried; "+
					"retry once it is reachable", err)
		}
		wanted := lowercasedSet(collections)
		for _, task := range tasksByNamespace[ReindexNamespace] {
			collection, overlaps, err := reindexTaskOverlaps(task, wanted, since)
			if err != nil {
				return err
			}
			if overlaps {
				return fmt.Errorf("%w: collection %q was migrated while this backup was being captured; "+
					"retry the backup once the migration finishes (poll GET /v1/schema/%s/indexes until every index reports status=\"ready\")",
					entitiesbackup.ErrBackupSpannedReindex, collection, collection)
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

// reindexTaskOverlaps applies the overlap rules to a single task: it overlaps
// when it targets one of the wanted collections and is either still running or
// reached a terminal status at or after since, equal timestamps counting as
// overlap. The returned collection name is the payload's, for the refusal
// message; it is only meaningful when overlaps is true.
//
// A non-nil error means the payload could not be read, so overlap can no
// longer be ruled out and the caller fails closed — but scoped by
// [DecodeReindexTaskPayload], which recovers the collection from payloads it
// cannot otherwise fully read. Without that scoping, one payload no node can
// decode (e.g. from a rolling upgrade that retyped a field) would refuse
// EVERY backup of EVERY collection until the completed-task TTL drops it.
// Only a payload naming no collection at all refuses everything.
//
// The "already over before the capture began" waiver applies to readable
// payloads only: nothing tore an unreadable one down, so its sidecar state
// may still be on disk regardless of what the timestamps say.
func reindexTaskOverlaps(task *distributedtask.Task, wanted map[string]struct{}, since time.Time) (string, bool, error) {
	_, collection, decodeErr := DecodeReindexTaskPayload(task.Payload)

	if !IsLiveReindexTaskStatus(task.Status) && decodeErr == nil {
		if task.Status == distributedtask.TaskStatusCancelled && !reindexTaskTouchedShards(task) {
			// A cancelled task that never claimed a unit wrote nothing, so it
			// cannot have spanned this backup. One is produced on purpose by
			// the submit path's post-commit rollback.
			return "", false, nil
		}
		if !task.FinishedAt.IsZero() && task.FinishedAt.Before(since) {
			return "", false, nil
		}
	}

	if collection == "" {
		// Nothing names what this task touched, and it could still have been
		// writing during the capture. Unbounded on purpose: declaring any
		// collection clean here would be a guess.
		return "", false, redactedOverlapErr(
			"cannot rule out a runtime-reindex during this backup: a task payload is unreadable; "+
				"retry once every node runs the same server version, and report this to Weaviate if it persists", decodeErr)
	}
	if _, ok := wanted[strings.ToLower(collection)]; !ok {
		return "", false, nil
	}
	if decodeErr != nil {
		// The collection is inside this backup and its task cannot be read, so
		// this backup fails — and only this one. The collection is named
		// because the caller already supplied it.
		return "", false, redactedOverlapErr(fmt.Sprintf(
			"cannot rule out a runtime-reindex of collection %q during this backup: its task payload is unreadable; retry once every node runs the same server version, and report this to Weaviate if it persists",
			collection), decodeErr)
	}
	return collection, true, nil
}

// reindexTaskTouchedShards reports whether any unit of the task ever left
// PENDING, i.e. whether a worker could have written to a shard. A CANCELLED
// task is not automatically harmless: cancel only applies to a STARTED task,
// which may already be rebuilding buckets. Unit state is what separates a
// genuinely untouched cancel from one that isn't.
func reindexTaskTouchedShards(task *distributedtask.Task) bool {
	if len(task.Units) == 0 {
		// No unit list at all is "unknown", not "untouched": every real task
		// carries its units from submission, so fail closed and clean up.
		return true
	}
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
		// Same contract as the other gates: the flag means no reindex check at
		// all. Residual: "no new task can start" isn't "no task is running" —
		// a reindex already in flight when the flag flipped is still live —
		// but the flag is the feature's escape hatch and must not itself fail
		// backups.
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
