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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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
// WARN: production HTTP gates on bootstrap completion (the lookup is
// wired by configure_api.go's post-bootstrap goroutine), so an external
// backup request cannot land before this builder is installed. The WARN
// is the operator-facing signal if startup ordering ever breaks the
// wiring; the prior conservative-refuse default broke every module-test
// fixture that bypassed the bootstrap path. See [DB.AnyLiveReindexForShard].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}

// AnyReindexActivityLookup reports whether any runtime-reindex task is live in
// the cluster. A non-nil error means the answer could not be determined.
type AnyReindexActivityLookup func(ctx context.Context) (bool, error)

// AnyCleanupInProgressLookup reports whether this node is still tearing
// reindex sidecar dirs for a task that has already reached a terminal status.
type AnyCleanupInProgressLookup func() bool

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

// unwiredRestoreGateWarnOnce keeps the "lookup not installed" WARN to one
// line per process, matching [unwiredGateWarnOnce] on the backup side.
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

var unwiredRestoreGateWarnOnce sync.Once

// RefuseIfAnyReindexInFlight is the restore-side, cluster-wide counterpart of
// the per-shard backup gate: a restoring class has no local index yet, so a
// per-class lookup could never see a live task. Fails closed on a live task
// or a lookup error; an unwired lookup allows the restore once with a WARN,
// matching [DB.AnyLiveReindexForShard].
func (db *DB) RefuseIfAnyReindexInFlight(ctx context.Context) error {
	db.reindexAuditMu.RLock()
	lookup := db.anyReindexActivityLookup
	cleanupLookup := db.anyCleanupInProgressLookup
	db.reindexAuditMu.RUnlock()

	// A cancelled task leaves DTM immediately but keeps deleting sidecar dirs
	// for tens of seconds after — exactly the window the error text's retry advice lands in.
	if cleanupLookup != nil && cleanupLookup() {
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
		unwiredRestoreGateWarnOnce.Do(func() {
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
// It returns a non-nil error both when one did and when the question can no
// longer be answered, so the caller can fail closed on either.
type ReindexOverlapLookup func(ctx context.Context, collections []string, since time.Time) error

// SetReindexOverlapLookup installs the lookup consulted by
// [DB.RefuseIfReindexOverlapped]. Wired from configure_api.go, which is where
// both the task list and the retention window are reachable.
func (db *DB) SetReindexOverlapLookup(lookup ReindexOverlapLookup) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexOverlapLookup = lookup
}

// ReindexTaskLister lists DTM tasks by namespace. Narrowed from the cluster
// service so the overlap rules below can be exercised without a RAFT node.
type ReindexTaskLister func(ctx context.Context) (map[string][]*distributedtask.Task, error)

// NewReindexOverlapLookup builds the commit-time overlap check.
//
// A task overlaps [since, now] when it is still running, or when it reached a
// terminal status at or after the backup started. Equal timestamps count as
// overlap: the capture and the migration cannot be ordered, so the backup loses.
//
// A finished task leaves the list once completedTaskTTL elapses, and after that
// its absence proves nothing. Rather than read the empty list as "all clear",
// refuse outright once the backup has run longer than the retention window —
// the only case where this check could otherwise be silently wrong.
func NewReindexOverlapLookup(list ReindexTaskLister, completedTaskTTL time.Duration) ReindexOverlapLookup {
	return func(ctx context.Context, collections []string, since time.Time) error {
		if completedTaskTTL > 0 && time.Since(since) >= completedTaskTTL {
			return fmt.Errorf(
				"cannot rule out a runtime-reindex during this backup: it ran longer than the %s the cluster keeps finished tasks for",
				completedTaskTTL)
		}
		tasksByNamespace, err := list(ctx)
		if err != nil {
			return fmt.Errorf("cannot rule out a runtime-reindex during this backup: %w", err)
		}
		wanted := make(map[string]struct{}, len(collections))
		for _, c := range collections {
			wanted[strings.ToLower(c)] = struct{}{}
		}
		for _, task := range tasksByNamespace[ReindexNamespace] {
			var payload ReindexTaskPayload
			if err := json.Unmarshal(task.Payload, &payload); err != nil {
				return fmt.Errorf(
					"cannot rule out a runtime-reindex during this backup: a task payload is unreadable: %w", err)
			}
			if _, ok := wanted[strings.ToLower(payload.Collection)]; !ok {
				continue
			}
			if !IsLiveReindexTaskStatus(task.Status) && !task.FinishedAt.IsZero() &&
				task.FinishedAt.Before(since) {
				continue
			}
			return fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
				entitiesbackup.ErrBackupSpannedReindex, payload.Collection)
		}
		return nil
	}
}

var unwiredOverlapWarnOnce sync.Once

// RefuseIfReindexOverlapped is the backup's commit-time backstop. Asking
// whether a reindex is live right now misses the whole class of tasks that
// started and finished inside the backup window — the capture is just as
// inconsistent, and nothing is running by the time anyone looks.
//
// Unwired means fail-open with a one-time WARN, matching the other gates:
// module tests construct a DB without the post-bootstrap install path, and
// production gates external traffic on bootstrap completion.
func (db *DB) RefuseIfReindexOverlapped(ctx context.Context, collections []string, since time.Time) error {
	db.reindexAuditMu.RLock()
	lookup := db.reindexOverlapLookup
	db.reindexAuditMu.RUnlock()

	if lookup == nil {
		unwiredOverlapWarnOnce.Do(func() {
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
