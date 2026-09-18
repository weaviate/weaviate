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
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// unwiredGateWarnOnce ensures the operator-facing WARN for the
// "lookup-not-installed" path fires at most once per process. The
// warning is informational: production gates HTTP serving on bootstrap
// completion, so under normal startup the unwired window is unreachable
// by an external backup request. If the WARN does fire in production
// logs, it means either (a) startup ordering is broken (lookup wiring
// never fires) or (b) a non-HTTP code path called Backupable before
// the lookup installed.
var unwiredGateWarnOnce sync.Once

// AnyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)?
//
// The lookup builder is installed by [DB.SetShardReindexActivityLookup]
// from the post-bootstrap goroutine in configure_api.go.
//
// Defaults to "no live reindex" when the lookup is unwired (with a
// one-time WARN): production gates HTTP serving on bootstrap
// completion, so no external request reaches the unwired window, and
// module-test fixtures skip the install path entirely. A builder that
// cannot reach DTM returns an error, which the caller refuses on.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) (bool, error) {
	if db.config.RuntimeReindexDisabled {
		// Runtime reindex is off, so no new task can start. Return before
		// consulting the lookup so the backup path makes no reindex check
		// at all — the pre-gate behavior this restores.
		return false, nil
	}
	db.reindexAuditMu.RLock()
	activityBuilder := db.shardReindexActivityLookupBuilder
	cleanupBuilder := db.reindexCleanupInProgressLookupBldr
	db.reindexAuditMu.RUnlock()
	if activityBuilder == nil {
		unwiredGateWarnOnce.Do(func() {
			logger := db.logger
			if logger == nil {
				logger = logrus.New()
			}
			logger.WithField("action", "backup_reindex_gate").
				Warn("backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. " +
					"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
		})
		return false, nil
	}
	lookup, err := activityBuilder()
	if err != nil {
		return false, err
	}
	if lookup == nil {
		return false, nil
	}
	if lookup(collection, shardName) {
		// Debug-level so flag-on operators get visibility into which
		// side of the OR fired the gate refusal. The matching cleanup
		// branch below logs at the same level.
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "activity_lookup_live_task").
				Debug("backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		}
		return true, nil
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets. The cleanup builder is optional — older
	// wiring paths and test fixtures that install only the activity
	// lookup keep the prior semantics.
	if cleanupBuilder == nil {
		return false, nil
	}
	cleanupLookup := cleanupBuilder()
	if cleanupLookup == nil {
		return false, nil
	}
	if cleanupLookup(collection, shardName) {
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		}
		return true, nil
	}
	return false, nil
}

// SetReindexCleanupInProgressLookup installs the builder used by
// [DB.AnyLiveReindexForShard] to detect terminal-task cleanup that has
// not yet finished tearing __reindex / __ingest sidecar dirs. Wired in
// post-bootstrap alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// ErrReindexGateUnavailable marks a refusal the gate issued without being able
// to check. Only a refusal naming a live task is something to wait for, so a
// replica movement counts this one against its error budget instead.
var ErrReindexGateUnavailable = errors.New("cannot check for a running runtime-reindex task")

var errGateNotWired = errors.New("the check is not installed yet (startup window); retry once the node has finished bootstrapping")

// refuseIfReindexInFlight is the per-shard backup-gate check used by
// [DB.Backupable], [Index.backupInactiveShardWithHardlinks],
// [Index.backupInactiveShardWithoutHardlinks], and
// [Shard.HaltForTransfer]. Consults DTM via [DB.AnyLiveReindexForShard],
// and refuses when it cannot check, so an unreachable DTM cannot let a
// backup race a reindex nobody can see.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		return reindexGateUnavailableError(collection, shardName, errGateNotWired)
	}
	live, err := i.db.AnyLiveReindexForShard(collection, shardName)
	if err != nil {
		return reindexGateUnavailableError(collection, shardName, err)
	}
	if !live {
		return nil
	}
	return reindexInFlightError(collection, shardName)
}

// reindexGateUnavailableError formats the refusal for a check that could not
// run. It wraps the in-flight sentinel too, so the backup path answers as it
// does for a live task.
func reindexGateUnavailableError(collection, shardName string, cause error) error {
	return fmt.Errorf("%w: shard %q (collection %q): %w; refusing in case one is running unseen (%w)",
		ErrReindexGateUnavailable, shardName, collection, cause,
		entitiesbackup.ErrBackupBlockedByInFlightReindex,
	)
}

// reindexInFlightError formats the operator-facing rejection for a task DTM
// reports as live. This gate never sees the task's status, so it states the
// cancel remedy with its condition attached rather than branching on it.
func reindexInFlightError(collection, shardName string) error {
	return fmt.Errorf(
		"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry once that task reaches a terminal state, which GET /v1/schema/<class>/indexes reports by moving the index off status=\"pending\" and status=\"indexing\". A cancel via POST /v1/schema/<class>/properties/<prop>/index/<indexType>/cancel is accepted only while the task is STARTED: it is refused with 409 in a coordination phase, and for a status this node cannot classify, which has to terminate on the nodes that do recognize it",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
	)
}

// NoSearchableIndexError formats the 400 for a searchable-index operation
// (rebuild/algorithm change) on a property with no searchable index.
// Centralised for identical phrasing across call sites; not used for the
// inverse case (already has one), which carries the opposite meaning.
func NoSearchableIndexError(propertyName string) string {
	return fmt.Sprintf(
		"property %q has no searchable index; PUT /v1/schema/{className}/properties/%s/index/searchable with a tokenization to add one first",
		propertyName, propertyName,
	)
}
