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
	"time"

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

// reindexGateMaxAge bounds how long a reindex submitted mid-backup goes unseen.
const reindexGateMaxAge = 5 * time.Second

// reindexGate reuses one task-list fetch, and its error, for every shard it
// checks until the fetch is older than maxAge, so a backup costs one leader
// round trip per maxAge rather than one per shard.
type reindexGate func(collection, shardName string) (bool, error)

func newReindexGate(db *DB, maxAge time.Duration) reindexGate {
	var (
		mu        sync.Mutex
		lookup    ShardReindexActivityLookup
		err       error
		fetchedAt time.Time
	)
	// Holding mu across the fetch makes concurrent shard workers share it.
	fetch := func() (ShardReindexActivityLookup, error) {
		mu.Lock()
		defer mu.Unlock()
		if fetchedAt.IsZero() || time.Since(fetchedAt) > maxAge {
			lookup, err = db.reindexActivityLookup()
			fetchedAt = time.Now()
		}
		return lookup, err
	}
	return func(collection, shardName string) (bool, error) {
		return db.anyLiveReindexForShard(collection, shardName, fetch)
	}
}

// reindexActivityLookup defaults to "no live reindex" when the lookup is
// unwired (with a one-time WARN). Refusing instead broke every module-test
// fixture that skips the post-bootstrap install; production HTTP gates on
// bootstrap completion, so external traffic cannot reach the unwired window.
func (db *DB) reindexActivityLookup() (ShardReindexActivityLookup, error) {
	db.reindexLookupsMu.RLock()
	activityBuilder := db.shardReindexActivityLookupBuilder
	db.reindexLookupsMu.RUnlock()
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
		return nil, nil
	}
	return activityBuilder()
}

// anyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)?
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
func (db *DB) anyLiveReindexForShard(collection, shardName string,
	fetch func() (ShardReindexActivityLookup, error),
) (bool, error) {
	if db.config.RuntimeReindexDisabled {
		// Runtime reindex is off, so no new task can start. Return before
		// consulting the lookup so the backup path makes no reindex check
		// at all — the pre-gate behavior this restores.
		return false, nil
	}
	lookup, err := fetch()
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
	db.reindexLookupsMu.RLock()
	cleanupBuilder := db.reindexCleanupInProgressLookupBldr
	db.reindexLookupsMu.RUnlock()
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
// [DB.anyLiveReindexForShard] to detect terminal-task cleanup that has
// not yet finished tearing __reindex / __ingest sidecar dirs. Wired in
// post-bootstrap alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexLookupsMu.Lock()
	defer db.reindexLookupsMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// ErrReindexGateUnavailable keeps a movement from waiting, so a task-manager outage shows as errors, not a stall.
var ErrReindexGateUnavailable = errors.New("cannot check for a running runtime-reindex task")

const noDatabaseBackReference = "this index has no database back-reference, so the check cannot run"

// refuseIfReindexInFlight is the per-shard backup-gate check used by
// [DB.Backupable], [Index.backupInactiveShardWithHardlinks],
// [Index.backupInactiveShardWithoutHardlinks], and [Shard.haltForTransfer].
//
// If i.db is nil it refuses the backup without calling gate.
func (i *Index) refuseIfReindexInFlight(shardName string, gate reindexGate) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexGateUnavailableError(collection, shardName, noDatabaseBackReference)
	}
	live, err := gate(collection, shardName)
	if err != nil {
		return reindexGateUnavailableError(collection, shardName, err.Error())
	}
	if !live {
		return nil
	}
	return reindexInFlightError(collection, shardName)
}

// reindexGateUnavailableError formats reason as text, not a wrapped error, so IsReversibleRefusal finds no FailedPrecondition status in it.
func reindexGateUnavailableError(collection, shardName, reason string) error {
	return fmt.Errorf("%w: shard %q (collection %q): %s; refusing in case one is running unseen (%w)",
		ErrReindexGateUnavailable, shardName, collection, reason,
		entitiesbackup.ErrBackupBlockedByInFlightReindex,
	)
}

// reindexInFlightError formats the operator-facing rejection.
//
// This gate never sees the task's status, so it states the cancel remedy
// with its condition attached rather than branching on it.
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
