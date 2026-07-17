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
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
//
// Default to "no live reindex" when the lookup is unwired (with a
// one-time WARN). The original conservative default (refuse) was
// correct in isolation but broke every module-test fixture that
// spins up Weaviate without going through the post-bootstrap
// install path; production HTTP gates on bootstrap completion so the
// unwired window is unreachable by external traffic.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
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
		return false
	}
	lookup := activityBuilder()
	if lookup == nil {
		return false
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
		return true
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets. The cleanup builder is optional — older
	// wiring paths and test fixtures that install only the activity
	// lookup keep the prior semantics.
	if cleanupBuilder == nil {
		return false
	}
	cleanupLookup := cleanupBuilder()
	if cleanupLookup == nil {
		return false
	}
	if cleanupLookup(collection, shardName) {
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		}
		return true
	}
	return false
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

// refuseIfReindexInFlight is the per-shard backup-gate check used by
// [DB.Backupable], [Index.backupInactiveShardWithHardlinks],
// [Index.backupInactiveShardWithoutHardlinks], and
// [Shard.HaltForTransfer]. Consults DTM via
// [DB.AnyLiveReindexForShard]; the filesystem-marker variant it
// replaced only saw the local node and lagged DTM's actual state.
//
// If i.db is nil the gate is conservative: it refuses the backup, on
// the assumption that wiring is in progress.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(i.Config.ClassName.String(), shardName, true)
	}
	if !i.db.AnyLiveReindexForShard(i.Config.ClassName.String(), shardName) {
		return nil
	}
	return reindexInFlightError(i.Config.ClassName.String(), shardName, false)
}

// reindexInFlightError formats the operator-facing rejection. The
// `preWire` flag distinguishes "DTM lookup says live" from "lookup not
// yet installed" so the error body can hint at the right next step.
func reindexInFlightError(collection, shardName string, preWire bool) error {
	if preWire {
		return fmt.Errorf(
			"%w: shard %q (collection %q): backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	}
	return fmt.Errorf(
		"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via POST /v1/schema/<class>/properties/<prop>/index/<indexType>/cancel",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
	)
}

// NoSearchableIndexError formats the operator-facing 400 returned when a
// searchable-index operation (rebuild or algorithm change) targets a
// property that has no searchable index. Centralised so every call site
// emits identical phrasing.
//
// The inverse case ("already has a searchable index", emitted by
// enable-searchable validation) is deliberately not unified with this
// helper since it carries the opposite meaning.
func NoSearchableIndexError(propertyName string) string {
	return fmt.Sprintf(
		"property %q has no searchable index; PUT /v1/schema/{className}/properties/%s/index/searchable with a tokenization to add one first",
		propertyName, propertyName,
	)
}
