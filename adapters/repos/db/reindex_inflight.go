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
)

// ErrBackupBlockedByInFlightReindex is returned when a backup attempt
// races a runtime-reindex on the same shard. The DTM unit driving the
// migration is not part of the backup payload, so a captured tracker
// dir cannot be safely restored.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight on this shard")

// AnyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)?
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go; calls before installation are conservatively
// refused so a backup landing pre-wire does not race a real reindex.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	db.reindexAuditMu.RLock()
	builder := db.shardReindexActivityLookupBuilder
	db.reindexAuditMu.RUnlock()
	if builder == nil {
		// Wiring not complete (startup window). Conservative: assume a
		// reindex IS in flight so a backup landing pre-wire does not race
		// a real reindex.
		return true
	}
	lookup := builder()
	if lookup == nil {
		// Defensive: the builder returned nil. Treat the same as the
		// pre-wire window.
		return true
	}
	return lookup(collection, shardName)
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
			ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	}
	return fmt.Errorf(
		"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
		ErrBackupBlockedByInFlightReindex, shardName, collection,
	)
}
