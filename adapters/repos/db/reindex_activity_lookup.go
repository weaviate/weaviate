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

// ShardReindexActivityLookup reports whether any LIVE reindex task in
// the DTM snapshot targets (collection, shardName). Used by the backup
// gate; consults RAFT-replicated DTM rather than local filesystem
// markers, so the answer is cluster-wide-consistent.
type ShardReindexActivityLookup func(collection, shardName string) bool

// ShardReindexActivityLookupBuilder returns a fresh snapshot. Building one
// costs a cluster-wide ListDistributedTasks RAFT query, so [reindexGate]
// builds it once per backup, not once per shard.
//
// A non-nil error means the snapshot could not be taken. The gate reports that
// separately from "a reindex is running" so the operator gets the right
// remediation; both refuse the backup.
type ShardReindexActivityLookupBuilder func() (ShardReindexActivityLookup, error)

// SetShardReindexActivityLookup installs the builder used by the backup gate
// ([reindexGate.refusalReason]). It is invoked once per backup, not once per
// shard: once for the precheck, once for each index's transfer phase (cold and
// hot alike), and once per replica-snapshot movement in replica_snapshot.go and
// shard_replica_snapshot.go, which are not backups at all.
//
// Calls before installation default to "no live reindex" with a one-time WARN.
// The window is real: configure_api.go installs this from a goroutine that
// MakeAppState does not wait for, so a backup can land before it. Refusing by
// default would be the safer polarity but broke every module-test fixture that
// bypasses the bootstrap path, so the WARN is what surfaces it for now.
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
