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

// ShardReindexActivityLookupBuilder returns a fresh snapshot; building one
// costs a cluster-wide RAFT query, so [reindexGate] builds it once per
// backup, not once per shard. A non-nil error means the snapshot could not
// be taken, reported separately from a live task so the operator gets the
// right remediation.
type ShardReindexActivityLookupBuilder func() (ShardReindexActivityLookup, error)

// SetShardReindexActivityLookup installs the builder [reindexGate.refusalReason]
// consults once per backup, not once per shard.
//
// Before installation, calls default to "no live reindex" with a one-time
// WARN: configure_api.go wires this from a post-bootstrap goroutine that
// MakeAppState does not wait for, so a backup can land first. Fail-open, not
// refuse, because refusing broke module-test fixtures that bypass bootstrap.
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
