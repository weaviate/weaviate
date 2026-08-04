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

// ShardReindexActivityLookupBuilder returns a fresh snapshot, or an error
// if cluster-wide reindex state could not be read (RAFT leader
// unreachable). That case means "unknown for every shard," not "no
// reindex running," so the gate refuses the whole pass with one message.
type ShardReindexActivityLookupBuilder func() (ShardReindexActivityLookup, error)

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([reindexGate]), invoked once per backup pass for a
// fresh DTM snapshot.
//
// Before installation, calls default to "no live reindex" (one-time WARN).
// The builder is wired post-bootstrap, after the node reports ready, so an
// external backup request can land in the gap and be allowed without a
// gate check — the WARN is the only signal. The prior refuse-by-default
// broke every module-test fixture that bypasses the bootstrap path.
// See [reindexGate].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
