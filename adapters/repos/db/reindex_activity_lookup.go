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
// if cluster-wide reindex state could not be read at all (the RAFT leader
// was unreachable). The error case is not "no reindex is running" and it
// is not "a reindex is running on this shard" either — it is "unknown for
// every shard", and the gate refuses the whole pass with one message
// saying so. Reporting it as an error rather than as a lookup that
// answers true keeps that distinction, which the refusal text needs.
type ShardReindexActivityLookupBuilder func() (ShardReindexActivityLookup, error)

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([DB.AnyLiveReindexForShard]). The builder is invoked once per
// backup pass to obtain a fresh DTM snapshot.
//
// Calls before installation default to "no live reindex" with a one-time
// WARN. The builder is wired by configure_api.go's post-bootstrap
// goroutine, which runs after the node starts answering
// /v1/.well-known/ready, so a backup request can land before it is
// installed and be allowed without a gate check. The WARN is the only
// signal that this happened. The prior conservative-refuse default broke
// every module-test fixture that bypassed the bootstrap path.
// See [DB.AnyLiveReindexForShard].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
