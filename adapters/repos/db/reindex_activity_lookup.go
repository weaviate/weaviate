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
