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
	"encoding/json"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// ShardReindexActivityLookup reports whether any LIVE reindex task in
// the DTM snapshot targets (collection, shardName). Used by the backup
// gate; consults RAFT-replicated DTM rather than local filesystem
// markers, so the answer is cluster-wide-consistent.
//
// unreadable says the snapshot could not be taken at all, which is a different
// answer from "nothing is in flight" and must not be published as a named task.
type ShardReindexActivityLookup func(collection, shardName string) (live, unreadable bool)

// ShardReindexActivityLookupBuilder returns a fresh snapshot.
type ShardReindexActivityLookupBuilder func() ShardReindexActivityLookup

// NewShardReindexActivityLookup snapshots which shards a reindex is
// working on. A shard whose migration this build cannot prove is finished
// would otherwise be captured half-migrated.
func NewShardReindexActivityLookup(tasks []*distributedtask.Task, logger logrus.FieldLogger) ShardReindexActivityLookup {
	type shardKey struct {
		collection string
		shardName  string
	}
	live := make(map[shardKey]bool)
	for _, task := range tasks {
		if !IsLiveReindexTaskStatus(task.Status) {
			continue
		}
		var payload ReindexTaskPayload
		// Skipping admits the shard; the restore gate decodes the collection field alone and still refuses.
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			logger.WithField("action", "backup_reindex_gate").
				WithField("task_id", task.ID).
				Warnf("backup-reindex gate: cannot decode task payload; skipping task: %v", err)
			continue
		}
		for _, shardName := range payload.UnitToShard {
			live[shardKey{payload.Collection, shardName}] = true
		}
	}
	return func(collection, shardName string) (bool, bool) {
		return live[shardKey{collection, shardName}], false
	}
}

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([DB.AnyLiveReindexForShard]). The builder is invoked per backup
// precheck to obtain a fresh DTM snapshot.
//
// Calls before installation default to "no live reindex", warned at most once an hour
// per gate. Installed ahead of the internal cluster listener, so no peer's canCommit
// can land first; the default is left for fixtures that skip that wiring entirely.
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
