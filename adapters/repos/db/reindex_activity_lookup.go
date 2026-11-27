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
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// ShardReindexActivityLookup reports whether any LIVE reindex task in
// the DTM snapshot targets (collection, shardName). Used by the backup
// gate; consults RAFT-replicated DTM rather than local filesystem
// markers, so the answer is cluster-wide-consistent.
type ShardReindexActivityLookup func(collection, shardName string) bool

// ShardReindexActivityLookupBuilder returns a fresh snapshot, or an error that refuses the backup or movement.
type ShardReindexActivityLookupBuilder func() (ShardReindexActivityLookup, error)

// NewShardReindexActivityLookup snapshots which shards a reindex is
// working on. A shard whose migration this build cannot prove is finished
// would otherwise be captured half-migrated.
// A task this node has not applied yet (hasUnit's known is false) covers every shard of its collection.
func NewShardReindexActivityLookup(tasks []*distributedtask.Task,
	hasUnit func(desc distributedtask.TaskDescriptor, shard string) (has, known bool), logger logrus.FieldLogger,
) ShardReindexActivityLookup {
	type liveTask struct {
		desc       distributedtask.TaskDescriptor
		collection string
	}
	var live []liveTask
	for _, task := range tasks {
		if !IsLiveReindexTaskStatus(task.Status) {
			continue
		}
		header, ok := ReindexHeaderOf(task)
		if !ok {
			logger.WithField("action", "backup_reindex_gate").
				WithField("task_id", task.ID).
				Warn("backup-reindex gate: task payload has no header; skipping task")
			continue
		}
		live = append(live, liveTask{desc: task.TaskDescriptor, collection: header.Collection()})
	}
	return func(collection, shardName string) bool {
		for _, task := range live {
			if task.collection != collection {
				continue
			}
			if has, known := hasUnit(task.desc, shardName); has || !known {
				return true
			}
		}
		return false
	}
}

// SetShardReindexActivityLookup installs the builder used by the backup
// gate ([Index.refuseIfReindexInFlight]). A gate calls it at most once per
// [reindexGateMaxAge], so one backup costs a bounded number of DTM fetches.
//
// Calls before installation default to "no live reindex" with a one-time
// WARN: production HTTP gates on bootstrap completion (the lookup is
// wired by configure_api.go's post-bootstrap goroutine), so an external
// backup request cannot land before this builder is installed. The WARN
// is the operator-facing signal if startup ordering ever breaks the
// wiring; the prior conservative-refuse default broke every module-test
// fixture that bypassed the bootstrap path. See [DB.reindexActivityLookup].
func (db *DB) SetShardReindexActivityLookup(builder ShardReindexActivityLookupBuilder) {
	db.reindexLookupsMu.Lock()
	defer db.reindexLookupsMu.Unlock()
	db.shardReindexActivityLookupBuilder = builder
}
