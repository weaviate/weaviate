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

package rest

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// migrationTaskRaft is the slice of RAFT the two reindex task sources read.
// Named so a test can reach them; as a literal inside the post-bootstrap
// goroutine neither could be.
type migrationTaskRaft interface {
	LocalDistributedTasks() map[string][]*distributedtask.Task
	FSMHasCaughtUp() bool
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

// newMigrationLocalTaskSource answers from this node's own applied FSM, so it
// costs no round-trip and cannot block a shard load.
//
// The gate covers the startup replay only: FSMHasCaughtUp compares against an
// index frozen when the store opened, so it goes true for good once the tail
// this node already held is applied. Before that a committed task really does
// read as absent, which is what licenses reconciliation's discard. After it,
// what stops a lagging map licensing one is reconciliation's own load-time
// verdict, which does nothing when both the task and its effect are missing.
func newMigrationLocalTaskSource(raft migrationTaskRaft) db.MigrationLocalTaskSource {
	return func() ([]*distributedtask.Task, bool) {
		if !raft.FSMHasCaughtUp() {
			return nil, false
		}
		return raft.LocalDistributedTasks()[db.ReindexNamespace], true
	}
}

// newMigrationClusterTaskSource routes to the leader, whose list sees tasks
// this node has not applied yet. Read once per reconciliation pass, off the
// shard-load path.
//
// It is not a linearizable read: the query short-circuits into the local FSM
// when this node is the leader, with no barrier, and no predicate here can
// tell a freshly elected leader's map from a caught-up one. The gate below
// only keeps the startup replay off the wire. What covers the rest is that
// reconciliation asks this node's own applied map first, where the task is
// bound to be: the record exists only because a unit started from that map.
func newMigrationClusterTaskSource(raft migrationTaskRaft) db.MigrationClusterTaskSource {
	return func(ctx context.Context) ([]*distributedtask.Task, error) {
		if !raft.FSMHasCaughtUp() {
			return nil, fmt.Errorf("this node is still applying its RAFT log, so it cannot tell a task " +
				"it has not added yet from one the cluster removed")
		}
		tasksByNamespace, err := raft.ListDistributedTasks(ctx)
		if err != nil {
			return nil, err
		}
		return tasksByNamespace[db.ReindexNamespace], nil
	}
}
