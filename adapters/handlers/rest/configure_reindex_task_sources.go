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
// Naming it is what makes the sources testable: as a literal inside the
// post-bootstrap goroutine neither could be reached from a test.
type migrationTaskRaft interface {
	LocalDistributedTasks() map[string][]*distributedtask.Task
	FSMHasCaughtUp() bool
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

// newMigrationLocalTaskSource answers from this node's own applied FSM, so it
// costs no round-trip and cannot block a shard load.
//
// The gate covers the startup replay only: [Raft.FSMHasCaughtUp] compares
// against an index frozen when the store opened, so it goes true for good once
// the tail this node already held is applied. Before that the map is genuinely
// partial and a committed task reads as absent, which is what licenses
// reconciliation's discard. After it, what keeps a lagging map from licensing
// one is reconciliation itself: its load-time verdict withholds wherever it
// would rest on two absences at once, an absent task and an absent effect.
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
// when this node is the leader, with no barrier. The gate below only keeps the
// startup replay off the wire, and no predicate here can tell a freshly
// elected leader's map from a settled one. What answers that is the order
// reconciliation asks in — this node's own applied map first, where the task
// is bound to be, because the record only exists at all because a unit started
// from that map. Finding it there is positive evidence no snapshot age
// spoils.
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
