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

// migrationTaskRaft is the RAFT slice the two reindex task sources below
// read; an interface so tests can substitute it.
type migrationTaskRaft interface {
	LocalDistributedTasks() map[string][]*distributedtask.Task
	FSMHasCaughtUp() bool
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

// newMigrationLocalTaskSource reads this node's own applied FSM (no
// round-trip, can't block a shard load). Gated on FSMHasCaughtUp: before
// catch-up, a committed-but-not-yet-applied task would read as absent, which
// reconciliation would wrongly treat as removed.
func newMigrationLocalTaskSource(raft migrationTaskRaft) db.MigrationLocalTaskSource {
	return func() ([]*distributedtask.Task, bool) {
		if !raft.FSMHasCaughtUp() {
			return nil, false
		}
		return raft.LocalDistributedTasks()[db.ReindexNamespace], true
	}
}

// newMigrationClusterTaskSource asks the leader for tasks this node hasn't
// applied yet. Not linearizable (a freshly elected leader may answer from a
// stale FSM); safe because reconciliation always checks this node's own
// applied map first, and a task's record only exists if that map created it.
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
