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
// Whether the answer is usable is measured, not asserted. A node still
// applying its RAFT tail — one restored from a leader-sent snapshot, or one
// with no local state of its own — holds a partial map in which a task the
// cluster committed reads as absent, and absent is what licenses
// reconciliation's discard.
func newMigrationLocalTaskSource(raft migrationTaskRaft) db.MigrationLocalTaskSource {
	return func() ([]*distributedtask.Task, bool) {
		if !raft.FSMHasCaughtUp() {
			return nil, false
		}
		return raft.LocalDistributedTasks()[db.ReindexNamespace], true
	}
}

// newMigrationClusterTaskSource routes to the leader, which is the only answer
// that cannot be behind. Reconciliation consults it where a local answer is
// about to be acted on destructively.
func newMigrationClusterTaskSource(raft migrationTaskRaft) db.MigrationClusterTaskSource {
	return func(ctx context.Context) ([]*distributedtask.Task, error) {
		tasksByNamespace, err := raft.ListDistributedTasks(ctx)
		if err != nil {
			return nil, err
		}
		return tasksByNamespace[db.ReindexNamespace], nil
	}
}
