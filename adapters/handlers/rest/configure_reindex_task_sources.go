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

type migrationTaskRaft interface {
	LocalDistributedTasks() map[string][]*distributedtask.Task
	FSMHasCaughtUp() bool
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

func newMigrationLocalTaskSource(raft migrationTaskRaft) db.MigrationLocalTaskSource {
	return func() ([]*distributedtask.Task, bool) {
		if !raft.FSMHasCaughtUp() {
			return nil, false
		}
		return raft.LocalDistributedTasks()[db.ReindexNamespace], true
	}
}

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
