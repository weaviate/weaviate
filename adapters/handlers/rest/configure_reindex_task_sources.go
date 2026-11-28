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
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/types"
)

type migrationTaskRaft interface {
	LocalTaskStatuses(namespace, shard string) []distributedtask.TaskStatusEntry
	FSMHasCaughtUp() bool
	ListDistributedTaskHeaders(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

func newMigrationLocalTaskSource(raft migrationTaskRaft) db.MigrationLocalTaskSource {
	return func(shard string) ([]distributedtask.TaskStatusEntry, bool) {
		if !raft.FSMHasCaughtUp() {
			return nil, false
		}
		return raft.LocalTaskStatuses(db.ReindexNamespace, shard), true
	}
}

func newMigrationClusterTaskSource(raft migrationTaskRaft) db.MigrationClusterTaskSource {
	return func(ctx context.Context) ([]*distributedtask.Task, error) {
		if !raft.FSMHasCaughtUp() {
			return nil, fmt.Errorf("this node is still applying its RAFT log, so it cannot tell a task " +
				"it has not added yet from one the cluster removed")
		}
		tasksByNamespace, err := listTaskHeaders(ctx, raft)
		if err != nil {
			return nil, err
		}
		return tasksByNamespace[db.ReindexNamespace], nil
	}
}

const catchUpRetryBudget = 2 * time.Second

// catchUpRetryLister retries a new leader's refusal, which also starts the catch-up nothing else starts without writes.
type catchUpRetryLister struct {
	distributedtask.TaskHeaderLister
}

func (l catchUpRetryLister) ListDistributedTaskHeaders(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return listTaskHeaders(ctx, l.TaskHeaderLister)
}

func listTaskHeaders(ctx context.Context, lister distributedtask.TaskHeaderLister) (map[string][]*distributedtask.Task, error) {
	return listRetryingCatchUp(ctx, lister.ListDistributedTaskHeaders)
}

// listRetryingCatchUp must not run inside an FSM apply: its wait would stall the catch-up it waits for.
func listRetryingCatchUp(ctx context.Context,
	list func(context.Context) (map[string][]*distributedtask.Task, error),
) (map[string][]*distributedtask.Task, error) {
	deadline := time.Now().Add(catchUpRetryBudget)
	for backoff := 25 * time.Millisecond; ; backoff = min(2*backoff, 400*time.Millisecond) {
		tasks, err := list(ctx)
		if err == nil || !leaderNotCaughtUp(err) || time.Now().Add(backoff).After(deadline) {
			return tasks, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(backoff):
		}
	}
}

func leaderNotCaughtUp(err error) bool {
	return errors.Is(err, types.ErrFSMNotCaughtUp) ||
		(status.Code(err) == codes.Unavailable && strings.Contains(err.Error(), types.ErrFSMNotCaughtUp.Error()))
}
