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
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// The scheduler reads this node's own task copy, which can trail the leader.
// Pins that a trailing copy cannot revert a newer run's schema flip: the node
// still sees an older run at SWAPPING after a newer run on the same property
// finished, fires the older run's flip, and the flip check refuses it.
func TestReindexStaleTaskCopyCannotRevertANewerFlip(t *testing.T) {
	logger, _ := test.NewNullLogger()
	leader := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  logger,
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
	})
	node := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logger})
	schema := &flipTarget{tokenization: "word"}

	req := func(sub any) *api.ApplyRequest {
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		return &api.ApplyRequest{SubCommand: raw}
	}
	run := func(id string, version uint64, target string) []func(m *distributedtask.Manager) {
		payload, err := json.Marshal(ReindexTaskPayload{
			Collection: "C", MigrationType: ReindexTypeChangeTokenization,
			Properties: []string{"name"}, TargetTokenization: target,
		})
		require.NoError(t, err)
		return []func(m *distributedtask.Manager){
			func(m *distributedtask.Manager) {
				require.NoError(t, m.AddTask(req(&api.AddDistributedTaskRequest{
					Namespace: ReindexNamespace, Id: id, Payload: payload,
					SubmittedAtUnixMillis: time.Now().UnixMilli(), UnitIds: []string{"u"},
				}), version))
			},
			func(m *distributedtask.Manager) { // the last unit finishes: SWAPPING
				require.NoError(t, m.RecordUnitCompletion(req(&api.RecordDistributedTaskUnitCompletionRequest{
					Namespace: ReindexNamespace, Id: id, Version: version, NodeId: "node-b", UnitId: "u",
					FinishedAtUnixMillis: time.Now().UnixMilli(),
				})))
			},
			func(m *distributedtask.Manager) { // another node's flip, admitted while SWAPPING
				if m == leader {
					require.NoError(t, schema.flip(leader, api.MigrationRun{Namespace: ReindexNamespace, ID: id, Version: version}, target))
				}
			},
			func(m *distributedtask.Manager) {
				require.NoError(t, m.MarkTaskFinalized(req(&api.MarkTaskFinalizedRequest{
					Namespace: ReindexNamespace, Id: id, Version: version, FinalizedAtUnixMillis: time.Now().UnixMilli(),
				})))
			},
		}
	}

	// The node has applied only the older run's first two entries.
	const nodeApplied = 2
	for i, entry := range append(run("t1", 1, "field"), run("t2", 5, "whitespace")...) {
		entry(leader)
		if i < nodeApplied {
			entry(node)
		}
	}
	require.Equal(t, "whitespace", schema.get())

	provider := &staleCopyFlipProvider{leader: leader, schema: schema}
	sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
		TaskLister:    node,
		TaskFinalizer: leaderFinalizer{leader: leader},
		Providers:     map[string]distributedtask.Provider{ReindexNamespace: provider},
		Logger:        logger,
		LocalNode:     "node-a",
		TickInterval:  20 * time.Millisecond,
	})
	require.NoError(t, sched.Start(context.Background()))

	require.Eventually(t, func() bool { return provider.flipsFired() > 0 }, 5*time.Second, 10*time.Millisecond,
		"the node must fire the older run's flip off its trailing copy")
	sched.Close()
	require.Equal(t, "whitespace", schema.get(), "the older run's late flip reverted the newer one")
}

type flipTarget struct {
	mu           sync.Mutex
	tokenization string
}

// flip makes the FSM's check for a migration's flip, then writes it.
func (f *flipTarget) flip(leader *distributedtask.Manager, run api.MigrationRun, target string) error {
	if err := leader.CheckMigrationPropertyUpdate("C", "name", run); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokenization = target
	return nil
}

func (f *flipTarget) get() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokenization
}

// staleCopyFlipProvider flips the schema from OnTaskCompleted at SWAPPING, as
// the reindex provider does, and counts the flips it fired.
type staleCopyFlipProvider struct {
	leader *distributedtask.Manager
	schema *flipTarget

	mu    sync.Mutex
	flips int
}

func (p *staleCopyFlipProvider) OnTaskCompleted(task *distributedtask.Task) error {
	if task.Status != distributedtask.TaskStatusSwapping {
		return nil
	}
	var payload ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return err
	}
	err := p.schema.flip(p.leader,
		api.MigrationRun{Namespace: task.Namespace, ID: task.ID, Version: task.Version}, payload.TargetTokenization)
	p.mu.Lock()
	p.flips++
	p.mu.Unlock()
	return err
}

func (p *staleCopyFlipProvider) flipsFired() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flips
}

func (p *staleCopyFlipProvider) OnGroupCompleted(*distributedtask.Task, string, []string) error {
	return nil
}

func (p *staleCopyFlipProvider) OnSwapRequested(*distributedtask.Task, string, []string) error {
	return nil
}

func (p *staleCopyFlipProvider) SetCompletionRecorder(distributedtask.TaskCompletionRecorder) {}

func (p *staleCopyFlipProvider) GetLocalTasks() []distributedtask.TaskDescriptor { return nil }

func (p *staleCopyFlipProvider) CleanupTask(distributedtask.TaskDescriptor) error { return nil }

func (p *staleCopyFlipProvider) StartTask(*distributedtask.Task) (distributedtask.TaskHandle, error) {
	return nil, errors.New("this node owns no unit")
}

// leaderFinalizer applies the scheduler's finalize and fail proposals on the leader.
type leaderFinalizer struct{ leader *distributedtask.Manager }

func (f leaderFinalizer) MarkDistributedTaskFinalized(_ context.Context, namespace, id string, version uint64) error {
	raw, err := json.Marshal(&api.MarkTaskFinalizedRequest{
		Namespace: namespace, Id: id, Version: version, FinalizedAtUnixMillis: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return f.leader.MarkTaskFinalized(&api.ApplyRequest{SubCommand: raw})
}

func (f leaderFinalizer) MarkDistributedTaskFailed(_ context.Context, namespace, id string, version uint64, errMsg string) error {
	raw, err := json.Marshal(&api.MarkTaskFailedRequest{
		Namespace: namespace, Id: id, Version: version, Error: errMsg, FailedAtUnixMillis: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return f.leader.MarkTaskFailed(&api.ApplyRequest{SubCommand: raw})
}
