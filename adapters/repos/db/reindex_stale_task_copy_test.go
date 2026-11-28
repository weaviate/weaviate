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

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A unit the leader already completed must not run again when this node relaunches it from a trailing copy.
func TestReindexRelaunchFromStaleCopySkipsCompletedUnit(t *testing.T) {
	enableFilterable := func(t *testing.T, ctx context.Context) (*Shard, *Index, *ReindexTaskPayload) {
		className := "StaleRelaunch_" + uuid.NewString()[:8]
		shd, idx := testShardWithSettings(t, ctx, newEnableFilterableTestClass(className, "title"),
			enthnsw.UserConfig{Skip: true}, true, false, false)
		shard := shd.(*Shard)
		t.Cleanup(func() { shard.Shutdown(context.Background()) })
		registerIndex(idx, className)
		for _, obj := range makeConvergenceTestObjects(t, 25, className) {
			require.NoError(t, shard.PutObject(ctx, obj))
		}
		return shard, idx, &ReindexTaskPayload{Collection: className, MigrationType: ReindexTypeEnableFilterable, Properties: []string{"title"}}
	}
	for _, tc := range []struct {
		name  string
		shard func(*testing.T, context.Context) (*Shard, *Index, *ReindexTaskPayload)
	}{
		{name: "semantic", shard: enableFilterable},
		{name: "format-only", shard: newUnitRetryShard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, idx, payload := tc.shard(t, ctx)
			unit, other := shard.migrationUnit(), "other__node2"
			desc := distributedtask.TaskDescriptor{ID: "T_stale", Version: 1}

			logger, hook := test.NewNullLogger()
			leader := fsmRecorder{distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logger}), t}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			require.NoError(t, leader.m.AddTask(leader.req(&api.AddDistributedTaskRequest{
				Namespace: ReindexNamespace, Id: desc.ID, Payload: raw,
				SubmittedAtUnixMillis: time.Now().UnixMilli(), UnitIds: []string{unit, other},
			}), desc.Version))
			require.NoError(t, leader.UpdateDistributedTaskUnitProgress(ctx, ReindexNamespace, desc.ID, desc.Version, "node1", unit, 0))
			require.NoError(t, leader.RecordDistributedTaskUnitCompletion(ctx, ReindexNamespace, desc.ID, desc.Version, "node1", unit))

			stale := &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusStarted, Payload: raw,
				Units: map[string]*distributedtask.Unit{
					unit:  {ID: unit, Shard: shard.Name(), NodeID: "node1", Status: distributedtask.UnitStatusInProgress},
					other: {ID: other, Shard: "other", NodeID: "node2"},
				},
			}
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", func() int { return 1 }, ctx)
			require.False(t, p.processOneUnit(ctx, stale, payload, idx, unit, leader))
			require.Zero(t, countLines(hook, "unit completed", unit), "the completed unit's shard work ran again")
			require.Empty(t, p.cachedReindexTasks(desc, unit))
		})
	}
}

// fsmRecorder writes straight to a Manager, as the leader's FSM applies them.
type fsmRecorder struct {
	m *distributedtask.Manager
	t *testing.T
}

func (r fsmRecorder) req(sub any) *api.ApplyRequest {
	raw, err := json.Marshal(sub)
	require.NoError(r.t, err)
	return &api.ApplyRequest{SubCommand: raw}
}

func (r fsmRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	return r.m.UpdateUnitProgress(r.req(&api.UpdateDistributedTaskUnitProgressRequest{
		Namespace: namespace, Id: taskID, Version: version, NodeId: nodeID, UnitId: unitID,
		Progress: progress, UpdatedAtUnixMillis: time.Now().UnixMilli(),
	}))
}

func (r fsmRecorder) RecordDistributedTaskUnitCompletion(_ context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error {
	return r.RecordDistributedTaskUnitFailure(context.Background(), namespace, taskID, version, nodeID, unitID, "")
}

func (r fsmRecorder) RecordDistributedTaskUnitFailure(_ context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	return r.m.RecordUnitCompletion(r.req(&api.RecordDistributedTaskUnitCompletionRequest{
		Namespace: namespace, Id: taskID, Version: version, NodeId: nodeID, UnitId: unitID,
		Error: errMsg, FinishedAtUnixMillis: time.Now().UnixMilli(),
	}))
}

// A trailing task copy firing an older run's flip must not revert a newer run's flip.
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
		TaskSource:    node,
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

// staleCopyFlipProvider flips the schema from OnTaskCompleted at SWAPPING, as the reindex provider does.
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
