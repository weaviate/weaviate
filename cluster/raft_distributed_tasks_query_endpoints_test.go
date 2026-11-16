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

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/rpc"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

func TestListDistributedTasksUnreachableLeader(t *testing.T) {
	list := func(ctx context.Context, srv *Raft) error {
		_, err := srv.ListDistributedTasks(ctx)
		return err
	}
	write := func(ctx context.Context, srv *Raft) error {
		return srv.MarkDistributedTaskFinalized(ctx, "ns", "t", 1)
	}
	bounded := 100 * time.Millisecond
	tests := []struct {
		name            string
		call            func(context.Context, *Raft) error
		consistencyWait time.Duration
		callerCancel    time.Duration
		wantCode        codes.Code
	}{
		{name: "caller without deadline", call: list, consistencyWait: bounded, wantCode: codes.DeadlineExceeded},
		{name: "caller without deadline cancels first", call: list, consistencyWait: time.Hour, callerCancel: 100 * time.Millisecond, wantCode: codes.Canceled},
		{name: "task write, caller cancels first", call: write, consistencyWait: time.Hour, callerCancel: 100 * time.Millisecond, wantCode: codes.Canceled},
		{name: "unit completion", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.RecordDistributedTaskUnitCompletion(ctx, "ns", "t", 1, "node-1", "u")
		}},
		{name: "unit progress", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.UpdateDistributedTaskUnitProgress(ctx, "ns", "t", 1, "node-1", "u", 0.5)
		}},
		{name: "clean up", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.CleanUpDistributedTask(ctx, "ns", "t", 1)
		}},
		{name: "mark failed", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.MarkDistributedTaskFailed(ctx, "ns", "t", 1, "e")
		}},
		{name: "post-completion ack", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.RecordDistributedTaskPostCompletionAck(ctx, "ns", "t", 1, "g", "node-1", true, "")
		}},
		{name: "preparation ack", consistencyWait: bounded, wantCode: codes.DeadlineExceeded, call: func(ctx context.Context, srv *Raft) error {
			return srv.RecordDistributedTaskPreparationCompleteAck(ctx, "ns", "t", 1, "g", "node-1", true, "")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			m.store.cfg.ConsistencyWaitTimeout = tt.consistencyWait
			// The RPC client below dials a freed port, so this leader never answers.
			heartbeatFromLeader(t, m.store)

			cl := rpc.NewClient(resolver.NewRpc(false, utils.MustGetFreeTCPPort()), 1<<20, false, m.logger)
			t.Cleanup(cl.Close)
			srv := NewRaft(mocks.NewMockNodeSelector(), m.store, cl)

			ctx := context.Background()
			if tt.callerCancel > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(tt.callerCancel, cancel)
			}
			done := make(chan error, 1)
			go func() { done <- tt.call(ctx, srv) }()
			select {
			case err := <-done:
				require.Equal(t, tt.wantCode, status.Code(err))
			case <-time.After(5 * time.Second):
				t.Fatal("call did not return")
			}
		})
	}
}

func TestSchedulerTickSurvivesUnreachableLeader(t *testing.T) {
	m := NewMockStore(t, "node-1", 0)
	m.store.cfg.ConsistencyWaitTimeout = 100 * time.Millisecond
	apply := func(sub any) *cmd.ApplyRequest {
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		return &cmd.ApplyRequest{SubCommand: raw}
	}
	tasks := m.store.distributedTasksManager
	require.NoError(t, tasks.AddTask(apply(&cmd.AddDistributedTaskRequest{Namespace: "ns", Id: "t", UnitIds: []string{"u"}}), 1))
	require.NoError(t, tasks.RecordUnitCompletion(apply(&cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace: "ns", Id: "t", Version: 1, NodeId: "node-1", UnitId: "u",
	})))
	heartbeatFromLeader(t, m.store)
	cl := rpc.NewClient(resolver.NewRpc(false, utils.MustGetFreeTCPPort()), 1<<20, false, m.logger)
	t.Cleanup(cl.Close)
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, cl)

	clock := clockwork.NewFakeClock()
	provider := &tickCountingProvider{}
	sched := distributedtask.NewScheduler(distributedtask.SchedulerParams{
		TaskLister:       srv.SchedulerTaskLister(),
		TaskFinalizer:    srv,
		TaskCleaner:      srv,
		Providers:        map[string]distributedtask.Provider{"ns": provider},
		Clock:            clock,
		Logger:           m.logger,
		LocalNode:        "node-1",
		TickInterval:     time.Minute,
		CompletedTaskTTL: time.Hour,
	})
	require.NoError(t, sched.Start(context.Background()))
	t.Cleanup(sched.Close)
	require.NoError(t, clock.BlockUntilContext(context.Background(), 1))

	for tick := 1; tick <= 2; tick++ {
		before := provider.calls.Load()
		clock.Advance(time.Minute)
		require.Eventually(t, func() bool { return provider.calls.Load() > before }, 5*time.Second, 10*time.Millisecond,
			"tick %d did not finish", tick)
	}
}

type tickCountingProvider struct{ calls atomic.Int32 }

func (p *tickCountingProvider) SetCompletionRecorder(distributedtask.TaskCompletionRecorder) {}

func (p *tickCountingProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	p.calls.Add(1)
	return nil
}

func (p *tickCountingProvider) CleanupTask(distributedtask.TaskDescriptor) error { return nil }

func (p *tickCountingProvider) StartTask(*distributedtask.Task) (distributedtask.TaskHandle, error) {
	return nil, errors.New("this node owns no unit")
}

// heartbeatFromLeader makes store a follower of a leader at 127.0.0.1:1.
func heartbeatFromLeader(t *testing.T, store *Store) {
	t.Helper()
	followerTransport := attachFollowerRaft(t, store)
	leaderAddr, leaderTransport := raft.NewInmemTransport("127.0.0.1:1")
	leaderTransport.Connect(followerTransport.LocalAddr(), followerTransport)
	require.NoError(t, leaderTransport.AppendEntries("node-1", followerTransport.LocalAddr(),
		&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("leader"), Addr: []byte(leaderAddr),
		}, Term: 1},
		&raft.AppendEntriesResponse{}))
	require.Equal(t, string(leaderAddr), store.Leader())
}

func TestSchedulerTaskLister(t *testing.T) {
	for _, tt := range []struct {
		name   string
		leader bool
	}{
		{name: "leader known", leader: true},
		{name: "no leader"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			sub, err := json.Marshal(&cmd.AddDistributedTaskRequest{Namespace: "ns", Id: "t", UnitIds: []string{"u"}})
			require.NoError(t, err)
			require.NoError(t, m.store.distributedTasksManager.AddTask(&cmd.ApplyRequest{SubCommand: sub}, 1))
			if tt.leader {
				heartbeatFromLeader(t, m.store)
			} else {
				attachFollowerRaft(t, m.store)
				require.Empty(t, m.store.Leader())
			}

			tasks, err := NewRaft(mocks.NewMockNodeSelector(), m.store, nil).SchedulerTaskLister().
				ListDistributedTasks(context.Background())
			if !tt.leader {
				require.ErrorIs(t, err, types.ErrLeaderNotFound)
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks["ns"], 1)
			require.Equal(t, "t", tasks["ns"][0].ID)
		})
	}
}
